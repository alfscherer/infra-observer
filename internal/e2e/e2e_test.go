package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/sim"
	"github.com/alfscherer/infra-observer/internal/testutil"
	"github.com/alfscherer/infra-observer/migrations"
)

const remediationPolicy = `
allowlist: {devices: [switch-01]}
policies:
  - id: remediate-interface-down
    trigger: {event: interface.down, device_type: switch}
    conditions: {duration: 300ms, retries_below: 3}
    proposal: {script: interface-remediation, allowed_actions: [bounce_interface]}
    safety: {dry_run: true, cooldown: 1m, require_tag: automation-enabled}
`

// backlog is the number of messages a durable consumer still has to process.
func (l *lab) backlog(stream, durable string) uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pending, ack, err := l.client.ConsumerBacklog(ctx, stream, durable)
	if err != nil {
		return 1 << 30
	}
	return pending + ack
}

func (l *lab) waitDrained(timeout time.Duration, durables ...string) {
	l.t.Helper()
	eventually(l.t, timeout, "consumers to drain their backlog", func() bool {
		for _, d := range durables {
			stream := messaging.StreamTelemetryNormalized
			if strings.HasPrefix(d, "normalizer") {
				stream = messaging.StreamTelemetryRaw
			}
			if l.backlog(stream, d) != 0 {
				return false
			}
		}
		return true
	})
}

func (l *lab) streamHasCorrelation(stream, corr string) bool {
	ctx := context.Background()
	s, err := l.client.JetStream().Stream(ctx, stream)
	if err != nil {
		l.t.Fatal(err)
	}
	info, _ := s.Info(ctx)
	for seq := max(info.State.FirstSeq, 1); seq <= info.State.LastSeq; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err == nil && m.Header.Get(messaging.HeaderCorrelationID) == corr {
			return true
		}
	}
	return false
}

// The specification's end-to-end scenario, unabridged:
//
//	simulated switch -> SNMP observation -> NATS -> JavaScript normalization /
//	enrichment -> state transition -> alert -> JavaScript automation proposal ->
//	Go policy validation -> automation dry-run -> persisted result
func TestEndToEndSwitchInterfaceDownToAuditedDryRun(t *testing.T) {
	l := newLab(t, labOpts{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.collectorLoop(ctx, nil)
	run(t, l.processor(l.pg, "").Run)
	run(t, l.automationRole(remediationPolicy, true).Run)

	eventually(t, 30*time.Second, "baseline telemetry in the database", func() bool { return l.countRows(`SELECT count(*) FROM observations`) > 300 })
	if evs, _, _ := l.pg.ListEvents(ctx, persistence.EventFilter{}, persistence.Page{Limit: 10}); len(evs) != 0 {
		t.Fatalf("a healthy lab must raise nothing: %+v", evs)
	}

	if _, err := l.world.Apply(sim.Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/2"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "interface.down event", func() bool { return len(l.events(l.pg, "switch-01", "interface.down")) == 1 })
	ev := l.events(l.pg, "switch-01", "interface.down")[0]
	if ev.Labels["interface"] != "Gi0/2" || ev.CorrelationID == "" {
		t.Fatalf("%+v", ev)
	}

	// the script proposes, Go validates, the engine dry-runs and audits
	var view persistence.AutomationView
	eventually(t, 30*time.Second, "an audited dry-run result", func() bool {
		items, _, _ := l.pg.ListAutomation(ctx, persistence.AutomationFilter{DeviceID: "switch-01"}, persistence.Page{Limit: 10})
		for _, v := range items {
			if v.Result != nil {
				view = v
				return true
			}
		}
		return false
	})
	r, res := view.Request, view.Result
	if r.Proposer != "script:interface-remediation" || r.Action != "bounce_interface" || r.Target != "Gi0/2" || !r.DryRun || res.Status != domain.AutomationDryRun {
		t.Fatalf("%+v %+v", r, res)
	}
	if !strings.Contains(res.Message, "dry-run: would bounce interface Gi0/2") || res.Details["before_oper"] != "down" {
		t.Fatalf("the dry run must report what a real SNMP read found: %q %v", res.Message, res.Details)
	}
	if r.EventID != ev.EventID || r.CorrelationID != ev.CorrelationID || res.CorrelationID != ev.CorrelationID {
		t.Fatalf("event, request and result must share one correlation id: %s %s %s", ev.CorrelationID, r.CorrelationID, res.CorrelationID)
	}
	// dry-run really did nothing to the device
	dev, _ := l.world.Device("switch-01")
	var oper bool
	l.world.With(func() { oper = dev.Iface[1].OperUp })
	if oper {
		t.Fatal("a dry run must not change the simulated device")
	}

	// traceability: the correlation id is on the raw telemetry message that started it all
	if !l.streamHasCorrelation(messaging.StreamTelemetryRaw, ev.CorrelationID) || !l.streamHasCorrelation(messaging.StreamTelemetryNormalized, ev.CorrelationID) {
		t.Fatal("the event's correlation id must be traceable back to the raw and normalized telemetry messages")
	}
	// the events and the automation trail reached NATS through the outbox
	eventually(t, 15*time.Second, "outbox drained to NATS", func() bool { return l.countRows(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0 })
	es, _ := l.client.JetStream().Stream(ctx, messaging.StreamEvents)
	info, _ := es.Info(ctx)
	if info.State.Msgs < 2 {
		t.Fatalf("events.device and events.alert notifications expected in JetStream: %+v", info.State)
	}
	if len(l.deadLetters()) != 0 {
		t.Fatalf("a healthy pipeline dead-letters nothing: %+v", l.deadLetters())
	}
}

func TestDuplicateDeliveryProducesOneEventOneAlertAndOneNotification(t *testing.T) {
	l := newLab(t, labOpts{})
	run(t, l.processor(l.pg, "").Run)

	base := time.Now().UTC().Add(-time.Minute)
	seq := [][]byte{
		rawObs("dup-1", "server-01", "collector.poll_success", true, base),
		rawObs("dup-2", "server-01", "collector.poll_success", false, base.Add(1*time.Second)),
		rawObs("dup-3", "server-01", "collector.poll_success", false, base.Add(2*time.Second)),
		rawObs("dup-4", "server-01", "collector.poll_success", false, base.Add(3*time.Second)), // third consecutive failure
	}
	for round := 0; round < 4; round++ { // the same four observations, delivered four times over
		for _, p := range seq {
			l.publishRaw(p)
		}
	}
	eventually(t, 30*time.Second, "device.down", func() bool { return len(l.events(l.pg, "server-01", "device.down")) >= 1 })
	l.waitDrained(30*time.Second, "normalizer", "processor")
	eventually(t, 15*time.Second, "outbox drained", func() bool { return l.countRows(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0 })

	if n := len(l.events(l.pg, "server-01", "device.down")); n != 1 {
		t.Fatalf("%d device.down events from 16 deliveries of 4 observations; want exactly 1", n)
	}
	if n := l.countRows(`SELECT count(*) FROM observations WHERE observation_id LIKE 'dup-%'`); n != 4 {
		t.Fatalf("%d observation rows, want 4", n)
	}
	if n := l.countRows(`SELECT count(*) FROM alerts WHERE device_id = 'server-01' AND status = 'firing'`); n != 1 {
		t.Fatalf("%d firing alerts", n)
	}
	if n := l.countRows(`SELECT count(*) FROM state_transitions WHERE device_id = 'server-01'`); n != 3 { // adopt, suspect, down
		t.Fatalf("%d transitions", n)
	}
	es, _ := l.client.JetStream().Stream(context.Background(), messaging.StreamEvents)
	info, _ := es.Info(context.Background(), streamSubjects())
	if info.State.Subjects[messaging.SubjectEventDevice] != 1 || info.State.Subjects[messaging.SubjectEventAlert] != 1 {
		t.Fatalf("exactly one notification per subject expected, got %v", info.State.Subjects)
	}
	if len(l.deadLetters()) != 0 {
		t.Fatal("duplicates are not errors")
	}
}

// gateStore holds every transaction open until released, modelling a consumer
// that is in the middle of processing when it dies.
type gateStore struct {
	*persistence.PGStore
	release chan struct{}
}

func (g *gateStore) Do(ctx context.Context, fn func(persistence.Tx) error) error {
	select {
	case <-g.release:
		return g.PGStore.Do(ctx, fn)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestConsumerCrashMidMessageLosesNothingAndDuplicatesNothing(t *testing.T) {
	l := newLab(t, labOpts{})
	gate := &gateStore{PGStore: l.pg, release: make(chan struct{})}
	procA := l.processor(gate, "")
	stopA := run(t, procA.Run)

	const total = 30
	base := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < total; i++ {
		l.publishRaw(rawObs(fmt.Sprintf("crash-%02d", i), "server-01", "snmp.sysUpTime", float64(i), base.Add(time.Duration(i)*time.Second)))
	}
	eventually(t, 15*time.Second, "worker A to hold messages in flight", func() bool { return procA.StageB != nil && procA.StageB.Stats().InFlight > 0 })
	stopA() // crash: transactions never committed, messages never acknowledged

	if n := l.countRows(`SELECT count(*) FROM observations WHERE observation_id LIKE 'crash-%'`); n != 0 {
		t.Fatalf("nothing may be committed by an interrupted consumer, found %d rows", n)
	}
	run(t, l.processor(l.pg, "").Run) // the restarted consumer
	eventually(t, 30*time.Second, "every message to be processed after the restart", func() bool {
		return l.countRows(`SELECT count(*) FROM observations WHERE observation_id LIKE 'crash-%'`) == total
	})
	l.waitDrained(20*time.Second, "normalizer", "processor")
	if n := l.countRows(`SELECT count(*) FROM observations WHERE observation_id LIKE 'crash-%'`); n != total {
		t.Fatalf("%d rows, want %d (no loss, no duplicates)", n, total)
	}
	if len(l.deadLetters()) != 0 {
		t.Fatalf("an interrupted consumer is not poison: %+v", l.deadLetters())
	}
}

func TestTemporaryDatabaseOutageIsAbsorbedByRetries(t *testing.T) {
	l := newLab(t, labOpts{maxDeliver: 10, retryDelay: 100 * time.Millisecond})
	for i := range l.devices {
		l.devices[i].Collection.Interval = 500 * time.Millisecond
	}
	l.reg.Replace(l.devices)
	store := &faultyStore{PGStore: l.pg}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.collectorLoop(ctx, nil)
	proc := l.processor(store, "")
	run(t, proc.Run)
	eventually(t, 20*time.Second, "baseline telemetry", func() bool { return l.countRows(`SELECT count(*) FROM observations`) > 100 })

	store.down.Store(true) // the database "disappears"
	_, _ = l.world.Apply(sim.Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/3"}})
	time.Sleep(1500 * time.Millisecond)
	if store.hits.Load() == 0 {
		t.Fatal("the outage never bit")
	}
	frozen := l.countRows(`SELECT count(*) FROM observations`)
	time.Sleep(300 * time.Millisecond)
	if l.countRows(`SELECT count(*) FROM observations`) != frozen {
		t.Fatal("nothing can be stored during the outage")
	}
	store.down.Store(false) // and comes back

	eventually(t, 40*time.Second, "the interface.down that happened during the outage", func() bool {
		return len(l.events(l.pg, "switch-01", "interface.down")) == 1
	})
	if proc.StageB.Stats().Retried == 0 {
		t.Fatal("messages should have been retried with backoff")
	}
	if n := len(l.deadLetters()); n != 0 {
		t.Fatalf("an outage shorter than the retry budget must lose nothing to the dead-letter stream, found %d", n)
	}
	if n := len(l.events(l.pg, "switch-01", "interface.down")); n != 1 {
		t.Fatalf("exactly one event despite retries: %d", n)
	}
}

func TestOutageBeyondTheRetryBudgetLandsInTheDeadLetterStreamAndReplaysCleanly(t *testing.T) {
	l := newLab(t, labOpts{maxDeliver: 3, retryDelay: 50 * time.Millisecond})
	store := &faultyStore{PGStore: l.pg}
	store.down.Store(true)
	run(t, l.processor(store, "").Run)

	const total = 12
	base := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < total; i++ {
		l.publishRaw(rawObs(fmt.Sprintf("late-%02d", i), "server-01", "snmp.sysUpTime", float64(i), base.Add(time.Duration(i)*time.Second)))
	}
	eventually(t, 30*time.Second, "all messages to exhaust their retries", func() bool { return len(l.deadLetters()) == total })
	for _, dl := range l.deadLetters() {
		if dl.Category != "dependency" || dl.Reason != "max_deliveries" || dl.Consumer != "processor" || dl.Attempts != 3 {
			t.Fatalf("%+v", dl)
		}
	}
	if n := l.countRows(`SELECT count(*) FROM observations`); n != 0 {
		t.Fatalf("%d", n)
	}

	store.down.Store(false) // the database is repaired; now recover the parked messages
	ctx := context.Background()
	for _, dl := range l.deadLetters() {
		if err := l.client.ReplayDeadLetter(ctx, dl.StreamSeq); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 30*time.Second, "replayed messages to be stored", func() bool { return l.countRows(`SELECT count(*) FROM observations`) == total })
	if len(l.deadLetters()) != 0 {
		t.Fatal("replayed dead letters are removed")
	}
}

func TestNATSRestartMidFlow(t *testing.T) {
	l := newLab(t, labOpts{})
	for i := range l.devices {
		l.devices[i].Collection.Interval = 300 * time.Millisecond
	}
	l.reg.Replace(l.devices)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.collectorLoop(ctx, nil)
	run(t, l.processor(l.pg, "").Run)
	eventually(t, 20*time.Second, "baseline telemetry", func() bool { return l.countRows(`SELECT count(*) FROM observations`) > 150 })

	before := l.countRows(`SELECT count(*) FROM observations`)
	l.nats.Restart() // the broker restarts underneath everything
	eventually(t, 20*time.Second, "reconnection", l.client.Connected)
	eventually(t, 40*time.Second, "telemetry to keep flowing through the new broker", func() bool {
		return l.countRows(`SELECT count(*) FROM observations`) > before+150
	})
	_, _ = l.world.Apply(sim.Scenario{Device: "ups-01", Event: "power-loss"})
	eventually(t, 30*time.Second, "detection to keep working after the restart", func() bool { return len(l.events(l.pg, "ups-01", "ups.on_battery")) == 1 })
	if n := len(l.deadLetters()); n != 0 {
		t.Fatalf("a broker restart must not dead-letter anything, found %d", n)
	}
}

func TestMalformedAndPoisonTelemetryIsDeadLetteredWhileGoodDataFlows(t *testing.T) {
	l := newLab(t, labOpts{})
	run(t, l.processor(l.pg, "").Run)

	now := time.Now().UTC()
	l.publishRaw(rawObs("good-before", "server-01", "snmp.sysUpTime", 1.0, now.Add(-10*time.Second)))
	l.publishRaw([]byte("this is not json {"))                                                          // malformed
	l.publishRaw([]byte(`{"observation_id":"poison","metric":"Not A Metric","value":{"nested":true}}`)) // structurally invalid
	l.publishRaw(rawObs("ghost", "ghost-99", "snmp.sysUpTime", 1.0, now.Add(-9*time.Second)))           // unregistered device
	l.publishRaw(rawObs("future", "server-01", "snmp.sysUpTime", 1.0, now.Add(72*time.Hour)))           // timestamp in the future
	l.publishRaw(rawObs("good-after", "server-01", "snmp.sysUpTime", 2.0, now.Add(-8*time.Second)))

	eventually(t, 30*time.Second, "four dead letters and two stored observations", func() bool {
		return len(l.deadLetters()) == 4 && l.countRows(`SELECT count(*) FROM observations WHERE observation_id LIKE 'good-%'`) == 2
	})
	byConsumer := map[string]int{}
	for _, dl := range l.deadLetters() {
		if dl.Category != "validation" || dl.Reason != "poison" || dl.Attempts != 1 {
			t.Fatalf("invalid input must be dead-lettered on the first attempt, not retried: %+v", dl)
		}
		byConsumer[dl.Consumer]++
	}
	if byConsumer["normalizer"] != 3 || byConsumer["processor"] != 1 {
		t.Fatalf("malformed, poison and future are rejected at stage A; the unregistered device at stage B: %v", byConsumer)
	}
}

func TestReplayRebuildsIdenticalStateInAnEmptyDatabase(t *testing.T) {
	l := newLab(t, labOpts{})
	ctx, cancelCollector := context.WithCancel(context.Background())
	l.collectorLoop(ctx, nil)
	stopProc := run(t, l.processor(l.pg, "").Run)

	eventually(t, 30*time.Second, "baseline", func() bool { return l.countRows(`SELECT count(*) FROM observations`) > 250 })
	_, _ = l.world.Apply(sim.Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/4"}})
	_, _ = l.world.Apply(sim.Scenario{Device: "server-01", Event: "high-cpu"})
	eventually(t, 30*time.Second, "events", func() bool {
		return len(l.events(l.pg, "switch-01", "interface.down")) == 1 && len(l.events(l.pg, "server-01", "cpu.high")) == 1
	})
	cancelCollector() // stop producing
	time.Sleep(700 * time.Millisecond)
	l.waitDrained(30*time.Second, "normalizer", "processor")
	stopProc()

	origEvents, _, _ := l.pg.ListEvents(context.Background(), persistence.EventFilter{}, persistence.Page{Limit: 500})
	origObs := l.countRows(`SELECT count(*) FROM observations`)
	origStates := l.countRows(`SELECT count(*) FROM state_current`)
	ids := func(evs []domain.Event) map[string]bool {
		m := map[string]bool{}
		for _, e := range evs {
			m[e.EventID] = true
		}
		return m
	}
	want := ids(origEvents)
	if len(want) < 2 {
		t.Fatal("need events to compare")
	}

	// A brand-new, empty database: disaster recovery. Fresh consumers read the
	// retained streams from the beginning and rebuild everything.
	pool2 := testutil.PostgresPool(t)
	if _, err := persistence.Migrate(context.Background(), pool2, migrations.FS); err != nil {
		t.Fatal(err)
	}
	store2 := persistence.NewPGStore(pool2)
	if err := store2.UpsertDevices(context.Background(), l.devices); err != nil {
		t.Fatal(err)
	}
	run(t, l.processor(store2, "-rebuild").Run)
	eventually(t, 60*time.Second, "the empty database to be rebuilt from the streams", func() bool {
		n, _ := store2.CountForTest(context.Background(), `SELECT count(*) FROM observations`)
		return n == origObs
	})
	l.waitDrained(30*time.Second, "processor-rebuild")
	rebuilt, _, _ := store2.ListEvents(context.Background(), persistence.EventFilter{}, persistence.Page{Limit: 500})
	got := ids(rebuilt)
	for id := range want {
		if !got[id] {
			t.Errorf("event %s exists in the original database but not in the rebuilt one", id)
		}
	}
	for id := range got {
		if !want[id] {
			t.Errorf("event %s was invented by the rebuild", id)
		}
	}
	if n, _ := store2.CountForTest(context.Background(), `SELECT count(*) FROM state_current`); n != origStates {
		t.Errorf("rebuilt %d state rows, original had %d", n, origStates)
	}
}

func TestBrokenJavaScriptExtensionsNeverStopThePipeline(t *testing.T) {
	extra := t.TempDir()
	for _, name := range []string{"throws", "spins", "invalid-output"} {
		src, err := os.ReadFile("../../testdata/faulty-scripts/transforms/" + name + ".js")
		if err != nil {
			t.Fatal(err)
		}
		writeScript(t, extra, "transforms", "fault-"+name, string(src))
	}
	l := newLab(t, labOpts{extraScripts: extra})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.collectorLoop(ctx, nil)
	run(t, l.processor(l.pg, "").Run)

	eventually(t, 30*time.Second, "baseline telemetry despite three broken transforms", func() bool { return l.countRows(`SELECT count(*) FROM observations`) > 200 })
	_, _ = l.world.Apply(sim.Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/5"}})
	eventually(t, 30*time.Second, "detection to keep working", func() bool { return len(l.events(l.pg, "switch-01", "interface.down")) == 1 })

	defer func() {
		if t.Failed() {
			for _, s := range l.svc.Registry.List() {
				t.Logf("script %-32s status=%-11s calls=%d failures=%d consecutive=%d last=%q", s.Key, s.Status, s.TotalCalls, s.TotalFailures, s.ConsecutiveFailures, s.LastError)
			}
			t.Logf("pool stats: %+v", l.svc.Pool.Stats())
		}
	}()
	eventually(t, 15*time.Second, "all three faulty scripts to be quarantined", func() bool { return l.svc.Health().Quarantined == 3 })
	for _, name := range []string{"fault-throws", "fault-spins", "fault-invalid-output"} {
		s, ok := l.svc.Registry.Get("transforms/" + name)
		if !ok || s.Status != registry.StatusQuarantined || s.TotalFailures == 0 {
			t.Errorf("%s: %+v", name, s)
		}
	}
	h := l.svc.Health()
	if h.Stats.Timeouts == 0 {
		t.Fatalf("the infinite loop must have been stopped by the deadline: %+v", h.Stats)
	}
	if h.Loaded < 5 {
		t.Fatalf("the shipped scripts keep running: %+v", h)
	}
	if n := len(l.deadLetters()); n != 0 {
		t.Fatalf("script failures are isolated; nothing may reach the dead-letter stream: %d", n)
	}
}

func TestDeviceAuthenticationFailureIsCategorisedAndAlertsThenRecovers(t *testing.T) {
	l := newLab(t, labOpts{})
	var mu sync.Mutex
	categories := map[string]int{}
	var polls atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.collectorLoop(ctx, func(e collector.PollEvent) {
		polls.Add(1)
		if e.Err != nil && e.Device.ID == "server-01" {
			mu.Lock()
			categories[string(domain.CategoryOf(e.Err))]++
			mu.Unlock()
		}
	})
	run(t, l.processor(l.pg, "").Run)
	eventually(t, 30*time.Second, "baseline", func() bool { return l.countRows(`SELECT count(*) FROM observations`) > 150 })

	_, _ = l.world.Apply(sim.Scenario{Device: "server-01", Event: "auth-failure"})
	eventually(t, 30*time.Second, "device.down after repeated authentication failures", func() bool { return len(l.events(l.pg, "server-01", "device.down")) == 1 })
	mu.Lock()
	auth := categories["authentication"]
	other := categories["timeout"] + categories["transient"]
	mu.Unlock()
	if auth < 3 || other != 0 {
		t.Fatalf("rejected credentials must be categorised as authentication, not as a timeout or a dead device: %v", categories)
	}
	if n := l.countRows(`SELECT count(*) FROM observations WHERE device_id = 'server-01' AND metric = 'system.reachable' AND value_bool = false AND metadata ->> 'error_category' = 'authentication'`); n == 0 {
		t.Fatal("the failed poll observations must carry the error category so operators can tell auth from outage")
	}

	_, _ = l.world.Apply(sim.Scenario{Device: "server-01", Event: "auth-ok"})
	eventually(t, 30*time.Second, "device.up after credentials work again", func() bool { return len(l.events(l.pg, "server-01", "device.up")) >= 1 })
}
