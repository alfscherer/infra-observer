package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/state"
)

var t0 = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

type harness struct {
	p     *Processor
	store *persistence.MemStore
	now   time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	norm, err := normalize.LoadFile("../../configs/normalization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defs, err := state.LoadFile("../../configs/states.yaml")
	if err != nil {
		t.Fatal(err)
	}
	devices := []domain.Device{
		{ID: "switch-01", Hostname: "switch-01.lab", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Site: "lab", Enabled: true,
			Tags: []string{"automation-enabled"}, Collection: domain.CollectionSpec{Protocol: "snmp"}},
		{ID: "server-01", Hostname: "server-01.lab", ManagementAddress: "x", DeviceType: domain.DeviceServer, Site: "lab", Enabled: true,
			Collection: domain.CollectionSpec{Protocol: "snmp"}},
		{ID: "off-01", Hostname: "off", ManagementAddress: "x", DeviceType: domain.DeviceServer, Enabled: false},
	}
	store := persistence.NewMemStore()
	_ = store.UpsertDevices(context.Background(), devices)
	h := &harness{store: store, now: t0}
	h.p = &Processor{
		Validator:  schema.Validator{Limits: schema.DefaultLimits(), Now: func() time.Time { return h.now.Add(time.Hour) }},
		Normalizer: norm, Enricher: enrich.Enricher{Inventory: inventory.NewRegistry(devices)}, States: defs, Store: store,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return h.now },
		StaleAfter: 2 * time.Minute, SweepInterval: 30 * time.Second, StartedAt: t0,
	}
	return h
}

func ifObs(n int, up bool) domain.Observation {
	id := fmt.Sprintf("if-%d", n)
	return domain.Observation{
		ObservationID: id, CorrelationID: "corr-" + id, DeviceID: "switch-01", Source: "snmp",
		Metric: "network.interface.operational", Value: up, Labels: map[string]string{"interface": "Gi0/1"},
		ObservedAt: t0.Add(time.Duration(n) * 10 * time.Second),
	}
}

func (h *harness) process(t *testing.T, o domain.Observation) Outcome {
	t.Helper()
	out, err := h.p.Process(context.Background(), o)
	if err != nil {
		t.Fatalf("process %s: %v", o.ObservationID, err)
	}
	return out
}

func TestInterfaceDownFlowProducesEventAlertAndNotifications(t *testing.T) {
	h := newHarness(t)
	h.process(t, ifObs(1, true))
	if out := h.process(t, ifObs(2, false)); len(out.Events) != 0 {
		t.Fatal("one failed sample must not alert")
	}
	out := h.process(t, ifObs(3, false))
	if len(out.Events) != 1 || out.Events[0].Type != "interface.down" || out.Transitions != 1 {
		t.Fatalf("second consecutive failure should raise interface.down: %+v", out)
	}
	ev := out.Events[0]
	if ev.CorrelationID != "corr-if-3" || ev.ObservationID != "if-3" || ev.Labels["interface"] != "Gi0/1" || ev.Severity != domain.SeverityWarning {
		t.Fatalf("event must be traceable to the observation that caused it: %+v", ev)
	}
	if !strings.Contains(ev.Message, "switch-01") || !strings.Contains(ev.Message, "Gi0/1") {
		t.Fatalf("message: %q", ev.Message)
	}
	if alerts := h.store.Alerts(); len(alerts) != 1 || alerts[0].Status != domain.AlertFiring {
		t.Fatalf("alert: %+v", alerts)
	}
	if h.store.OutboxPending() != 2 {
		t.Fatalf("expected events.device + events.alert notifications, got %d", h.store.OutboxPending())
	}
	// recovery resolves the alert
	h.process(t, ifObs(4, true))
	out = h.process(t, ifObs(5, true))
	if len(out.Events) != 1 || out.Events[0].Type != "interface.up" || !out.Events[0].Resolves {
		t.Fatalf("recovery: %+v", out)
	}
	if alerts := h.store.Alerts(); alerts[0].Status != domain.AlertResolved {
		t.Fatalf("alert should be resolved: %+v", alerts)
	}
}

func TestDuplicateDeliveryChangesNothing(t *testing.T) {
	h := newHarness(t)
	seq := []domain.Observation{ifObs(1, true), ifObs(2, false), ifObs(3, false)}
	for _, o := range seq {
		h.process(t, o)
	}
	events, alerts, pending, obsN, trans := len(h.store.Events()), len(h.store.Alerts()), h.store.OutboxPending(), h.store.ObservationCount(), len(h.store.Transitions())

	for round := 0; round < 3; round++ {
		for _, o := range seq {
			out := h.process(t, o)
			if !out.Duplicate || len(out.Events) != 0 {
				t.Fatalf("redelivered %s must be a recognised duplicate: %+v", o.ObservationID, out)
			}
		}
	}
	if len(h.store.Events()) != events || len(h.store.Alerts()) != alerts || h.store.OutboxPending() != pending ||
		h.store.ObservationCount() != obsN || len(h.store.Transitions()) != trans {
		t.Fatal("duplicate delivery produced duplicate events, alerts, notifications or rows")
	}
}

func TestReplayWithNewObservationIDsDoesNotRepeatAlert(t *testing.T) {
	// Same facts, re-collected: a *different* observation stream that keeps the
	// interface down must not produce a second alert while the first is open.
	h := newHarness(t)
	h.process(t, ifObs(1, true))
	h.process(t, ifObs(2, false))
	h.process(t, ifObs(3, false))
	for n := 4; n < 30; n++ {
		if out := h.process(t, ifObs(n, false)); len(out.Events) != 0 {
			t.Fatalf("sample %d: sustained failure must not re-alert (alert storm)", n)
		}
	}
	if len(h.store.Events()) != 1 || len(h.store.Alerts()) != 1 {
		t.Fatalf("events=%d alerts=%d, want 1/1", len(h.store.Events()), len(h.store.Alerts()))
	}
}

func TestOutOfOrderSampleIsIgnored(t *testing.T) {
	h := newHarness(t)
	h.process(t, ifObs(1, true))
	h.process(t, ifObs(5, true))
	out := h.process(t, ifObs(3, false)) // arrives late
	if out.Ignored != 1 || len(out.Events) != 0 {
		t.Fatalf("late sample must not affect state: %+v", out)
	}
}

func TestTemporaryDatabaseOutageIsRetryableAndLossless(t *testing.T) {
	h := newHarness(t)
	h.process(t, ifObs(1, true))
	h.process(t, ifObs(2, false))
	h.store.FailNext(domain.Errorf(domain.CategoryDependency, "database unavailable"))
	_, err := h.p.Process(context.Background(), ifObs(3, false))
	if !domain.IsRetryable(err) {
		t.Fatalf("an outage must surface as a retryable error, got %v", err)
	}
	if h.store.ObservationCount() != 2 || len(h.store.Events()) != 0 {
		t.Fatal("failed processing must leave no partial data")
	}
	// the broker redelivers; this time the database is back
	out := h.process(t, ifObs(3, false))
	if len(out.Events) != 1 {
		t.Fatalf("retry after outage must complete normally: %+v", out)
	}
}

func TestUnknownAndDisabledDevices(t *testing.T) {
	h := newHarness(t)
	ghost := ifObs(1, true)
	ghost.DeviceID = "ghost"
	if _, err := h.p.Process(context.Background(), ghost); domain.CategoryOf(err) != domain.CategoryValidation {
		t.Fatalf("unregistered device is poison, got %v", err)
	}
	off := ifObs(1, true)
	off.DeviceID = "off-01"
	out, err := h.p.Process(context.Background(), off)
	if err != nil || out.Dropped != "device_disabled" || h.store.ObservationCount() != 0 {
		t.Fatalf("disabled device: %+v %v", out, err)
	}
}

func TestUnmatchedMetricIsStoredButTracksNoState(t *testing.T) {
	h := newHarness(t)
	o := domain.Observation{ObservationID: "x1", CorrelationID: "c", DeviceID: "server-01", Source: "snmp",
		Metric: "system.uptime", Value: 100.0, ObservedAt: t0}
	out := h.process(t, o)
	if out.Transitions != 0 || h.store.ObservationCount() != 1 {
		t.Fatalf("%+v", out)
	}
}

func TestNormalizeMessageStageA(t *testing.T) {
	h := newHarness(t)
	raw := domain.Observation{
		ObservationID: "r1", CorrelationID: "c1", DeviceID: "switch-01", Source: "snmp", Metric: "snmp.ifOperStatus", Value: 2.0,
		Labels: map[string]string{"ifIndex": "1", "ifName": "Gi0/1"}, ObservedAt: t0,
	}
	payload, _ := schema.Encode(raw)
	got, err := h.p.NormalizeMessage(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	o := got.Observation
	if o.Metric != "network.interface.operational" || o.Value != false || o.Labels["interface"] != "Gi0/1" || !got.Mapped {
		t.Fatalf("%+v", o)
	}
	if o.ReceivedAt.IsZero() || o.CorrelationID != "c1" || o.ObservationID != "r1" {
		t.Fatalf("received_at must be stamped and identity preserved: %+v", o)
	}
	if _, err := schema.NewValidator().DecodeObservation(got.Payload); err != nil {
		t.Fatalf("stage A output must itself be a valid observation: %v", err)
	}
}

func TestNormalizeMessageRejectsPoison(t *testing.T) {
	h := newHarness(t)
	bad := map[string]string{
		"garbage":       `not json`,
		"missing":       `{"observation_id":"x"}`,
		"bad value":     `{"observation_id":"x","correlation_id":"c","device_id":"d","source":"snmp","metric":"snmp.ifOperStatus","value":"up","observed_at":"2026-08-01T12:00:00Z"}`,
		"unknown field": `{"observation_id":"x","correlation_id":"c","device_id":"d","source":"snmp","metric":"m","value":1,"observed_at":"2026-08-01T12:00:00Z","extra":1}`,
	}
	for name, body := range bad {
		_, err := h.p.NormalizeMessage(context.Background(), []byte(body))
		if err == nil || domain.IsRetryable(err) {
			t.Errorf("%s: must be a permanent validation failure, got %v", name, err)
		}
	}
}

func TestSweepTurnsSilenceIntoDown(t *testing.T) {
	h := newHarness(t)
	devices := []domain.Device{{ID: "server-01", Enabled: true, DeviceType: domain.DeviceServer, Collection: domain.CollectionSpec{Protocol: "snmp"}}}
	reach := func(n int, ok bool) domain.Observation {
		return domain.Observation{ObservationID: fmt.Sprintf("r-%d", n), CorrelationID: "c", DeviceID: "server-01", Source: "snmp",
			Metric: "system.reachable", Value: ok, Labels: nil, ObservedAt: t0.Add(time.Duration(n) * 10 * time.Second)}
	}
	h.process(t, reach(1, true))
	last := t0.Add(10 * time.Second)

	if n, err := h.p.Sweep(context.Background(), last.Add(30*time.Second), devices); err != nil || n != 0 {
		t.Fatalf("recent data is not stale: %d %v", n, err)
	}
	var events []domain.Event
	for i := 0; i < 4; i++ { // silence continues; the sweeper runs every 30s
		now := last.Add(3*time.Minute + time.Duration(i)*30*time.Second)
		before := len(h.store.Events())
		if _, err := h.p.Sweep(context.Background(), now, devices); err != nil {
			t.Fatal(err)
		}
		events = append(events, h.store.Events()[before:]...)
	}
	if len(events) != 1 || events[0].Type != "device.down" {
		t.Fatalf("silence should produce exactly one device.down: %+v", events)
	}
	// same sweep bucket twice is a duplicate, not a second failure
	before := h.store.ObservationCount()
	_, _ = h.p.Sweep(context.Background(), last.Add(3*time.Minute+30*time.Second), devices)
	if h.store.ObservationCount() != before {
		t.Fatal("re-running a sweep bucket must be idempotent")
	}
}

func TestSweepCoversDevicesThatNeverReported(t *testing.T) {
	h := newHarness(t)
	devices := []domain.Device{{ID: "server-01", Enabled: true, DeviceType: domain.DeviceServer, Collection: domain.CollectionSpec{Protocol: "snmp"}}}
	var down []domain.Event
	for i := 0; i < 6; i++ {
		before := len(h.store.Events())
		_, _ = h.p.Sweep(context.Background(), t0.Add(3*time.Minute+time.Duration(i)*30*time.Second), devices)
		down = append(down, h.store.Events()[before:]...)
	}
	if len(down) != 1 || down[0].DeviceID != "server-01" {
		t.Fatalf("a registered device that never reports must eventually alert: %+v", down)
	}
}

func TestConcurrentProcessingOfDuplicatesProducesOneEvent(t *testing.T) {
	h := newHarness(t)
	h.process(t, ifObs(1, true))
	h.process(t, ifObs(2, false))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.p.Process(context.Background(), ifObs(3, false))
		}()
	}
	wg.Wait()
	if len(h.store.Events()) != 1 || len(h.store.Alerts()) != 1 || h.store.OutboxPending() != 2 {
		t.Fatalf("events=%d alerts=%d outbox=%d", len(h.store.Events()), len(h.store.Alerts()), h.store.OutboxPending())
	}
}

// --- relay -------------------------------------------------------------------

type fakePublisher struct {
	mu   sync.Mutex
	got  []messaging.OutMsg
	fail error
}

func (f *fakePublisher) PublishBatch(_ context.Context, msgs []messaging.OutMsg) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.got = append(f.got, msgs...)
	return nil
}

func TestRelayPublishesOutboxAndSurvivesOutage(t *testing.T) {
	h := newHarness(t)
	h.process(t, ifObs(1, true))
	h.process(t, ifObs(2, false))
	h.process(t, ifObs(3, false))
	pub := &fakePublisher{fail: errors.New("nats down")}
	r := &Relay{Store: h.store, Publisher: pub, BatchSize: 1}

	if n, err := r.Drain(context.Background()); err == nil || n != 0 || h.store.OutboxPending() != 2 {
		t.Fatalf("outage: nothing may be lost or marked sent: n=%d err=%v pending=%d", n, err, h.store.OutboxPending())
	}
	pub.fail = nil
	n, err := r.Drain(context.Background())
	if err != nil || n != 2 || h.store.OutboxPending() != 0 {
		t.Fatalf("after recovery: n=%d err=%v pending=%d", n, err, h.store.OutboxPending())
	}
	subjects := map[string]string{}
	for _, m := range pub.got {
		subjects[m.Subject] = m.MsgID
		ev, err := schema.Decode[domain.Event](m.Payload, 0)
		if err != nil || ev.Type != "interface.down" {
			t.Fatalf("payload: %v %+v", err, ev)
		}
		if m.Headers[messaging.HeaderCorrelationID] != "corr-if-3" || m.Headers[messaging.HeaderSchema] != schema.EventV1 {
			t.Fatalf("headers: %v", m.Headers)
		}
	}
	if subjects[messaging.SubjectEventDevice] == "" || subjects[messaging.SubjectEventAlert] == "" ||
		subjects[messaging.SubjectEventDevice] == subjects[messaging.SubjectEventAlert] {
		t.Fatalf("both subjects need distinct msg ids for stream-level de-duplication: %v", subjects)
	}
}
