package sim

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/rules"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/state"
)

// pipeSink feeds collector output straight into the processing stages, in the
// same order NATS would (encode -> stage A -> stage B), minus the broker.
type pipeSink struct {
	t *testing.T
	p *pipeline.Processor
}

func (s pipeSink) Observations(ctx context.Context, obs []domain.Observation) error {
	for _, o := range obs {
		if ctx.Err() != nil {
			return ctx.Err() // shutting down: not a failure of the pipeline
		}
		payload, err := schema.Encode(o)
		if err != nil {
			return err
		}
		n, err := s.p.NormalizeMessage(ctx, payload)
		if err != nil {
			s.t.Errorf("stage A rejected %s: %v", o.Metric, err)
			continue
		}
		if _, err := s.p.Process(ctx, n.Observation); err != nil && ctx.Err() == nil {
			s.t.Errorf("stage B failed for %s: %v", n.Observation.Metric, err)
		}
	}
	return nil
}
func (pipeSink) Inventory(context.Context, collector.InventoryFact) error { return nil }

// TestSimulatedScenariosExerciseTheRealPipeline is the guarantee behind "the
// simulation must exercise the same pipeline as real telemetry": a scenario
// changes simulated device state, the real SNMP collector observes it over UDP,
// and the real stages turn it into events.
func TestSimulatedScenariosExerciseTheRealPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("uses real UDP polling and wall-clock time")
	}
	devs := labDevices(t)
	w := NewWorld(devs, Options{Seed: 5})
	devs = startAgents(t, w, devs)
	for i := range devs {
		devs[i].Collection.Interval = 120 * time.Millisecond
	}
	norm, _ := normalize.LoadFile("../../configs/normalization.yaml")
	defs, _ := state.LoadFile("../../configs/states.yaml")
	rs, _ := rules.LoadFile("../../configs/rules.yaml")
	store := persistence.NewMemStore()
	if err := store.UpsertDevices(context.Background(), devs); err != nil {
		t.Fatal(err)
	}
	proc := &pipeline.Processor{
		Validator: schema.NewValidator(), Normalizer: norm, Enricher: enrich.Enricher{Inventory: inventory.NewRegistry(devs)},
		States: defs, Rules: rs, Store: store, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	// The shipped JavaScript extensions are part of the pipeline under test.
	cfg, err := config.Load("../../configs/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scripting.Directories = []string{"../../scripts"}
	svc, rep := scripting.New(cfg.Scripting, cfg.Scripts, []runtime.Installer{api.Installer(api.Deps{Inventory: inventory.NewRegistry(devs)})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer svc.Close()
	if rep.Failed != 0 {
		t.Fatalf("scripts: %v", rep.Errors)
	}
	proc.Ext = &scripting.Extensions{Svc: svc, Validator: proc.Validator}
	sched := &collector.Scheduler{
		Devices: func() []domain.Device { return devs }, Poller: realPoller(t), Sink: pipeSink{t: t, p: proc},
		Workers: 6, DefaultInterval: time.Second, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = sched.Run(ctx) }()
	clockCtx, stopClock := context.WithCancel(ctx)
	go w.RunClock(clockCtx, 100*time.Millisecond)
	defer func() { stopClock(); cancel(); wg.Wait() }()

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	hasEvent := func(device, typ string) func() bool {
		return func() bool {
			for _, e := range store.Events() {
				if e.DeviceID == device && e.Type == typ {
					return true
				}
			}
			return false
		}
	}
	eventOf := func(device, typ string) domain.Event {
		for _, e := range store.Events() {
			if e.DeviceID == device && e.Type == typ {
				return e
			}
		}
		return domain.Event{}
	}

	// 1. Healthy baseline: several cycles, then no alert may exist.
	waitFor("baseline observations", func() bool { return store.ObservationCount() > 300 })
	if evs := store.Events(); len(evs) != 0 {
		t.Fatalf("a healthy lab must be silent, got %+v", evs)
	}

	// 2. Interface goes down -> interface.down after 2 consecutive samples.
	if _, err := w.Apply(Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/1"}}); err != nil {
		t.Fatal(err)
	}
	waitFor("interface.down", hasEvent("switch-01", "interface.down"))
	ev := eventOf("switch-01", "interface.down")
	if ev.Labels["interface"] != "Gi0/1" || ev.CorrelationID == "" || ev.Severity != domain.SeverityWarning {
		t.Fatalf("event: %+v", ev)
	}
	_, _ = w.Apply(Scenario{Device: "switch-01", Event: "interface-up", Args: map[string]string{"interface": "Gi0/1"}})
	waitFor("interface.up", hasEvent("switch-01", "interface.up"))

	// 3. Server CPU pinned -> state definition and windowed rule both fire.
	_, _ = w.Apply(Scenario{Device: "server-01", Event: "high-cpu"})
	waitFor("cpu.high", hasEvent("server-01", "cpu.high"))
	waitFor("cpu.sustained_high (rule)", hasEvent("server-01", "cpu.sustained_high"))
	_, _ = w.Apply(Scenario{Device: "server-01", Event: "cpu-normal"})

	// 3b. The AP reports vendor.cpu.load and tenths-of-a-degree temperature, which
	// no Go mapping understands. Only the JavaScript transforms turn them into
	// canonical metrics, so these events prove scripts run in the real pipeline.
	_, _ = w.Apply(Scenario{Device: "ap-01", Event: "high-cpu"})
	waitFor("cpu.high on the AP (via script)", hasEvent("ap-01", "cpu.high"))
	_, _ = w.Apply(Scenario{Device: "ap-01", Event: "cpu-normal"})
	_, _ = w.Apply(Scenario{Device: "ap-01", Event: "overheat"})
	waitFor("temperature.high on the AP (via script)", hasEvent("ap-01", "temperature.high"))
	_, _ = w.Apply(Scenario{Device: "ap-01", Event: "cool"})

	// 4. UPS loses mains -> critical event on the first sample.
	_, _ = w.Apply(Scenario{Device: "ups-01", Event: "power-loss"})
	waitFor("ups.on_battery", hasEvent("ups-01", "ups.on_battery"))

	// 5. AP stops answering -> polls fail, device.down only after 3 in a row.
	_, _ = w.Apply(Scenario{Device: "ap-01", Event: "offline"})
	waitFor("device.down", hasEvent("ap-01", "device.down"))
	if e := eventOf("ap-01", "device.down"); e.Severity != domain.SeverityCritical {
		t.Fatalf("device.down: %+v", e)
	}
	_, _ = w.Apply(Scenario{Device: "ap-01", Event: "online"})
	waitFor("device.up", hasEvent("ap-01", "device.up"))

	// Nothing unrelated fired: the other switch, the workstation stayed quiet.
	for _, e := range store.Events() {
		if e.DeviceID == "switch-02" || e.DeviceID == "workstation-01" {
			t.Fatalf("unrelated device raised %+v", e)
		}
	}
}
