package sim

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/automation/adapters"
	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/state"
)

const liveLabPolicies = `
allowlist: {devices: [switch-01]}
policies:
  - id: remediate-interface-down
    trigger: {event: interface.down, device_type: switch}
    conditions: {duration: 400ms, retries_below: 2}
    proposal: {script: interface-remediation, allowed_actions: [bounce_interface]}
    safety: {dry_run: false, cooldown: 1m, require_tag: automation-enabled}
`

// TestClosedLoopAlertToRemediationToRecovery drives the whole platform once:
//
//	simulated port goes down -> real SNMP poll -> pipeline -> interface.down alert
//	-> JavaScript proposes a bounce -> Go validates it and applies every gate
//	-> live adapter action against the simulator -> port is up again
//	-> next polls -> interface.up recovery, alert resolved.
func TestClosedLoopAlertToRemediationToRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("uses real UDP polling and wall-clock time")
	}
	devs := labDevices(t)
	w := NewWorld(devs, Options{Seed: 11})
	devs = startAgents(t, w, devs)
	for i := range devs {
		devs[i].Collection.Interval = 120 * time.Millisecond
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := inventory.NewRegistry(devs)
	store := persistence.NewMemStore()
	_ = store.UpsertDevices(context.Background(), devs)

	// scripts: transforms + enrichers + the remediation proposer
	cfg, err := config.Load("../../configs/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scripting.Directories = []string{"../../scripts"}
	svc, rep := scripting.New(cfg.Scripting, cfg.Scripts, []runtime.Installer{api.Installer(api.Deps{Inventory: reg, Log: log})}, log)
	defer svc.Close()
	if rep.Failed != 0 {
		t.Fatal(rep.Errors)
	}
	ext := &scripting.Extensions{Svc: svc, Validator: schema.NewValidator(), Log: log}

	norm, _ := normalize.LoadFile("../../configs/normalization.yaml")
	defs, _ := state.LoadFile("../../configs/states.yaml")
	proc := &pipeline.Processor{
		Validator: schema.NewValidator(), Normalizer: norm, Enricher: enrich.Enricher{Inventory: reg}, States: defs, Store: store, Ext: ext, Log: log,
	}

	pols, err := automation.Parse([]byte(liveLabPolicies))
	if err != nil {
		t.Fatal(err)
	}
	reader := &adapters.Reader{Dialer: realPoller(t).Dialer, Secrets: realPoller(t).Secrets}
	engine := &automation.Engine{
		Policies: pols, Store: store, Devices: reg, Proposer: ext, GlobalDryRun: false, // both switches turned: live
		Adapters: adapters.NewSet(DeviceController{C: w.Control()}, reader, &adapters.Generic{}), Log: log,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	sched := &collector.Scheduler{Devices: func() []domain.Device { return devs }, Poller: realPoller(t), Sink: pipeSink{t: t, p: proc},
		Workers: 6, DefaultInterval: time.Second, Log: log}
	wg.Add(2)
	go func() { defer wg.Done(); _ = sched.Run(ctx) }()
	go w.RunClock(ctx, 100*time.Millisecond)
	go func() { // stands in for the automation worker: alert events in, engine ticks
		defer wg.Done()
		seen := map[string]bool{}
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				for _, ev := range store.Events() {
					if ev.AlertKey != "" && !ev.Resolves && !seen[ev.EventID] {
						seen[ev.EventID] = true
						if _, err := engine.HandleEvent(ctx, ev); err != nil && ctx.Err() == nil {
							t.Errorf("handle: %v", err)
						}
					}
				}
				if _, err := engine.Tick(ctx); err != nil && ctx.Err() == nil {
					t.Errorf("tick: %v", err)
				}
			}
		}
	}()
	defer func() { cancel(); wg.Wait() }()

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	hasEvent := func(typ string) bool {
		for _, e := range store.Events() {
			if e.DeviceID == "switch-01" && e.Type == typ {
				return true
			}
		}
		return false
	}

	waitFor("baseline", func() bool { return store.ObservationCount() > 200 })
	// Gi0/1 is described as an uplink in the inventory: the script must decline
	// to touch it, while the ordinary port Gi0/2 gets remediated.
	_, _ = w.Apply(Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/2"}})

	waitFor("interface.down alert", func() bool { return hasEvent("interface.down") })
	waitFor("the remediation request to complete", func() bool {
		for _, r := range store.AutomationRequests() {
			if r.Status == domain.AutomationSucceeded {
				return true
			}
		}
		return false
	})
	waitFor("interface.up recovery (the port was healed by the live action)", func() bool { return hasEvent("interface.up") })

	reqs := store.AutomationRequests()
	if len(reqs) != 1 {
		t.Fatalf("exactly one automation request expected: %+v", reqs)
	}
	req := reqs[0]
	if req.Action != "bounce_interface" || req.Target != "Gi0/2" || req.Proposer != "script:interface-remediation" || req.DryRun || req.DeviceID != "switch-01" {
		t.Fatalf("%+v", req)
	}
	_, res, _ := store.GetAutomation(context.Background(), req.RequestID)
	if res == nil || res.Status != domain.AutomationSucceeded || res.DryRun || res.Details["changed"] != "true" || res.Details["before_oper"] != "down" {
		t.Fatalf("the audited result should record what the adapter found and did: %+v", res)
	}
	// correlation: the request carries the correlation id of the alert event it answers
	var down domain.Event
	for _, e := range store.Events() {
		if e.Type == "interface.down" && e.Labels["interface"] == "Gi0/2" {
			down = e
		}
	}
	if down.EventID == "" || req.EventID != down.EventID || req.CorrelationID != down.CorrelationID || res.CorrelationID != down.CorrelationID {
		t.Fatalf("one correlation id must trace poll -> event -> request -> result: event=%+v request=%+v", down, req)
	}
	for _, a := range store.Alerts() {
		if a.Source == "interface-operational" && a.Status != domain.AlertResolved {
			t.Fatalf("the alert should be resolved after recovery: %+v", a)
		}
	}
	// the uplink was never touched
	for _, r := range reqs {
		if r.Target == "Gi0/1" {
			t.Fatal("the uplink must never be remediated")
		}
	}
}
