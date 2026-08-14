package automation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

var t0 = time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC) // a Monday

type fakeAdapter struct {
	mu    sync.Mutex
	calls []Action
	err   error
	delay time.Duration
}

func (f *fakeAdapter) Capabilities(context.Context, domain.Device) ([]Capability, error) {
	return nil, nil
}
func (f *fakeAdapter) Execute(_ context.Context, _ domain.Device, a Action) (ActionResult, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	f.calls = append(f.calls, a)
	f.mu.Unlock()
	if f.err != nil {
		return ActionResult{}, f.err
	}
	msg := "executed " + a.Type
	if a.DryRun {
		msg = "dry-run: would execute " + a.Type
	}
	return ActionResult{Message: msg, Changed: !a.DryRun}, nil
}
func (f *fakeAdapter) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }
func (f *fakeAdapter) last() Action {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

type oneAdapter struct{ a DeviceAdapter }

func (o oneAdapter) For(domain.Device, string) (DeviceAdapter, error) { return o.a, nil }

type fakeProposer struct {
	prop *Proposal
	err  error
}

func (f fakeProposer) Propose(context.Context, Policy, domain.Event, domain.Device) (*Proposal, error) {
	return f.prop, f.err
}

type rig struct {
	e       *Engine
	store   *persistence.MemStore
	adapter *fakeAdapter
	now     time.Time
}

const policiesYAML = `
allowlist: {devices: [switch-01]}
maintenance_windows:
  - {id: patching, match: {tags: [patch-window]}, recurring: {days: [mon], start: "11:00", end: "13:00"}}
policies:
  - id: restart-agent
    trigger: {event: device.down, device_type: [server, workstation]}
    conditions: {duration: 10m, retries_below: 2}
    action: {type: restart_service, service: observer-agent}
    safety: {dry_run: true, cooldown: 30m, require_tag: automation-enabled}
  - id: bounce
    trigger: {event: interface.down, device_type: switch}
    conditions: {duration: 2m}
    action: {type: bounce_interface, target: "{interface}"}
    safety: {cooldown: 30m}
  - id: live-bounce
    trigger: {event: interface.flapping, device_type: switch}
    action: {type: bounce_interface, target: "{interface}"}
    safety: {dry_run: false, cooldown: 1m}
  - id: diagnose
    trigger: {event: cpu.sustained_high}
    action: {type: collect_diagnostics}
    safety: {dry_run: false, cooldown: 1m}
  - id: approve-me
    trigger: {event: ap.trouble, device_type: access_point}
    action: {type: restart_ap_service, service: radio}
    safety: {require_approval: true, approval_timeout: 30m, cooldown: 1m}
  - id: scripted
    trigger: {event: script.trouble}
    proposal: {script: helper, allowed_actions: [bounce_interface, noop]}
    safety: {cooldown: 1m}
`

func devices() []domain.Device {
	return []domain.Device{
		{ID: "switch-01", Hostname: "sw1", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Site: "lab", Enabled: true,
			Tags: []string{"automation-enabled"}, Capabilities: []string{"snmp", "interface-admin"}},
		{ID: "switch-02", Hostname: "sw2", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Site: "lab", Enabled: true,
			Tags: []string{"automation-enabled"}, Capabilities: []string{"snmp", "interface-admin"}},
		{ID: "notag-sw", Hostname: "n", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Enabled: true, Capabilities: []string{"interface-admin"}},
		{ID: "nocap-sw", Hostname: "n", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Enabled: true, Tags: []string{"automation-enabled"}},
		{ID: "server-01", Hostname: "srv", ManagementAddress: "x", DeviceType: domain.DeviceServer, Enabled: true,
			Tags: []string{"automation-enabled"}, Capabilities: []string{"service-restart", "diagnostics"}},
		{ID: "patch-srv", Hostname: "patch", ManagementAddress: "x", DeviceType: domain.DeviceServer, Enabled: true,
			Tags: []string{"automation-enabled", "patch-window"}, Capabilities: []string{"service-restart", "diagnostics"}},
		{ID: "ap-01", Hostname: "ap", ManagementAddress: "x", DeviceType: domain.DeviceAccessPoint, Enabled: true,
			Tags: []string{"automation-enabled"}, Capabilities: []string{"service-restart"}},
		{ID: "off", Hostname: "off", ManagementAddress: "x", DeviceType: domain.DeviceServer, Enabled: false, Tags: []string{"automation-enabled"}},
	}
}

func newRig(t *testing.T, globalDry bool) *rig {
	t.Helper()
	pols, err := Parse([]byte(policiesYAML))
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{store: persistence.NewMemStore(), adapter: &fakeAdapter{}, now: t0}
	r.e = &Engine{
		Policies: pols, Store: r.store, Devices: inventory.NewRegistry(devices()), Adapters: oneAdapter{r.adapter},
		GlobalDryRun: globalDry, Now: func() time.Time { return r.now }, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return r
}

var seq atomic.Int32

// openAlert records an alert-opening event in the store (so the "still
// firing" gate sees it) and returns the event.
func (r *rig) openAlert(t *testing.T, device, typ string, labels map[string]string, at time.Time) domain.Event {
	t.Helper()
	n := seq.Add(1)
	ev := domain.Event{
		EventID: fmt.Sprintf("evt-%d", n), CorrelationID: fmt.Sprintf("corr-%d", n), DeviceID: device, Type: typ, Severity: domain.SeverityWarning,
		Message: "m", Labels: labels, OccurredAt: at, Source: "test", AlertKey: fmt.Sprintf("%s/%s/%s", typ, device, domain.LabelsKey(labels)),
	}
	_ = r.store.Do(context.Background(), func(tx persistence.Tx) error {
		_, _ = tx.InsertEvent(context.Background(), ev)
		_, err := tx.OpenAlert(context.Background(), ev)
		return err
	})
	return ev
}

func (r *rig) resolve(ev domain.Event) {
	rec := ev
	rec.EventID, rec.Resolves = ev.EventID+"-r", true
	_ = r.store.Do(context.Background(), func(tx persistence.Tx) error { _, err := tx.ResolveAlert(context.Background(), rec); return err })
}

func (r *rig) handle(t *testing.T, ev domain.Event) []domain.AutomationRequest {
	t.Helper()
	got, err := r.e.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func (r *rig) tick(t *testing.T) int {
	t.Helper()
	n, err := r.e.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (r *rig) result(t *testing.T, id string) (*domain.AutomationRequest, *domain.AutomationResult) {
	t.Helper()
	req, res, err := r.store.GetAutomation(context.Background(), id)
	if err != nil || req == nil {
		t.Fatalf("request %s: %v", id, err)
	}
	return req, res
}

func TestRequestIsDelayedByDurationAndDryRunByDefault(t *testing.T) {
	r := newRig(t, true)
	ev := r.openAlert(t, "server-01", "device.down", nil, t0)
	reqs := r.handle(t, ev)
	if len(reqs) != 1 {
		t.Fatalf("%+v", reqs)
	}
	req := reqs[0]
	if req.Action != "restart_service" || req.Params["service"] != "observer-agent" || req.Proposer != "policy" || !req.DryRun ||
		req.Status != domain.AutomationPending || !req.NotBefore.Equal(t0.Add(10*time.Minute)) || req.CorrelationID != ev.CorrelationID || req.EventID != ev.EventID {
		t.Fatalf("%+v", req)
	}
	r.now = t0.Add(9 * time.Minute)
	if r.tick(t) != 0 || r.adapter.count() != 0 {
		t.Fatal("nothing may run before the duration has elapsed: the alert might heal by itself")
	}
	r.now = t0.Add(10*time.Minute + time.Second)
	if r.tick(t) != 1 {
		t.Fatal("due request not processed")
	}
	if r.adapter.count() != 1 || !r.adapter.last().DryRun {
		t.Fatalf("policy is dry-run: %+v", r.adapter.calls)
	}
	got, res := r.result(t, req.RequestID)
	if got.Status != domain.AutomationDryRun || res == nil || res.Status != domain.AutomationDryRun || !res.DryRun || res.CorrelationID != ev.CorrelationID {
		t.Fatalf("%+v %+v", got, res)
	}
	if a := r.adapter.last(); a.RequestID != req.RequestID || a.CorrelationID != ev.CorrelationID {
		t.Fatalf("adapter must receive the ids needed for tracing: %+v", a)
	}
}

func TestRedeliveredEventDoesNotCreateSecondRequestOrExecution(t *testing.T) {
	r := newRig(t, true)
	ev := r.openAlert(t, "server-01", "device.down", nil, t0)
	if len(r.handle(t, ev)) != 1 {
		t.Fatal("first delivery")
	}
	for i := 0; i < 3; i++ {
		if len(r.handle(t, ev)) != 0 {
			t.Fatal("redelivery must be a no-op")
		}
	}
	r.now = t0.Add(time.Hour)
	r.tick(t)
	r.tick(t)
	if len(r.store.AutomationRequests()) != 1 || r.adapter.count() != 1 {
		t.Fatalf("requests=%d executions=%d", len(r.store.AutomationRequests()), r.adapter.count())
	}
	var subjects []string
	_, _ = r.store.DrainOutbox(context.Background(), 10, func(_ context.Context, m []persistence.OutboxMessage) error {
		for _, x := range m {
			subjects = append(subjects, x.Subject)
		}
		return nil
	})
	if fmt.Sprint(subjects) != "[automation.request automation.result]" {
		t.Fatalf("one request and one result notification expected: %v", subjects)
	}
}

func TestOnlyOpeningEventsTriggerAutomation(t *testing.T) {
	r := newRig(t, true)
	ev := r.openAlert(t, "server-01", "device.down", nil, t0)
	rec := ev
	rec.EventID, rec.Resolves = "rec", true
	if len(r.handle(t, rec)) != 0 {
		t.Fatal("a recovery must never trigger remediation")
	}
	info := ev
	info.EventID, info.AlertKey = "info", ""
	if len(r.handle(t, info)) != 0 {
		t.Fatal("events that do not open alerts do not trigger automation")
	}
	other := r.openAlert(t, "server-01", "unrelated.event", nil, t0)
	if len(r.handle(t, other)) != 0 {
		t.Fatal("no policy matches")
	}
	ghost := ev
	ghost.EventID, ghost.DeviceID = "ghost", "nope"
	if len(r.handle(t, ghost)) != 0 {
		t.Fatal("unknown device")
	}
	sw := r.openAlert(t, "switch-01", "device.down", nil, t0)
	if len(r.handle(t, sw)) != 0 {
		t.Fatal("device_type filter: restart-agent is for servers and workstations")
	}
}

func TestAlertResolvedBeforeDueExpiresWithoutExecuting(t *testing.T) {
	r := newRig(t, true)
	ev := r.openAlert(t, "server-01", "device.down", nil, t0)
	req := r.handle(t, ev)[0]
	r.resolve(ev)
	r.now = t0.Add(11 * time.Minute)
	r.tick(t)
	got, res := r.result(t, req.RequestID)
	if got.Status != domain.AutomationExpired || res.Details["gate"] != "alert" || r.adapter.count() != 0 {
		t.Fatalf("%+v %+v calls=%d", got, res, r.adapter.count())
	}
}

func TestGatesDenyWithAnAuditedReason(t *testing.T) {
	cases := []struct {
		name   string
		device string
		event  string
		labels map[string]string
		when   time.Time
		gate   string
	}{
		{"missing tag", "notag-sw", "interface.down", map[string]string{"interface": "Gi0/1"}, t0, "tag"},
		{"missing capability", "nocap-sw", "interface.down", map[string]string{"interface": "Gi0/1"}, t0, "capability"},
		{"disabled device", "off", "device.down", nil, t0, "device"},
		{"maintenance window", "patch-srv", "device.down", nil, t0, "maintenance"}, // Monday 12:00 is inside 11:00-13:00
	}
	for _, c := range cases {
		r := newRig(t, true)
		// "off" is disabled, so it never matches at HandleEvent; force a request the way a stale one would exist
		ev := r.openAlert(t, c.device, c.event, c.labels, c.when)
		reqs := r.handle(t, ev)
		if c.device == "off" {
			reqs = []domain.AutomationRequest{{RequestID: "forced", CorrelationID: "c", PolicyID: "restart-agent", EventID: ev.EventID, AlertKey: ev.AlertKey,
				DeviceID: "off", Action: "restart_service", Params: map[string]string{"service": "x"}, Proposer: "policy", DryRun: true,
				Status: domain.AutomationPending, NotBefore: t0, CreatedAt: t0}}
			_, _ = r.store.InsertAutomationRequest(context.Background(), reqs[0])
		}
		if len(reqs) != 1 {
			t.Fatalf("%s: expected a request, got %+v", c.name, reqs)
		}
		r.now = c.when.Add(time.Hour)
		if c.gate == "maintenance" {
			r.now = c.when.Add(10*time.Minute + time.Second) // still inside the window
		}
		r.tick(t)
		got, res := r.result(t, reqs[0].RequestID)
		if got.Status != domain.AutomationDenied || res == nil || res.Details["gate"] != c.gate || res.Message == "" || r.adapter.count() != 0 {
			t.Errorf("%s: want denied by %s, got %+v %+v (calls=%d)", c.name, c.gate, got.Status, res, r.adapter.count())
		}
	}
}

func TestCooldownAndRetryLimit(t *testing.T) {
	r := newRig(t, true)
	run := func(at time.Time) (domain.AutomationRequest, *domain.AutomationResult) {
		r.now = at
		ev := r.openAlert(t, "server-01", "device.down", nil, at)
		req := r.handle(t, ev)[0]
		r.now = at.Add(11 * time.Minute)
		r.tick(t)
		_, res := r.result(t, req.RequestID)
		return req, res
	}
	// Patch window would deny; move to Tuesday so it does not interfere.
	base := t0.Add(24 * time.Hour)
	_, res1 := run(base)
	if res1.Status != domain.AutomationDryRun {
		t.Fatalf("first: %+v", res1)
	}
	// the same alert opens again 5 minutes later: inside the 30m cooldown
	_, res2 := run(base.Add(15 * time.Minute))
	if res2.Status != domain.AutomationDenied || res2.Details["gate"] != "cooldown" {
		t.Fatalf("cooldown must hold: %+v", res2)
	}
	// after the cooldown a second attempt is allowed (limit 2)
	_, res3 := run(base.Add(2 * time.Hour))
	if res3.Status != domain.AutomationDryRun {
		t.Fatalf("second attempt after cooldown: %+v", res3)
	}
	// the third attempt exceeds retries_below: 2 for the same alert key
	_, res4 := run(base.Add(4 * time.Hour))
	if res4.Status != domain.AutomationDenied || res4.Details["gate"] != "retries" {
		t.Fatalf("retry limit must hold: %+v", res4)
	}
	if r.adapter.count() != 2 {
		t.Fatalf("exactly two executions expected, got %d", r.adapter.count())
	}
}

func TestLiveExecutionNeedsTwoSwitchesAndAnAllowlist(t *testing.T) {
	flap := map[string]string{"interface": "Gi0/2"}
	// live-bounce says dry_run: false, but the global switch is still dry-run
	r := newRig(t, true)
	req := r.handle(t, r.openAlert(t, "switch-01", "interface.flapping", flap, t0))[0]
	if !req.DryRun {
		t.Fatal("global dry-run must win over the policy's dry_run: false")
	}
	r.tick(t)
	if r.adapter.count() != 1 || !r.adapter.last().DryRun {
		t.Fatalf("%+v", r.adapter.calls)
	}

	// both switches off: an allowlisted device is mutated for real
	r = newRig(t, false)
	req = r.handle(t, r.openAlert(t, "switch-01", "interface.flapping", flap, t0))[0]
	if req.DryRun {
		t.Fatal("both switches are off, the request is live")
	}
	r.tick(t)
	if r.adapter.count() != 1 || r.adapter.last().DryRun || r.adapter.last().Target != "Gi0/2" {
		t.Fatalf("%+v", r.adapter.calls)
	}
	if _, res := r.result(t, req.RequestID); res.Status != domain.AutomationSucceeded {
		t.Fatalf("%+v", res)
	}

	// a device that is NOT on the allowlist is never mutated live
	r = newRig(t, false)
	req = r.handle(t, r.openAlert(t, "switch-02", "interface.flapping", flap, t0))[0]
	r.tick(t)
	if _, res := r.result(t, req.RequestID); res.Status != domain.AutomationDenied || res.Details["gate"] != "allowlist" || r.adapter.count() != 0 {
		t.Fatalf("%+v calls=%d", res, r.adapter.count())
	}

	// read-only actions do not need the allowlist
	r = newRig(t, false)
	req = r.handle(t, r.openAlert(t, "server-01", "cpu.sustained_high", nil, t0))[0]
	r.tick(t)
	if _, res := r.result(t, req.RequestID); res.Status != domain.AutomationSucceeded || r.adapter.count() != 1 {
		t.Fatalf("%+v", res)
	}
}

func TestApprovalFlow(t *testing.T) {
	r := newRig(t, true)
	ev := r.openAlert(t, "ap-01", "ap.trouble", nil, t0)
	req := r.handle(t, ev)[0]
	r.tick(t)
	got, res := r.result(t, req.RequestID)
	if got.Status != domain.AutomationAwaitingApproval || res != nil || r.adapter.count() != 0 {
		t.Fatalf("must wait for a human: %+v %+v", got, res)
	}
	if err := r.e.Approve(context.Background(), "nope", "alice"); err == nil {
		t.Fatal("unknown request")
	}
	if err := r.e.Approve(context.Background(), req.RequestID, "  "); err == nil {
		t.Fatal("approver identity is required")
	}
	if err := r.e.Approve(context.Background(), req.RequestID, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := r.e.Approve(context.Background(), req.RequestID, "mallory"); err == nil {
		t.Fatal("cannot approve twice")
	}
	r.tick(t)
	got, res = r.result(t, req.RequestID)
	if got.Status != domain.AutomationDryRun || got.ApprovedBy != "alice" || res == nil || r.adapter.count() != 1 {
		t.Fatalf("%+v %+v", got, res)
	}
}

func TestUnapprovedRequestsExpire(t *testing.T) {
	r := newRig(t, true)
	req := r.handle(t, r.openAlert(t, "ap-01", "ap.trouble", nil, t0))[0]
	r.tick(t)
	r.now = t0.Add(29 * time.Minute)
	r.tick(t)
	if got, _ := r.result(t, req.RequestID); got.Status != domain.AutomationAwaitingApproval {
		t.Fatalf("%+v", got)
	}
	r.now = t0.Add(31 * time.Minute)
	r.tick(t)
	got, res := r.result(t, req.RequestID)
	if got.Status != domain.AutomationExpired || res == nil || res.Details["gate"] != "approval" || r.adapter.count() != 0 {
		t.Fatalf("%+v %+v", got, res)
	}
	if err := r.e.Approve(context.Background(), req.RequestID, "alice"); err == nil {
		t.Fatal("an expired request cannot be approved")
	}
}

// --- scripted proposals: the safety boundary --------------------------------

func TestScriptProposalsGoThroughTheSameValidation(t *testing.T) {
	ev := func(r *rig) domain.Event { return r.openAlert(t, "switch-01", "script.trouble", nil, t0) }
	cases := []struct {
		name     string
		prop     *Proposal
		err      error
		wantDeny string
		wantExec bool
	}{
		{"valid proposal is accepted", &Proposal{Action: "bounce_interface", Target: "Gi0/1", Reason: "stuck"}, nil, "", true},
		{"action outside the policy's allow list", &Proposal{Action: "disable_interface", Target: "Gi0/1"}, nil, "not among the actions policy", false},
		{"action outside the catalog", &Proposal{Action: "rm_rf", Target: "/"}, nil, "not among the actions policy", false},
		{"shell metacharacters in target", &Proposal{Action: "bounce_interface", Target: "Gi0/1; reboot"}, nil, "valid target", false},
		{"command substitution in target", &Proposal{Action: "bounce_interface", Target: "$(reboot)"}, nil, "valid target", false},
		{"unexpected parameter", &Proposal{Action: "bounce_interface", Target: "Gi0/1", Params: map[string]string{"force": "true"}}, nil, "no parameter", false},
		{"action for the wrong device type", &Proposal{Action: "noop", Target: "x"}, nil, "takes no target", false},
		{"oversize reason", &Proposal{Action: "noop", Reason: string(make([]byte, 400))}, nil, "reason is too long", false},
		{"proposer failure", nil, domain.Errorf(domain.CategoryScript, "script exploded"), "proposal failed", false},
	}
	for _, c := range cases {
		r := newRig(t, true)
		r.e.Proposer = fakeProposer{prop: c.prop, err: c.err}
		reqs := r.handle(t, ev(r))
		if len(reqs) != 1 {
			t.Fatalf("%s: every proposal outcome must be audited, got %d requests", c.name, len(reqs))
		}
		r.now = t0.Add(time.Hour)
		r.tick(t)
		got, res := r.result(t, reqs[0].RequestID)
		if c.wantExec {
			if got.Status != domain.AutomationDryRun || got.Proposer != "script:helper" || got.Reason != "stuck" || r.adapter.count() != 1 {
				t.Errorf("%s: %+v calls=%d", c.name, got, r.adapter.count())
			}
			continue
		}
		if got.Status != domain.AutomationDenied || res == nil || !containsFold(res.Message, c.wantDeny) || r.adapter.count() != 0 {
			t.Errorf("%s: want denied containing %q, got %+v %+v calls=%d", c.name, c.wantDeny, got.Status, res, r.adapter.count())
		}
	}
}

func containsFold(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestScriptProposingNothingCreatesNoRequest(t *testing.T) {
	r := newRig(t, true)
	r.e.Proposer = fakeProposer{}
	if got := r.handle(t, r.openAlert(t, "switch-01", "script.trouble", nil, t0)); len(got) != 0 {
		t.Fatalf("null proposal means no action: %+v", got)
	}
}

func TestScriptPolicyWithoutProposerIsDeniedNotCrashed(t *testing.T) {
	r := newRig(t, true)
	reqs := r.handle(t, r.openAlert(t, "switch-01", "script.trouble", nil, t0))
	if len(reqs) != 1 || reqs[0].Status != domain.AutomationDenied {
		t.Fatalf("%+v", reqs)
	}
}

// --- execution ----------------------------------------------------------------

func TestAdapterFailureIsRecordedAndNotRetried(t *testing.T) {
	r := newRig(t, true)
	r.adapter.err = domain.Errorf(domain.CategoryTimeout, "device did not answer")
	req := r.handle(t, r.openAlert(t, "server-01", "cpu.sustained_high", nil, t0))[0]
	r.tick(t)
	got, res := r.result(t, req.RequestID)
	if got.Status != domain.AutomationFailed || res.Details["error_category"] != "timeout" || res.Message == "" {
		t.Fatalf("%+v %+v", got, res)
	}
	r.now = t0.Add(time.Hour)
	r.tick(t)
	if r.adapter.count() != 1 {
		t.Fatal("automation is not a retry loop: a failed action is surfaced, not repeated")
	}
}

func TestConcurrentWorkersExecuteOnce(t *testing.T) {
	r := newRig(t, true)
	r.adapter.delay = 30 * time.Millisecond
	r.handle(t, r.openAlert(t, "server-01", "cpu.sustained_high", nil, t0))
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = r.e.Tick(context.Background()) }()
	}
	wg.Wait()
	if r.adapter.count() != 1 {
		t.Fatalf("the claim must be exclusive: executed %d times", r.adapter.count())
	}
}

func TestAbandonedClaimIsRecoveredAfterTheLease(t *testing.T) {
	r := newRig(t, true)
	req := r.handle(t, r.openAlert(t, "server-01", "cpu.sustained_high", nil, t0))[0]
	// a worker claims the request and then dies without completing it
	if ok, _ := r.store.ClaimAutomation(context.Background(), req.RequestID, t0, r.e.lease()); !ok {
		t.Fatal("claim")
	}
	r.now = t0.Add(time.Minute)
	if r.tick(t) != 0 {
		t.Fatal("still inside the lease: someone else may be working on it")
	}
	r.now = t0.Add(10 * time.Minute)
	if r.tick(t) != 1 || r.adapter.count() != 1 {
		t.Fatal("after the lease the request is re-executed (at-least-once; adapters must be idempotent)")
	}
}

func TestDatabaseOutageDuringGatesLeavesRequestRecoverable(t *testing.T) {
	r := newRig(t, true)
	req := r.handle(t, r.openAlert(t, "server-01", "cpu.sustained_high", nil, t0))[0]
	flaky := &flakyStore{MemStore: r.store, failAlertCheck: true}
	r.e.Store = flaky
	if _, err := r.e.Tick(context.Background()); err == nil {
		t.Fatal("the outage must surface to the caller")
	}
	got, _ := r.result(t, req.RequestID)
	if got.Status != domain.AutomationPending || r.adapter.count() != 0 {
		t.Fatalf("a platform failure must not deny or lose the request: %+v", got.Status)
	}
	flaky.failAlertCheck = false
	if _, err := r.e.Tick(context.Background()); err != nil || r.adapter.count() != 1 {
		t.Fatalf("recovery: %v calls=%d", err, r.adapter.count())
	}
}

type flakyStore struct {
	*persistence.MemStore
	failAlertCheck bool
}

func (f *flakyStore) AlertFiring(ctx context.Context, key string) (bool, error) {
	if f.failAlertCheck {
		return false, domain.Errorf(domain.CategoryDependency, "database unavailable")
	}
	return f.MemStore.AlertFiring(ctx, key)
}

func TestTemplatesExpandEventLabels(t *testing.T) {
	r := newRig(t, true)
	req := r.handle(t, r.openAlert(t, "switch-01", "interface.down", map[string]string{"interface": "Gi0/7"}, t0))[0]
	if req.Action != "bounce_interface" || req.Target != "Gi0/7" {
		t.Fatalf("%+v", req)
	}
	bad := r.handle(t, r.openAlert(t, "switch-01", "interface.down", map[string]string{"interface": "Gi0/1; reboot"}, t0))[0]
	if bad.Status != domain.AutomationDenied {
		t.Fatalf("a hostile label value must not become a target: %+v", bad)
	}
}

func TestOutcomeHooks(t *testing.T) {
	r := newRig(t, true)
	var reqs, results int
	r.e.OnRequest = func(string) { reqs++ }
	r.e.OnResult = func(string, domain.AutomationStatus, error) { results++ }
	r.handle(t, r.openAlert(t, "server-01", "cpu.sustained_high", nil, t0))
	r.tick(t)
	if reqs != 1 || results != 1 {
		t.Fatalf("%d %d", reqs, results)
	}
	_ = errors.New
}

func TestRetryablePlatformFailureFromProposerIsNotSwallowed(t *testing.T) {
	r := newRig(t, true)
	r.e.Proposer = fakeProposer{err: domain.Errorf(domain.CategoryTransient, "script executor saturated")}
	ev := r.openAlert(t, "switch-01", "script.trouble", nil, t0)
	if _, err := r.e.HandleEvent(context.Background(), ev); err == nil || !domain.IsRetryable(err) {
		t.Fatalf("a platform failure must fail the delivery so it is retried, got %v", err)
	}
	if len(r.store.AutomationRequests()) != 0 {
		t.Fatal("no denial may be recorded for something that was never actually decided")
	}
	// the retry, once the executor has capacity, works normally
	r.e.Proposer = fakeProposer{prop: &Proposal{Action: "noop"}}
	if reqs, err := r.e.HandleEvent(context.Background(), ev); err != nil || len(reqs) != 1 {
		t.Fatalf("%v %v", reqs, err)
	}
}
