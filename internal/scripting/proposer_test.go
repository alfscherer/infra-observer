package scripting

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

func downEvent(device, iface string) domain.Event {
	return domain.Event{
		EventID: "evt-" + device + iface, CorrelationID: "corr-1", DeviceID: device, Type: "interface.down", Severity: domain.SeverityWarning,
		Message: "down", Labels: map[string]string{"interface": iface, "if_index": "2"}, OccurredAt: t0, Source: "interface-operational",
		AlertKey: "interface-operational/" + device + "/interface=" + iface,
	}
}

var labSwitch = domain.Device{ID: "switch-01", Hostname: "switch-01.lab", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Site: "lab", Enabled: true,
	Tags: []string{"automation-enabled"}, Capabilities: []string{"interface-admin"},
	Attributes: map[string]string{"interface.Gi0/1.description": "uplink to switch-02"}}

func proposerPolicy(script string, allowed ...string) automation.Policy {
	return automation.Policy{ID: "p", Proposal: &automation.ProposalRef{Script: script, AllowedActions: allowed}}
}

func TestProposeDecodesAProposalOrNull(t *testing.T) {
	ext, _ := newExt(t, map[string]string{
		"automation/good": meta(`[]`) + `export function proposeAutomation(e, d, c) { return {action: "bounce_interface", target: e.labels.interface, reason: "r", params: {}} }`,
		"automation/none": meta(`[]`) + `export function proposeAutomation() { return null }`,
		"automation/ctx":  meta(`[]`) + `export function proposeAutomation(e, d, c) { return {action: c.allowed_actions[0], target: d.id + "-" + c.policy_id} }`,
	})
	p, err := ext.Propose(context.Background(), proposerPolicy("good", "bounce_interface"), downEvent("switch-01", "Gi0/3"), labSwitch)
	if err != nil || p == nil || p.Action != "bounce_interface" || p.Target != "Gi0/3" || p.Reason != "r" {
		t.Fatalf("%+v %v", p, err)
	}
	if p, err := ext.Propose(context.Background(), proposerPolicy("none", "noop"), downEvent("switch-01", "Gi0/3"), labSwitch); p != nil || err != nil {
		t.Fatalf("null means no proposal: %+v %v", p, err)
	}
	p, _ = ext.Propose(context.Background(), proposerPolicy("ctx", "noop", "x"), downEvent("switch-01", "Gi0/3"), labSwitch)
	if p == nil || p.Action != "noop" || p.Target != "switch-01-p" {
		t.Fatalf("scripts receive the policy id and allowed actions as context: %+v", p)
	}
}

func TestProposeRejectsMalformedProposalsAsScriptFailures(t *testing.T) {
	for name, body := range map[string]string{
		"string":        `"bounce it"`,
		"array":         `[{action: "noop"}]`,
		"unknown field": `{action: "noop", sudo: true}`,
		"params number": `{action: "noop", params: {n: 5}}`,
		"no action":     `{target: "x"}`,
	} {
		ext, svc := newExt(t, map[string]string{"automation/bad": meta(`[]`) + "export function proposeAutomation() { return " + body + " }"})
		p, err := ext.Propose(context.Background(), proposerPolicy("bad", "noop"), downEvent("switch-01", "Gi0/3"), labSwitch)
		if name == "no action" {
			// structurally a proposal; the engine's catalog validation rejects the empty action
			if err != nil || p == nil || p.Action != "" {
				t.Errorf("%s: %+v %v", name, p, err)
			}
			continue
		}
		if err == nil || p != nil || domain.CategoryOf(err) != domain.CategoryScript {
			t.Errorf("%s: want a script-category error, got %+v %v", name, p, err)
		}
		if s, _ := svc.Registry.Get("automation/bad"); s.TotalFailures != 1 {
			t.Errorf("%s: malformed output must count against the script", name)
		}
	}
}

func TestProposeOfInactiveScriptIsANamedError(t *testing.T) {
	ext, svc := newExt(t, map[string]string{"automation/x": meta(`[]`) + `export function proposeAutomation() { return null }`})
	svc.Registry.SetEnabled("automation/x", false)
	if _, err := ext.Propose(context.Background(), proposerPolicy("x", "noop"), downEvent("switch-01", "Gi0/3"), labSwitch); err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("%v", err)
	}
	if _, err := ext.Propose(context.Background(), proposerPolicy("missing", "noop"), downEvent("switch-01", "Gi0/3"), labSwitch); err == nil {
		t.Fatal("unknown script")
	}
}

// --- the safety boundary, end to end ------------------------------------------

type recordingAdapter struct {
	mu    sync.Mutex
	calls []automation.Action
}

func (r *recordingAdapter) Capabilities(context.Context, domain.Device) ([]automation.Capability, error) {
	return nil, nil
}
func (r *recordingAdapter) Execute(_ context.Context, _ domain.Device, a automation.Action) (automation.ActionResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, a)
	return automation.ActionResult{Message: "ok"}, nil
}

type oneAdapter struct{ a automation.DeviceAdapter }

func (o oneAdapter) For(domain.Device, string) (automation.DeviceAdapter, error) { return o.a, nil }

func engineWith(t *testing.T, ext *Extensions, policyYAML string) (*automation.Engine, *persistence.MemStore, *recordingAdapter, func(time.Time)) {
	t.Helper()
	pols, err := automation.Parse([]byte(policyYAML))
	if err != nil {
		t.Fatal(err)
	}
	store := persistence.NewMemStore()
	ad := &recordingAdapter{}
	now := t0
	e := &automation.Engine{
		Policies: pols, Store: store, Devices: inventory.NewRegistry([]domain.Device{labSwitch}), Adapters: oneAdapter{ad},
		Proposer: ext, GlobalDryRun: true, Now: func() time.Time { return now }, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return e, store, ad, func(at time.Time) { now = at }
}

func openAlert(t *testing.T, store *persistence.MemStore, ev domain.Event) {
	t.Helper()
	_ = store.Do(context.Background(), func(tx persistence.Tx) error {
		_, _ = tx.InsertEvent(context.Background(), ev)
		_, err := tx.OpenAlert(context.Background(), ev)
		return err
	})
}

const scriptedPolicy = `
policies:
  - id: p
    trigger: {event: interface.down}
    conditions: {duration: 2m}
    proposal: {script: SCRIPT, allowed_actions: [bounce_interface, enable_interface]}
    safety: {cooldown: 1m}
`

func TestShippedRemediationScriptThroughTheRealEngine(t *testing.T) {
	cfg, _ := config.Load("../../configs/config.yaml")
	cfg.Scripting.Directories = []string{"../../scripts"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, rep := New(cfg.Scripting, cfg.Scripts, []runtime.Installer{api.Installer(api.Deps{Log: log})}, log)
	defer svc.Close()
	if rep.Failed != 0 {
		t.Fatal(rep.Errors)
	}
	ext := &Extensions{Svc: svc, Validator: schema.NewValidator(), Log: log}
	e, store, ad, setNow := engineWith(t, ext, strings.Replace(scriptedPolicy, "SCRIPT", "interface-remediation", 1))

	ordinary := downEvent("switch-01", "Gi0/2")
	openAlert(t, store, ordinary)
	reqs, err := e.HandleEvent(context.Background(), ordinary)
	if err != nil || len(reqs) != 1 {
		t.Fatalf("%v %v", reqs, err)
	}
	r := reqs[0]
	if r.Proposer != "script:interface-remediation" || r.Action != "bounce_interface" || r.Target != "Gi0/2" || !r.DryRun || r.CorrelationID != "corr-1" {
		t.Fatalf("%+v", r)
	}
	uplink := downEvent("switch-01", "Gi0/1")
	openAlert(t, store, uplink)
	if reqs, _ := e.HandleEvent(context.Background(), uplink); len(reqs) != 0 {
		t.Fatalf("the script's judgment (never an uplink) must hold: %+v", reqs)
	}
	setNow(t0.Add(3 * time.Minute))
	if n, err := e.Tick(context.Background()); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	if len(ad.calls) != 1 || !ad.calls[0].DryRun || ad.calls[0].Type != "bounce_interface" || ad.calls[0].Target != "Gi0/2" {
		t.Fatalf("%+v", ad.calls)
	}
}

func TestHostileProposalsEndAsAuditedDenialsAndNeverReachAnAdapter(t *testing.T) {
	proposals := map[string]string{
		"action the policy did not allow": `{action: "disable_interface", target: "Gi0/2"}`,
		"action outside the catalog":      `{action: "run_shell", params: {cmd: "reboot"}}`,
		"shell metacharacters in target":  `{action: "bounce_interface", target: "Gi0/2; reboot"}`,
		"command substitution":            `{action: "bounce_interface", target: "$(reboot)"}`,
		"path traversal":                  `{action: "bounce_interface", target: "../../etc/passwd"}`,
		"unexpected parameter":            `{action: "bounce_interface", target: "Gi0/2", params: {force: "true"}}`,
		"missing target":                  `{action: "bounce_interface"}`,
		"host action for a switch":        `{action: "restart_service", params: {service: "x"}}`,
		"script that throws":              `(() => { throw new Error("boom") })()`,
		"script that loops":               `(() => { for(;;){} })()`,
	}
	for name, body := range proposals {
		ext, svc := newExt(t, map[string]string{"automation/evil": meta(`[]`) + "export function proposeAutomation(e, d, c) { return " + body + " }"})
		e, store, ad, setNow := engineWith(t, ext, strings.Replace(scriptedPolicy, "SCRIPT", "evil", 1))
		ev := downEvent("switch-01", "Gi0/2")
		openAlert(t, store, ev)
		reqs, err := e.HandleEvent(context.Background(), ev)
		if err != nil || len(reqs) != 1 {
			t.Fatalf("%s: every proposal outcome is audited: %v %v", name, reqs, err)
		}
		setNow(t0.Add(time.Hour))
		_, _ = e.Tick(context.Background())
		got, res, _ := store.GetAutomation(context.Background(), reqs[0].RequestID)
		if got.Status != domain.AutomationDenied || res == nil || res.Details["gate"] != "proposal" || res.Message == "" {
			t.Errorf("%s: want an audited denial, got %+v %+v", name, got.Status, res)
		}
		if len(ad.calls) != 0 {
			t.Errorf("%s: nothing may reach a device adapter: %+v", name, ad.calls)
		}
		_ = svc
	}
}

func TestScriptThatAlwaysMisbehavesIsEventuallyQuarantinedAndPoliciesKeepWorking(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "automation", "flaky", meta(`[]`)+`export function proposeAutomation() { throw new Error("boom") }`)
	cfg := config.Default().Scripting
	cfg.Directories, cfg.QuarantineAfter = []string{dir}, 3
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, _ := New(cfg, nil, nil, log)
	defer svc.Close()
	ext := &Extensions{Svc: svc, Validator: schema.NewValidator(), Log: log}
	e, store, _, _ := engineWith(t, ext, strings.Replace(scriptedPolicy, "SCRIPT", "flaky", 1))
	for i := 0; i < 5; i++ {
		ev := downEvent("switch-01", fmt.Sprintf("Gi0/%d", i+1))
		openAlert(t, store, ev)
		if _, err := e.HandleEvent(context.Background(), ev); err != nil {
			t.Fatalf("the engine must keep processing events: %v", err)
		}
	}
	s, _ := svc.Registry.Get("automation/flaky")
	if s.Status != registry.StatusQuarantined {
		t.Fatalf("%+v", s)
	}
	for _, r := range store.AutomationRequests() {
		if r.Status != domain.AutomationDenied {
			t.Fatalf("%+v", r)
		}
	}
}

// --- integrations ---------------------------------------------------------------

type hookServer struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []*http.Request
	body []string
	code int
}

func newHook(t *testing.T) *hookServer {
	h := &hookServer{code: 202}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.reqs, h.body = append(h.reqs, r), append(h.body, string(b))
		code := h.code
		h.mu.Unlock()
		w.WriteHeader(code)
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *hookServer) count() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.reqs) }

func integrationExt(t *testing.T, files map[string]string, settings map[string]map[string]config.ScriptSettings, h *hookServer) *Extensions {
	t.Helper()
	dir := t.TempDir()
	for name, src := range files {
		parts := strings.SplitN(name, "/", 2)
		writeScript(t, dir, parts[0], parts[1], src)
	}
	cfg := config.Default().Scripting
	cfg.Directories, cfg.QuarantineAfter, cfg.ExecutionTimeout = []string{dir}, 1000, 500*time.Millisecond
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	deps := api.Deps{
		Inventory: inventory.NewRegistry([]domain.Device{labSwitch}), Log: log,
		Endpoints: map[string]config.Endpoint{"hook": {URLRef: "endpoints/hook", Timeout: time.Second}},
		Secrets:   secrets.Static{"endpoints/hook": {"url": h.srv.URL, "token": "tok"}},
	}
	svc, rep := New(cfg, settings, []runtime.Installer{api.Installer(deps)}, log)
	t.Cleanup(svc.Close)
	if rep.Failed != 0 {
		t.Fatal(rep.Errors)
	}
	return &Extensions{Svc: svc, Validator: schema.NewValidator(), Log: log}
}

func webhookSource(t *testing.T) string {
	b, err := os.ReadFile(filepath.Join("../../scripts/integrations/webhook.js"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestShippedWebhookIntegration(t *testing.T) {
	h := newHook(t)
	ext := integrationExt(t, map[string]string{"integrations/webhook": webhookSource(t)},
		map[string]map[string]config.ScriptSettings{"integrations": {"webhook": {Config: map[string]any{"endpoint_ref": "hook"}}}}, h)
	crit := domain.Event{EventID: "e1", CorrelationID: "corr-7", DeviceID: "switch-01", Type: "device.down", Severity: domain.SeverityCritical, Message: "switch-01 is unreachable", OccurredAt: t0, Source: "s", AlertKey: "k"}
	out, err := ext.HandleEvent(context.Background(), crit, labSwitch)
	if err != nil || len(out) != 1 || !out[0].Result.OK || out[0].Err != nil {
		t.Fatalf("%+v %v", out, err)
	}
	if h.count() != 1 {
		t.Fatal("no request")
	}
	req := h.reqs[0]
	if req.Method != "POST" || req.URL.Path != "/v1/alerts" || req.Header.Get("Authorization") != "Bearer tok" || req.Header.Get("X-Request-Id") != "corr-7" || req.Header.Get("Idempotency-Key") == "" {
		t.Fatalf("%s %s %v", req.Method, req.URL.Path, req.Header)
	}
	if !strings.Contains(h.body[0], `"hostname":"switch-01.lab"`) || !strings.Contains(h.body[0], `"severity":"critical"`) {
		t.Fatalf("%s", h.body[0])
	}
	// redelivery of the same event carries the same idempotency key
	_, _ = ext.HandleEvent(context.Background(), crit, labSwitch)
	if h.reqs[1].Header.Get("Idempotency-Key") != req.Header.Get("Idempotency-Key") {
		t.Fatal("at-least-once delivery needs a stable idempotency key so the receiver can de-duplicate")
	}
	// below min_severity: the script is never even invoked
	info := crit
	info.EventID, info.Severity = "e2", domain.SeverityInfo
	before := ext.Svc.Pool.Stats().Calls
	out, _ = ext.HandleEvent(context.Background(), info, labSwitch)
	if len(out) != 0 || ext.Svc.Pool.Stats().Calls != before {
		t.Fatal("severity filter is applied in Go before any JavaScript runs")
	}
}

func TestIntegrationFailuresAreIsolatedAndReported(t *testing.T) {
	h := newHook(t)
	h.code = 500
	ext := integrationExt(t, map[string]string{
		"integrations/a-throws": `export const meta = {version: "1", events: ["*"]}` + "\nexport function handleEvent() { throw new Error('bug') }\n",
		"integrations/b-bad":    `export const meta = {version: "1", events: ["*"]}` + "\nexport function handleEvent() { return 42 }\n",
		"integrations/c-hook":   webhookSource(t),
	}, map[string]map[string]config.ScriptSettings{"integrations": {"c-hook": {Config: map[string]any{"endpoint_ref": "hook"}}}}, h)
	ev := domain.Event{EventID: "e1", CorrelationID: "c", DeviceID: "switch-01", Type: "device.down", Severity: domain.SeverityCritical, Message: "m", OccurredAt: t0, Source: "s"}
	out, err := ext.HandleEvent(context.Background(), ev, labSwitch)
	if err != nil || len(out) != 3 {
		t.Fatalf("%+v %v", out, err)
	}
	if out[0].Err == nil || out[1].Err == nil {
		t.Fatalf("broken integrations are reported: %+v", out)
	}
	if out[2].Err != nil || out[2].Result.OK || !strings.Contains(out[2].Result.Message, "500") {
		t.Fatalf("a failing webhook is a reported result, not a crash: %+v", out[2])
	}
}

func TestIntegrationsMustDeclareEvents(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "integrations", "greedy", `export const meta = {version: "1"}`+"\nexport function handleEvent() { return {ok: true} }\n")
	cfg := config.Default().Scripting
	cfg.Directories = []string{dir}
	svc, rep := New(cfg, nil, nil, slog.New(slog.DiscardHandler))
	defer svc.Close()
	if rep.Failed != 1 || !strings.Contains(strings.Join(rep.Errors, ";"), "meta.events") {
		t.Fatalf("%+v", rep)
	}
}

func TestIntegrationEmittedEventsAreStoredOnceAndNotifiedOnce(t *testing.T) {
	h := newHook(t)
	ext := integrationExt(t, map[string]string{"integrations/ticketer": `export const meta = {version: "1", events: ["device.down"]}
export function handleEvent(event, context) {
  events.emit({type: "ticket.created", severity: "info", message: "ticket opened for " + event.device_id, labels: {system: "helpdesk"}});
  return {ok: true};
}`}, nil, h)
	store := persistence.NewMemStore()
	runner := IntegrationRunner{Ext: ext, Log: slog.New(slog.DiscardHandler), Persist: func(ctx context.Context, ev domain.Event) error { return pipeline.PersistEvent(ctx, store, ev) }}
	ev := domain.Event{EventID: "e1", CorrelationID: "corr-1", DeviceID: "switch-01", Type: "device.down", Severity: domain.SeverityCritical, Message: "m", OccurredAt: t0, Source: "s", AlertKey: "k"}
	for i := 0; i < 3; i++ { // at-least-once: the same event delivered three times
		if err := runner.Run(context.Background(), ev, labSwitch); err != nil {
			t.Fatal(err)
		}
	}
	evs := store.Events()
	if len(evs) != 1 || evs[0].Type != "ticket.created" || evs[0].Source != "script:ticketer" || evs[0].CorrelationID != "corr-1" || evs[0].DeviceID != "switch-01" {
		t.Fatalf("%+v", evs)
	}
	if store.OutboxPending() != 1 {
		t.Fatalf("one notification expected, got %d", store.OutboxPending())
	}
}

// --- worker ---------------------------------------------------------------------

func TestWorkerRejectsPoisonAndProcessesEvents(t *testing.T) {
	e, store, _, _ := engineWith(t, nil, `
policies:
  - {id: noop-policy, trigger: {event: interface.down}, action: {type: noop}, safety: {cooldown: 1m}}
`)
	w := &automation.Worker{Engine: e, Devices: inventory.NewRegistry([]domain.Device{labSwitch})}
	for name, body := range map[string]string{
		"not json":       `nope`,
		"unknown field":  `{"event_id":"e","device_id":"d","type":"t","surprise":1}`,
		"missing fields": `{"event_id":"e"}`,
	} {
		err := w.Handle(context.Background(), []byte(body))
		if err == nil || domain.IsRetryable(err) {
			t.Errorf("%s: poison must be a permanent validation error, got %v", name, err)
		}
	}
	ev := downEvent("switch-01", "Gi0/2")
	openAlert(t, store, ev)
	payload, _ := schema.Encode(ev)
	for i := 0; i < 3; i++ {
		if err := w.Handle(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(store.AutomationRequests()); n != 1 {
		t.Fatalf("redelivery must not create duplicate requests, got %d", n)
	}
}
