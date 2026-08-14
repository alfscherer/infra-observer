package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

type fakeState struct {
	latest map[string]*domain.Observation
}

func (f fakeState) Latest(_ context.Context, dev, metric string, labels map[string]string) (*domain.Observation, error) {
	return f.latest[dev+"|"+metric+"|"+domain.LabelsKey(labels)], nil
}

type rig struct {
	svc   *scripting.Service
	logs  *bytes.Buffer
	logMu *sync.Mutex
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func devices() []domain.Device {
	return []domain.Device{
		{ID: "switch-01", Hostname: "switch-01.lab", ManagementAddress: "10.0.0.1", DeviceType: domain.DeviceSwitch, Site: "lab",
			Tags: []string{"automation-enabled"}, Capabilities: []string{"snmp"}, CredentialsRef: "snmp/lab", Enabled: true,
			Attributes: map[string]string{"role": "core"}},
		{ID: "server-01", Hostname: "server-01.lab", ManagementAddress: "10.0.0.2", DeviceType: domain.DeviceServer, Site: "lab", Enabled: true},
		{ID: "server-02", Hostname: "server-02.dc", ManagementAddress: "10.0.1.2", DeviceType: domain.DeviceServer, Site: "dc", Enabled: false},
	}
}

func setup(t *testing.T, files map[string]string, settings map[string]map[string]config.ScriptSettings, deps api.Deps) (*scripting.Service, *syncBuf) {
	t.Helper()
	dir := t.TempDir()
	for name, src := range files { // name is kind/id
		p := filepath.Join(dir, name+".js")
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte("export const meta = {version: \"1\", description: \"t\", metrics: [\"*\"], events: [\"*\"]}\n"+src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	logs := &syncBuf{}
	deps.Log = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if deps.Inventory == nil {
		deps.Inventory = inventory.NewRegistry(devices())
	}
	cfg := config.Default().Scripting
	cfg.Directories, cfg.Workers, cfg.ExecutionTimeout, cfg.QuarantineAfter = []string{dir}, 2, 500*time.Millisecond, 10000
	svc, rep := scripting.New(cfg, settings, []runtime.Installer{api.Installer(deps)}, deps.Log)
	t.Cleanup(svc.Close)
	if rep.Failed != 0 {
		t.Fatalf("scripts failed to load: %v", rep.Errors)
	}
	return svc, logs
}

func invoke(t *testing.T, svc *scripting.Service, key, fn string, cfg map[string]any, args ...any) (json.RawMessage, *api.Call, error) {
	t.Helper()
	sc, _ := svc.Registry.Get(key)
	call := api.NewCall(api.ScriptInfo{Key: key, Kind: sc.Kind, ID: sc.ID, Version: sc.Version, Config: cfg}, "corr-1", "trigger-1", "switch-01")
	out, err := svc.Call(context.Background(), key, fn, call, args...)
	return out, call, err
}

func TestReadOnlyFunctions(t *testing.T) {
	obs := &domain.Observation{Value: 0.42, Labels: map[string]string{"interface": "Gi0/1"}, ObservedAt: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)}
	svc, _ := setup(t, map[string]string{"enrichers/reader": `
export function enrich(o) {
  const d = device.get("switch-01");
  return {
    device: d,
    missing: device.get("nope"),
    lab: inventory.lookup({site: "lab"}).map(x => x.id),
    servers: inventory.lookup({device_type: "server", enabled: true}).map(x => x.id),
    tagged: inventory.lookup({tag: "automation-enabled"}).map(x => x.id),
    all: inventory.lookup().length,
    latest: state.get("switch-01", "system.cpu.utilization"),
    unknown: state.get("switch-01", "no.metric"),
    cfg: config.get("threshold"),
    noCfg: config.get("nothing"),
  };
}`}, nil, api.Deps{State: fakeState{latest: map[string]*domain.Observation{"switch-01|system.cpu.utilization|": obs}}})

	out, _, err := invoke(t, svc, "enrichers/reader", "enrich", map[string]any{"threshold": 0.9}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Device  map[string]any `json:"device"`
		Missing any            `json:"missing"`
		Lab     []string       `json:"lab"`
		Servers []string       `json:"servers"`
		Tagged  []string       `json:"tagged"`
		All     int            `json:"all"`
		Latest  map[string]any `json:"latest"`
		Unknown any            `json:"unknown"`
		Cfg     float64        `json:"cfg"`
		NoCfg   any            `json:"noCfg"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("%s: %v", out, err)
	}
	if got.Device["hostname"] != "switch-01.lab" || got.Device["site"] != "lab" || got.Device["attributes"].(map[string]any)["role"] != "core" {
		t.Fatalf("device: %v", got.Device)
	}
	if _, leaked := got.Device["credentials_ref"]; leaked {
		t.Fatal("credentials_ref must never be visible to scripts")
	}
	if got.Missing != nil || got.Unknown != nil || got.NoCfg != nil {
		t.Fatalf("absent things are null, not errors: %+v", got)
	}
	if strings.Join(got.Lab, ",") != "server-01,switch-01" || strings.Join(got.Servers, ",") != "server-01" ||
		strings.Join(got.Tagged, ",") != "switch-01" || got.All != 3 {
		t.Fatalf("lookup: %+v", got)
	}
	if got.Latest["value"] != 0.42 || got.Latest["observed_at"] != "2026-08-01T12:00:00Z" {
		t.Fatalf("state.get: %v", got.Latest)
	}
	if got.Cfg != 0.9 {
		t.Fatalf("config.get returns the script's own config: %v", got.Cfg)
	}
}

func TestReturnedValuesAreCopiesNotGoMemory(t *testing.T) {
	svc, _ := setup(t, map[string]string{"enrichers/mut": `
export function enrich() {
  const d = device.get("switch-01");
  d.tags.push("injected"); d.attributes.role = "hacked"; d.hostname = "x";
  return device.get("switch-01");
}`}, nil, api.Deps{})
	out, _, err := invoke(t, svc, "enrichers/mut", "enrich", nil, 1)
	if err != nil || strings.Contains(string(out), "injected") || strings.Contains(string(out), "hacked") {
		t.Fatalf("a script mutating what the host gave it must not affect anything else: %s %v", out, err)
	}
	if d, _ := inventory.NewRegistry(devices()).Get("switch-01"); d.Attributes["role"] != "core" {
		t.Fatal("inventory corrupted")
	}
}

func TestLoggingIsStructuredBudgetedAndCannotForgeFields(t *testing.T) {
	svc, logs := setup(t, map[string]string{"transforms/chatty": `
export function transform(o) {
  log.info("hello", {device_id: "forged", component: "forged", n: 5, nested: {a: 1}});
  for (let i = 0; i < 100; i++) log.debug("spam " + i);
  log.warn("x".repeat(5000));
  return o;
}`}, nil, api.Deps{})
	if _, _, err := invoke(t, svc, "transforms/chatty", "transform", nil, 1); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	var scriptLines int
	for _, l := range lines {
		var rec map[string]any
		if json.Unmarshal([]byte(l), &rec) != nil || rec["component"] != "script" {
			continue
		}
		scriptLines++
		if rec["script_id"] != "chatty" || rec["script_version"] != "1" || rec["device_id"] != "switch-01" {
			t.Fatalf("platform fields must be authoritative: %s", l)
		}
		if rec["msg"] == "hello" {
			f := rec["fields"].(map[string]any)
			if f["device_id"] != "forged" || f["n"] != "5" {
				t.Fatalf("script fields live under their own group: %s", l)
			}
		}
	}
	if scriptLines != api.MaxLogLines {
		t.Fatalf("log budget: %d lines emitted, want %d", scriptLines, api.MaxLogLines)
	}
	if strings.Contains(logs.String(), strings.Repeat("x", api.MaxLogMessage+1)) {
		t.Fatal("log message must be truncated")
	}
}

func TestHostAPIIsUnavailableDuringLoad(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "transforms"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "transforms", "eager.js"), []byte(`export const meta = {version: "1", metrics: ["*"]}
const d = device.get("switch-01")
export function transform(o) { return o }`), 0o644)
	cfg := config.Default().Scripting
	cfg.Directories = []string{dir}
	svc, rep := scripting.New(cfg, nil, []runtime.Installer{api.Installer(api.Deps{Inventory: inventory.NewRegistry(devices())})}, slog.New(slog.DiscardHandler))
	defer svc.Close()
	if rep.Failed != 1 {
		t.Fatalf("a script that calls the host API while loading must be rejected: %+v", rep)
	}
}

func TestKindBasedPermissions(t *testing.T) {
	src := func(body string) string { return "export function %s(x) {\n" + body + "\n}" }
	files := map[string]string{
		"transforms/t":   strings.Replace(src("events.emit({type:'a.b', severity:'info', message:'m'}); return x"), "%s", "transform", 1),
		"enrichers/e":    strings.Replace(src("return http.request({endpoint:'x', path:'/'})"), "%s", "enrich", 1),
		"automation/a":   strings.Replace(src("metrics.emit({metric:'a.b', value:1}); return null"), "%s", "proposeAutomation", 1),
		"integrations/i": strings.Replace(src("events.emit({type:'ticket.created', severity:'info', message:'m'}); return {ok:true}"), "%s", "handleEvent", 1),
	}
	svc, _ := setup(t, files, nil, api.Deps{})
	for key, fn := range map[string]string{"transforms/t": "transform", "enrichers/e": "enrich", "automation/a": "proposeAutomation"} {
		_, _, err := invoke(t, svc, key, fn, nil, 1)
		var se *runtime.Error
		if !errors.As(err, &se) || !strings.Contains(se.Message, "not available") {
			t.Errorf("%s must be denied its I/O capability: %v", key, err)
		}
	}
	if _, call, err := invoke(t, svc, "integrations/i", "handleEvent", nil, 1); err != nil || len(call.Events()) != 1 {
		t.Fatalf("integration may emit events: %v", err)
	}
}

func TestEventsEmitValidationAndDeterminism(t *testing.T) {
	svc, _ := setup(t, map[string]string{"integrations/emitter": `
export function handleEvent(spec) { events.emit(spec); return true }`}, nil, api.Deps{Now: func() time.Time { return time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC) }})
	good := map[string]any{"type": "ticket.created", "severity": "warning", "message": "opened", "labels": map[string]string{"id": "T-1"}}
	_, call, err := invoke(t, svc, "integrations/emitter", "handleEvent", nil, good)
	if err != nil || len(call.Events()) != 1 {
		t.Fatal(err)
	}
	ev := call.Events()[0]
	if ev.DeviceID != "switch-01" || ev.Source != "script:emitter" || ev.CorrelationID != "corr-1" || ev.Severity != domain.SeverityWarning || ev.EventID == "" || ev.Labels["id"] != "T-1" {
		t.Fatalf("%+v", ev)
	}
	_, call2, _ := invoke(t, svc, "integrations/emitter", "handleEvent", nil, good)
	if call2.Events()[0].EventID != ev.EventID {
		t.Fatal("re-handling the same trigger must yield the same event id (idempotent)")
	}
	bad := map[string]map[string]any{
		"type":         {"type": "Not Valid", "severity": "info", "message": "m"},
		"severity":     {"type": "a.b", "severity": "loud", "message": "m"},
		"device":       {"type": "a.b", "severity": "info", "message": "m", "device_id": "ghost"},
		"unknown key":  {"type": "a.b", "severity": "info", "message": "m", "extra": 1},
		"long message": {"type": "a.b", "severity": "info", "message": strings.Repeat("x", 600)},
		"many labels":  {"type": "a.b", "severity": "info", "message": "m", "labels": manyLabels(30)},
	}
	for name, spec := range bad {
		if _, _, err := invoke(t, svc, "integrations/emitter", "handleEvent", nil, spec); err == nil {
			t.Errorf("%s: invalid emission must be rejected", name)
		}
	}
	svc2, _ := setup(t, map[string]string{"integrations/flood": `
export function handleEvent() { for (let i = 0; i < 50; i++) events.emit({type: "a.b", severity: "info", message: "m", labels: {i: String(i)}}); }`}, nil, api.Deps{})
	if _, call, err := invoke(t, svc2, "integrations/flood", "handleEvent", nil); err == nil || len(call.Events()) != api.MaxEvents {
		t.Fatalf("per-invocation event budget: %v events=%d", err, len(call.Events()))
	}
}

func manyLabels(n int) map[string]string {
	m := map[string]string{}
	for i := 0; i < n; i++ {
		m[strings.Repeat("k", i+1)] = "v"
	}
	return m
}

func TestMetricsEmitIsValidated(t *testing.T) {
	svc, _ := setup(t, map[string]string{"collectors/c": `
export function collect(spec) { metrics.emit(spec); return null }`}, nil, api.Deps{})
	_, call, err := invoke(t, svc, "collectors/c", "collect", nil, map[string]any{"metric": "vendor.queue.depth", "value": 12, "labels": map[string]string{"q": "a"}})
	if err != nil || len(call.Observations()) != 1 {
		t.Fatal(err)
	}
	o := call.Observations()[0]
	if o.Source != "script:c" || o.DeviceID != "switch-01" || o.Metric != "vendor.queue.depth" || o.Value != 12.0 || o.ObservationID == "" || o.CorrelationID != "corr-1" {
		t.Fatalf("%+v", o)
	}
	for name, spec := range map[string]map[string]any{
		"bad metric":    {"metric": "Bad Name", "value": 1},
		"object value":  {"metric": "a.b", "value": map[string]any{"x": 1}},
		"missing value": {"metric": "a.b"},
		"unknown dev":   {"metric": "a.b", "value": 1, "device_id": "ghost"},
	} {
		if _, _, err := invoke(t, svc, "collectors/c", "collect", nil, spec); err == nil {
			t.Errorf("%s: expected rejection (script output goes through the same validation as any observation)", name)
		}
	}
}

// --- http.request -------------------------------------------------------------

type httpRig struct {
	svc      *scripting.Service
	srv      *httptest.Server
	hits     atomic.Int32
	lastReq  atomic.Pointer[http.Request]
	lastBody atomic.Pointer[string]
}

func newHTTPRig(t *testing.T, handler http.HandlerFunc, script string, cfg map[string]any) *httpRig {
	t.Helper()
	r := &httpRig{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.hits.Add(1)
		r.lastReq.Store(req)
		b := new(bytes.Buffer)
		_, _ = b.ReadFrom(req.Body)
		s := b.String()
		r.lastBody.Store(&s)
		handler(w, req)
	}))
	t.Cleanup(r.srv.Close)
	sec := secrets.Static{"endpoints/hook": {"url": r.srv.URL + "/base", "token": "s3cret-token"}}
	settings := map[string]map[string]config.ScriptSettings{"integrations": {"h": {Config: cfg}}}
	r.svc, _ = setup(t, map[string]string{"integrations/h": script}, settings, api.Deps{
		Endpoints: map[string]config.Endpoint{"hook": {URLRef: "endpoints/hook", Timeout: 300 * time.Millisecond}, "other": {URLRef: "endpoints/other"}},
		Secrets:   sec,
	})
	return r
}

const hookScript = `export function handleEvent(req) { return http.request(req) }`

func hookCfg() map[string]any { return map[string]any{"endpoint_ref": "hook"} }

func TestHTTPRequestHappyPathKeepsSecretsInGo(t *testing.T) {
	r := newHTTPRig(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"id": 7}`))
	}, hookScript, hookCfg())
	out, _, err := invoke(t, r.svc, "integrations/h", "handleEvent", hookCfg(),
		map[string]any{"endpoint": "hook", "method": "POST", "path": "/v1/events?x=1", "body": map[string]any{"a": 1}, "headers": map[string]string{"Accept": "application/json"}})
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	if resp["status"] != 201.0 || resp["ok"] != true || resp["body"] != `{"id": 7}` || resp["content_type"] != "application/json" {
		t.Fatalf("response: %v", resp)
	}
	if strings.Contains(string(out), "s3cret-token") || strings.Contains(string(out), r.srv.URL) {
		t.Fatal("neither the token nor the endpoint URL may reach the script")
	}
	req := r.lastReq.Load()
	if req.URL.Path != "/base/v1/events" || req.URL.RawQuery != "x=1" || req.Method != "POST" {
		t.Fatalf("request: %s %s", req.Method, req.URL)
	}
	if req.Header.Get("Authorization") != "Bearer s3cret-token" {
		t.Fatal("Go must attach the credential")
	}
	if req.Header.Get("Content-Type") != "application/json" || req.Header.Get("X-Request-Id") != "corr-1" {
		t.Fatalf("headers: %v", req.Header)
	}
	if *r.lastBody.Load() != `{"a":1}` {
		t.Fatalf("body: %s", *r.lastBody.Load())
	}
}

func TestHTTPRequestPolicyDenials(t *testing.T) {
	r := newHTTPRig(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }, hookScript, hookCfg())
	denied := map[string]map[string]any{
		"endpoint not granted to script": {"endpoint": "other", "path": "/x"},
		"endpoint does not exist":        {"endpoint": "nowhere", "path": "/x"},
		"arbitrary url in endpoint":      {"endpoint": "http://127.0.0.1:1", "path": "/x"},
		"absolute url as path":           {"endpoint": "hook", "path": "http://evil.example/x"},
		"protocol relative path":         {"endpoint": "hook", "path": "//evil.example/x"},
		"relative path":                  {"endpoint": "hook", "path": "v1"},
		"newline in path":                {"endpoint": "hook", "path": "/x\r\nHost: evil"},
		"bad method":                     {"endpoint": "hook", "path": "/x", "method": "DELETE"},
		"authorization header":           {"endpoint": "hook", "path": "/x", "headers": map[string]string{"Authorization": "Bearer stolen"}},
		"host header":                    {"endpoint": "hook", "path": "/x", "headers": map[string]string{"Host": "evil"}},
		"oversize body":                  {"endpoint": "hook", "path": "/x", "method": "POST", "body": strings.Repeat("x", api.MaxRequestBody+1)},
		"unknown field":                  {"endpoint": "hook", "path": "/x", "proxy": "evil"},
	}
	for name, spec := range denied {
		_, _, err := invoke(t, r.svc, "integrations/h", "handleEvent", hookCfg(), spec)
		var se *runtime.Error
		if !errors.As(err, &se) || se.Kind != runtime.KindException {
			t.Errorf("%s: must be refused with a script exception, got %v", name, err)
		}
	}
	if r.hits.Load() != 0 {
		t.Fatalf("no denied request may reach the network; server saw %d", r.hits.Load())
	}
	// a script without any endpoint grant in its config gets nothing
	if _, _, err := invoke(t, r.svc, "integrations/h", "handleEvent", map[string]any{}, map[string]any{"endpoint": "hook", "path": "/x"}); err == nil {
		t.Fatal("no grant, no access")
	}
}

func TestHTTPRequestDoesNotFollowRedirectsAndBoundsResponses(t *testing.T) {
	var target atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { target.Add(1) }))
	defer evil.Close()
	r := newHTTPRig(t, func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/base/redirect":
			http.Redirect(w, req, evil.URL, http.StatusFound)
		case "/base/big":
			_, _ = w.Write(bytes.Repeat([]byte("a"), api.MaxResponseBody+500))
		}
	}, hookScript, hookCfg())
	out, _, err := invoke(t, r.svc, "integrations/h", "handleEvent", hookCfg(), map[string]any{"endpoint": "hook", "path": "/redirect"})
	if err != nil || !strings.Contains(string(out), `"status":302`) || target.Load() != 0 {
		t.Fatalf("redirects must not be followed: %s %v (target hits %d)", out, err, target.Load())
	}
	out, _, err = invoke(t, r.svc, "integrations/h", "handleEvent", hookCfg(), map[string]any{"endpoint": "hook", "path": "/big"})
	if err != nil || !strings.Contains(string(out), `"truncated":true`) || len(out) > api.MaxResponseBody+1000 {
		t.Fatalf("response must be bounded: len=%d %v", len(out), err)
	}
}

func TestHTTPRequestTimeoutsAndFailuresDoNotLeakURLs(t *testing.T) {
	r := newHTTPRig(t, func(w http.ResponseWriter, _ *http.Request) { time.Sleep(700 * time.Millisecond) }, hookScript, hookCfg())
	start := time.Now()
	_, _, err := invoke(t, r.svc, "integrations/h", "handleEvent", hookCfg(), map[string]any{"endpoint": "hook", "path": "/slow"})
	if err == nil || !strings.Contains(err.Error(), "timeout") || time.Since(start) > 650*time.Millisecond {
		t.Fatalf("endpoint timeout (300ms) must apply: %v after %v", err, time.Since(start))
	}
	if strings.Contains(err.Error(), r.srv.URL) {
		t.Fatal("errors must not leak the endpoint URL")
	}
	r.srv.Close()
	_, _, err = invoke(t, r.svc, "integrations/h", "handleEvent", hookCfg(), map[string]any{"endpoint": "hook", "path": "/x"})
	if err == nil || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("connection failure: %v", err)
	}
}

func TestHTTPRequestBudget(t *testing.T) {
	r := newHTTPRig(t, func(w http.ResponseWriter, _ *http.Request) {}, `
export function handleEvent() {
  const out = [];
  for (let i = 0; i < 5; i++) out.push(http.request({endpoint: "hook", path: "/x"}).status);
  return out;
}`, hookCfg())
	_, _, err := invoke(t, r.svc, "integrations/h", "handleEvent", hookCfg())
	if err == nil || r.hits.Load() != int32(api.MaxHTTPCalls) {
		t.Fatalf("request budget: err=%v hits=%d want %d", err, r.hits.Load(), api.MaxHTTPCalls)
	}
}

func TestAllowedEndpointsFromConfig(t *testing.T) {
	got := api.AllowedEndpoints(map[string]any{"endpoint_ref": "a", "backup_endpoint_ref": "b", "endpoint_refs": []any{"c", "d"}, "other": "e", "endpoint_ref_x": "f"})
	want := map[string]bool{"a": true, "b": true, "c": true, "d": true}
	if len(got) != 4 {
		t.Fatalf("%v", got)
	}
	for _, g := range got {
		if !want[g] {
			t.Fatalf("unexpected %s", g)
		}
	}
}

func TestOnlyDocumentedGlobalsAreExposed(t *testing.T) {
	svc, _ := setup(t, map[string]string{"transforms/probe": `
export function transform() {
  return Object.getOwnPropertyNames(globalThis).filter(n => ["log","device","inventory","state","config","events","metrics","http"].includes(n)).sort();
}`}, nil, api.Deps{})
	out, _, err := invoke(t, svc, "transforms/probe", "transform", nil, 1)
	if err != nil || string(out) != `["config","device","events","http","inventory","log","metrics","state"]` {
		t.Fatalf("host objects: %s %v", out, err)
	}
}
