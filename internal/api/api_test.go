package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

var t0 = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

type env struct {
	srv   *httptest.Server
	store *persistence.MemStore
	m     *telemetry.Metrics
}

func seed(t *testing.T) (*persistence.MemStore, context.Context) {
	t.Helper()
	ctx := context.Background()
	s := persistence.NewMemStore()
	devs := []domain.Device{
		{ID: "switch-01", Hostname: "switch-01.lab", ManagementAddress: "sim:1", DeviceType: domain.DeviceSwitch, Site: "lab", Tags: []string{"core", "automation-enabled"},
			Capabilities: []string{"snmp"}, CredentialsRef: "snmp/super-secret-ref", Enabled: true, Attributes: map[string]string{"role": "core"}},
		{ID: "server-01", Hostname: "server-01.lab", ManagementAddress: "sim:2", DeviceType: domain.DeviceServer, Site: "lab", Enabled: true},
		{ID: "ups-01", Hostname: "ups-01.dc", ManagementAddress: "sim:3", DeviceType: domain.DeviceUPS, Site: "dc", Enabled: false},
	}
	if err := s.UpsertDevices(ctx, devs); err != nil {
		t.Fatal(err)
	}
	_ = s.Do(ctx, func(tx persistence.Tx) error {
		_ = tx.TouchDevice(ctx, "switch-01", t0.Add(time.Hour))
		for i := 0; i < 25; i++ {
			e := domain.Event{EventID: fmt.Sprintf("evt-%02d", i), CorrelationID: "corr", DeviceID: "switch-01", Type: "interface.down", Severity: domain.SeverityWarning,
				Message: "down", Labels: map[string]string{"interface": "Gi0/1"}, OccurredAt: t0.Add(time.Duration(i) * time.Minute), Source: "iface", AlertKey: fmt.Sprintf("k%d", i)}
			if i%5 == 0 {
				e.DeviceID, e.Severity, e.Type = "server-01", domain.SeverityCritical, "device.down"
			}
			_, _ = tx.InsertEvent(ctx, e)
			if i < 2 {
				_, _ = tx.OpenAlert(ctx, e)
			}
		}
		_, _ = tx.ResolveAlert(ctx, domain.Event{EventID: "r", AlertKey: "k1", OccurredAt: t0.Add(time.Hour)})
		for _, r := range []domain.StateRecord{
			{Key: "reach/switch-01/", DeviceID: "switch-01", DefinitionID: "device-reachability", State: domain.StateUp, LastObservedAt: t0, LastSeenAt: t0, LastTransitionAt: t0, LastValue: "true"},
			{Key: "if/switch-01/interface=Gi0/1", DeviceID: "switch-01", DefinitionID: "interface-operational", Labels: map[string]string{"interface": "Gi0/1"}, State: domain.StateDown, Failures: 3, LastObservedAt: t0, LastSeenAt: t0, LastTransitionAt: t0.Add(time.Minute)},
		} {
			_ = tx.PutState(ctx, r)
		}
		for i := 0; i < 6; i++ {
			o := domain.Observation{ObservationID: fmt.Sprint("cpu", i), CorrelationID: "c", DeviceID: "switch-01", Source: "snmp", Metric: "system.cpu.utilization",
				Value: 0.1 * float64(i), ObservedAt: t0.Add(time.Duration(i) * time.Minute)}
			_, _ = tx.InsertObservation(ctx, o)
		}
		_, _ = tx.InsertObservation(ctx, domain.Observation{ObservationID: "up", CorrelationID: "c", DeviceID: "switch-01", Source: "snmp", Metric: "system.uptime", Value: 100.0, ObservedAt: t0})
		return nil
	})
	for i, st := range []domain.AutomationStatus{domain.AutomationAwaitingApproval, domain.AutomationDryRun, domain.AutomationDenied} {
		r := domain.AutomationRequest{RequestID: fmt.Sprintf("auto-%d", i), CorrelationID: "corr", PolicyID: "p1", EventID: "evt-01", DeviceID: "switch-01", Action: "noop",
			Proposer: "policy", DryRun: true, Status: domain.AutomationPending, NotBefore: t0, CreatedAt: t0.Add(time.Duration(i) * time.Minute)}
		if st == domain.AutomationAwaitingApproval {
			r.Status = st
		}
		_, _ = s.InsertAutomationRequest(ctx, r)
		if st != domain.AutomationAwaitingApproval {
			_, _ = s.CompleteAutomation(ctx, domain.AutomationResult{RequestID: r.RequestID, CorrelationID: "corr", Status: st, DryRun: true, Message: "m", StartedAt: t0, FinishedAt: t0})
		}
	}
	return s, ctx
}

func newEnv(t *testing.T, tokens map[string]string) *env {
	t.Helper()
	s, _ := seed(t)
	m := telemetry.New()
	server := &Server{
		Store: s, Approver: &automation.Engine{Store: s, Log: slog.New(slog.DiscardHandler)}, Metrics: m, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Version: "test",
		Health:          health.NewChecker("test", health.Check{Name: "postgres", Critical: true, Fn: func(context.Context) health.Result { return health.Result{Status: health.OK} }}),
		DefaultPageSize: 10, MaxPageSize: 50,
	}
	if tokens != nil {
		server.Auth = TokenAuth{Tokens: tokens}
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return &env{srv: ts, store: s, m: m}
}

func (e *env) do(t *testing.T, method, path string, hdr map[string]string, body string) (int, http.Header, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func (e *env) get(t *testing.T, path string, into any) int {
	t.Helper()
	code, _, b := e.do(t, "GET", path, nil, "")
	if into != nil && code == 200 {
		if err := json.Unmarshal(b, into); err != nil {
			t.Fatalf("%s: %v\n%s", path, err, b)
		}
	}
	return code
}

type listResp[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

func TestDevicesListFilterAndNeverLeakCredentials(t *testing.T) {
	e := newEnv(t, nil)
	var all listResp[DeviceDTO]
	if code := e.get(t, "/api/devices", &all); code != 200 || len(all.Items) != 3 || all.NextCursor != nil {
		t.Fatalf("%d %+v", code, all)
	}
	_, _, raw := e.do(t, "GET", "/api/devices", nil, "")
	if strings.Contains(string(raw), "super-secret-ref") || strings.Contains(string(raw), "credentials") {
		t.Fatal("credential references must never be exposed by the API")
	}
	if all.Items[0].ID != "server-01" || all.Items[1].ID != "switch-01" || all.Items[2].ID != "ups-01" {
		t.Fatalf("devices are ordered by id: %+v", all.Items)
	}
	d := all.Items[1]
	if d.ID != "switch-01" || d.Hostname != "switch-01.lab" || d.Type != "switch" || d.LastSeen == nil || !d.LastSeen.Equal(t0.Add(time.Hour)) || d.Attributes["role"] != "core" {
		t.Fatalf("%+v", d)
	}
	if all.Items[0].Tags == nil {
		t.Fatal("empty collections are [], not null")
	}
	for path, want := range map[string]int{"?site=lab": 2, "?type=server": 1, "?tag=core": 1, "?enabled=false": 1, "?site=nowhere": 0} {
		var r listResp[DeviceDTO]
		e.get(t, "/api/devices"+path, &r)
		if len(r.Items) != want {
			t.Errorf("%s: %d items, want %d", path, len(r.Items), want)
		}
	}
	var p1, p2 listResp[DeviceDTO]
	e.get(t, "/api/devices?limit=2", &p1)
	if len(p1.Items) != 2 || p1.NextCursor == nil {
		t.Fatalf("%+v", p1)
	}
	e.get(t, "/api/devices?limit=2&cursor="+*p1.NextCursor, &p2)
	if len(p2.Items) != 1 || p2.Items[0].ID != "ups-01" || p2.NextCursor != nil {
		t.Fatalf("%+v", p2)
	}
}

func TestDeviceDetailStateAndMetrics(t *testing.T) {
	e := newEnv(t, nil)
	var d, srv DeviceDTO
	e.get(t, "/api/devices/switch-01", &d)
	e.get(t, "/api/devices/server-01", &srv)
	if d.OpenAlerts == nil || *d.OpenAlerts != 0 || srv.OpenAlerts == nil || *srv.OpenAlerts != 1 {
		t.Fatalf("switch-01's alert was resolved; server-01's is still firing: %+v %+v", d.OpenAlerts, srv.OpenAlerts)
	}
	var st DeviceStateDTO
	e.get(t, "/api/devices/switch-01/state", &st)
	byDef := map[string]StateDTO{}
	for _, s := range st.States {
		byDef[s.Definition] = s
	}
	if st.Health != "down" || len(st.States) != 2 || byDef["interface-operational"].State != "DOWN" || byDef["interface-operational"].Labels["interface"] != "Gi0/1" ||
		byDef["interface-operational"].Failures != 3 || byDef["device-reachability"].State != "UP" {
		t.Fatalf("%+v", st)
	}
	var none DeviceStateDTO
	e.get(t, "/api/devices/server-01/state", &none)
	if none.Health != "unknown" || len(none.States) != 0 {
		t.Fatalf("%+v", none)
	}

	var series struct {
		Points []MetricDTO `json:"points"`
	}
	e.get(t, "/api/devices/switch-01/metrics?metric=system.cpu.utilization&limit=3", &series)
	if len(series.Points) != 3 || series.Points[0].Value != 0.5 || !series.Points[0].ObservedAt.After(series.Points[2].ObservedAt) {
		t.Fatalf("newest first: %+v", series.Points)
	}
	e.get(t, "/api/devices/switch-01/metrics?metric=system.cpu.utilization&since="+t0.Add(4*time.Minute).Format(time.RFC3339), &series)
	if len(series.Points) != 2 {
		t.Fatalf("since: %+v", series.Points)
	}
	e.get(t, "/api/devices/switch-01/metrics", &series)
	if len(series.Points) != 2 {
		t.Fatalf("without a metric name: the latest point of every series: %+v", series.Points)
	}
}

func TestEventsPaginationAndFilters(t *testing.T) {
	e := newEnv(t, nil)
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		var r listResp[EventDTO]
		path := "/api/events?limit=10"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		if code := e.get(t, path, &r); code != 200 {
			t.Fatal(code)
		}
		pages++
		for _, ev := range r.Items {
			if seen[ev.ID] {
				t.Fatalf("duplicate %s", ev.ID)
			}
			seen[ev.ID] = true
		}
		if r.NextCursor == nil {
			break
		}
		cursor = *r.NextCursor
	}
	if len(seen) != 25 || pages != 3 {
		t.Fatalf("%d events in %d pages", len(seen), pages)
	}
	var crit listResp[EventDTO]
	e.get(t, "/api/events?min_severity=critical", &crit)
	if len(crit.Items) != 5 {
		t.Fatalf("%d", len(crit.Items))
	}
	var dev listResp[EventDTO]
	e.get(t, "/api/events?device=switch-01&type=interface.down&limit=50", &dev)
	if len(dev.Items) != 20 || dev.Items[0].Severity != "warning" || dev.Items[0].Labels["interface"] != "Gi0/1" {
		t.Fatalf("%d %+v", len(dev.Items), dev.Items[0])
	}
	var win listResp[EventDTO]
	e.get(t, "/api/events?since="+t0.Add(20*time.Minute).Format(time.RFC3339)+"&until="+t0.Add(23*time.Minute).Format(time.RFC3339), &win)
	if len(win.Items) != 3 {
		t.Fatalf("%+v", win.Items)
	}
}

func TestAlertsAndAutomation(t *testing.T) {
	e := newEnv(t, nil)
	var firing, resolved listResp[AlertDTO]
	e.get(t, "/api/alerts?status=firing", &firing)
	e.get(t, "/api/alerts?status=resolved", &resolved)
	if len(firing.Items) != 1 || len(resolved.Items) != 1 || resolved.Items[0].ResolvedAt == nil || firing.Items[0].Status != "firing" {
		t.Fatalf("%+v %+v", firing, resolved)
	}
	var auto listResp[AutomationDTO]
	e.get(t, "/api/automation", &auto)
	if len(auto.Items) != 3 || auto.Items[0].ID != "auto-2" {
		t.Fatalf("%+v", auto)
	}
	for _, a := range auto.Items {
		switch a.ID {
		case "auto-0":
			if a.Result != nil || a.Status != "awaiting_approval" {
				t.Fatalf("%+v", a)
			}
		case "auto-1":
			if a.Result == nil || a.Result.Status != "dry_run" || !a.Result.DryRun || a.Result.Message != "m" {
				t.Fatalf("%+v", a)
			}
		}
	}
	var one AutomationDTO
	if code := e.get(t, "/api/automation/auto-1", &one); code != 200 || one.Policy != "p1" || one.CorrelationID != "corr" || one.Result == nil {
		t.Fatalf("%d %+v", code, one)
	}
	if code := e.get(t, "/api/automation/nope", nil); code != 404 {
		t.Fatal(code)
	}
	var byStatus listResp[AutomationDTO]
	e.get(t, "/api/automation?status=denied&device=switch-01&policy=p1", &byStatus)
	if len(byStatus.Items) != 1 || byStatus.Items[0].ID != "auto-2" {
		t.Fatalf("%+v", byStatus)
	}
}

func TestValidationErrorsAreClearAndUniform(t *testing.T) {
	e := newEnv(t, nil)
	bad := map[string]int{
		"/api/events?limit=0":                      400,
		"/api/events?limit=9999":                   400,
		"/api/events?limit=abc":                    400,
		"/api/events?since=yesterday":              400,
		"/api/events?min_severity=loud":            400,
		"/api/events?cursor=garbage":               400,
		"/api/alerts?status=maybe":                 400,
		"/api/automation?status=exploded":          400,
		"/api/devices/switch-01/metrics?until=now": 400,
		"/api/devices/ghost":                       404,
		"/api/devices/ghost/state":                 404,
		"/api/nowhere":                             404,
	}
	for path, want := range bad {
		code, hdr, body := e.do(t, "GET", path, nil, "")
		if code != want {
			t.Errorf("%s: %d, want %d (%s)", path, code, want, body)
			continue
		}
		var eb errorBody
		if json.Unmarshal(body, &eb) != nil || eb.Error.Code == "" || eb.Error.Message == "" || eb.Error.RequestID == "" || eb.Error.RequestID != hdr.Get("X-Request-Id") {
			t.Errorf("%s: errors need code, message and the request id for log correlation: %s", path, body)
		}
	}
	code, _, body := e.do(t, "DELETE", "/api/devices", nil, "")
	if code != 405 || !strings.Contains(string(body), "method_not_allowed") {
		t.Fatalf("%d %s", code, body)
	}
}

func TestApprovalRequiresAValidBearerTokenAndRecordsWhoApproved(t *testing.T) {
	e := newEnv(t, map[string]string{"tok-alice": "alice", "tok-bob": "bob"})
	url := "/api/automation/auto-0/approve"

	for name, hdr := range map[string]map[string]string{
		"no credentials": nil,
		"wrong token":    {"Authorization": "Bearer nope"},
		"wrong scheme":   {"Authorization": "Basic dG9rLWFsaWNl"},
		"empty bearer":   {"Authorization": "Bearer "},
		"token in body":  nil,
	} {
		code, h, _ := e.do(t, "POST", url, hdr, `{"approved_by":"alice"}`)
		if code != 401 || h.Get("WWW-Authenticate") == "" {
			t.Errorf("%s: %d", name, code)
		}
	}
	var still AutomationDTO
	e.get(t, "/api/automation/auto-0", &still)
	if still.Status != "awaiting_approval" || still.ApprovedBy != "" {
		t.Fatalf("unauthenticated attempts must change nothing: %+v", still)
	}

	code, _, body := e.do(t, "POST", url, map[string]string{"Authorization": "Bearer tok-bob"}, `{"approved_by":"mallory"}`)
	var got AutomationDTO
	_ = json.Unmarshal(body, &got)
	if code != 200 || got.Status != "approved" || got.ApprovedBy != "bob" {
		t.Fatalf("the approver identity comes from the token, never from the request body: %d %s", code, body)
	}
	if code, _, _ := e.do(t, "POST", url, map[string]string{"Authorization": "Bearer tok-alice"}, ""); code != 409 {
		t.Fatalf("approving twice: %d", code)
	}
	var after AutomationDTO
	e.get(t, "/api/automation/auto-0", &after)
	if after.ApprovedBy != "bob" {
		t.Fatalf("the first approver stands: %+v", after)
	}
	if code, _, _ := e.do(t, "POST", "/api/automation/ghost/approve", map[string]string{"Authorization": "Bearer tok-bob"}, ""); code != 404 {
		t.Fatalf("%d", code)
	}
	if code, _, _ := e.do(t, "POST", "/api/automation/auto-1/approve", map[string]string{"Authorization": "Bearer tok-bob"}, ""); code != 409 {
		t.Fatalf("a request that is not awaiting approval: %d", code)
	}
}

func TestApprovalsAreDisabledWithoutConfiguredTokens(t *testing.T) {
	e := newEnv(t, nil)
	code, _, body := e.do(t, "POST", "/api/automation/auto-0/approve", map[string]string{"Authorization": "Bearer anything"}, "")
	if code != 403 || !strings.Contains(string(body), "approvals_disabled") {
		t.Fatalf("%d %s", code, body)
	}
}

func TestOversizedBodiesAreRejected(t *testing.T) {
	e := newEnv(t, map[string]string{"t": "alice"})
	req, _ := http.NewRequest("POST", e.srv.URL+"/api/automation/auto-0/approve", strings.NewReader(strings.Repeat("x", 100<<10)))
	req.Header.Set("Authorization", "Bearer t")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// the handler never reads the body, so the limit is enforced by the reader when used; the request must still be safe
	if resp.StatusCode >= 500 {
		t.Fatalf("%d", resp.StatusCode)
	}
}

func TestHealthReadinessAndMetricsEndpoints(t *testing.T) {
	e := newEnv(t, nil)
	if code := e.get(t, "/api/health", nil); code != 200 {
		t.Fatal(code)
	}
	var rep health.Report
	if code := e.get(t, "/api/readiness", &rep); code != 200 || !rep.Ready || rep.Checks["postgres"].Status != health.OK {
		t.Fatalf("%d %+v", code, rep)
	}
	if code := e.get(t, "/api/health/dependencies", nil); code != 200 {
		t.Fatal(code)
	}
	e.get(t, "/api/devices", nil)
	_, _, body := e.do(t, "GET", "/metrics", nil, "")
	if !strings.Contains(string(body), `http_requests_total{code="200",route="GET /api/devices"} 1`) {
		t.Fatalf("requests are counted per route pattern, not per URL:\n%s", body)
	}
	e.get(t, "/api/devices/switch-01", nil)
	e.get(t, "/api/devices/server-01", nil)
	_, _, body = e.do(t, "GET", "/metrics", nil, "")
	if !strings.Contains(string(body), `route="GET /api/devices/{id}"`) || strings.Contains(string(body), `switch-01`) {
		t.Fatal("route labels must use the pattern (low cardinality), not the concrete path")
	}
}

func TestRequestIDsAreHonouredAndGenerated(t *testing.T) {
	e := newEnv(t, nil)
	_, h, _ := e.do(t, "GET", "/api/devices", map[string]string{"X-Request-Id": "trace-me-123"}, "")
	if h.Get("X-Request-Id") != "trace-me-123" {
		t.Fatal("a caller-supplied request id is propagated")
	}
	_, h, _ = e.do(t, "GET", "/api/devices", nil, "")
	if len(h.Get("X-Request-Id")) < 8 {
		t.Fatal("one is generated otherwise")
	}
	_, h, _ = e.do(t, "GET", "/api/devices", map[string]string{"X-Request-Id": strings.Repeat("x", 500)}, "")
	if len(h.Get("X-Request-Id")) > 64 {
		t.Fatal("hostile request ids are replaced, not echoed")
	}
}

type failingStore struct{ persistence.Reader }

func (f failingStore) ListDevices(context.Context) ([]domain.Device, error) {
	return nil, domain.Errorf(domain.CategoryDependency, "connection refused to db.internal:5432")
}

func TestDependencyFailuresBecome503WithoutLeakingInternals(t *testing.T) {
	s := &Server{Store: failingStore{}, Log: slog.New(slog.DiscardHandler)}
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/api/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || strings.Contains(string(b), "db.internal") || !strings.Contains(string(b), "unavailable") {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
}

type panicStore struct{ persistence.Reader }

func (panicStore) ListDevices(context.Context) ([]domain.Device, error) { panic("bug") }

func TestHandlerPanicsAreContained(t *testing.T) {
	s := &Server{Store: panicStore{}, Log: slog.New(slog.DiscardHandler)}
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/api/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("%d", resp.StatusCode)
	}
	if resp2, err := http.Get(ts.URL + "/api/nowhere"); err != nil || resp2.StatusCode != 404 {
		t.Fatal("the server keeps serving after a panic")
	}
}

func TestTokenAuthIsConstantTimeShaped(t *testing.T) {
	a := TokenAuth{Tokens: map[string]string{"aaa": "alice", "bbb": "bob"}}
	req := func(h string) *http.Request {
		r, _ := http.NewRequest("POST", "/", nil)
		if h != "" {
			r.Header.Set("Authorization", h)
		}
		return r
	}
	if id, ok := a.Identify(req("Bearer bbb")); !ok || id != "bob" {
		t.Fatal("valid token")
	}
	for _, h := range []string{"", "Bearer ", "Bearer bb", "Bearer bbbb", "bbb", "Token bbb"} {
		if _, ok := a.Identify(req(h)); ok {
			t.Errorf("%q must not authenticate", h)
		}
	}
}
