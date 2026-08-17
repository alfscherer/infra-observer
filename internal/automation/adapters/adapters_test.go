package adapters_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/automation/adapters"
	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/secrets"
	"github.com/alfscherer/infra-observer/internal/sim"
)

type recorder struct {
	mu    sync.Mutex
	calls []string
	fail  error
}

func (r *recorder) Do(_ context.Context, device, event string, args map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	r.calls = append(r.calls, device+":"+event+":"+args["interface"])
	return nil
}
func (r *recorder) n() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.calls) }

// lab starts the real simulator (UDP SNMP agents) and returns devices whose
// addresses point at it, plus a Reader speaking real SNMP.
func lab(t *testing.T) (*sim.World, map[string]domain.Device, *adapters.Reader) {
	t.Helper()
	devs, err := inventory.LoadFile("../../../configs/inventory.yaml")
	if err != nil {
		t.Fatal(err)
	}
	w := sim.NewWorld(devs, sim.Options{Seed: 9})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	byID := map[string]domain.Device{}
	for _, d := range devs {
		a, err := w.Listen(ctx, d.ID, "127.0.0.1:0", slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		d.ManagementAddress = a.Addr().String()
		byID[d.ID] = d
	}
	return w, byID, &adapters.Reader{
		Dialer:  snmp.GoSNMPDialer{Timeout: 300 * time.Millisecond, MaxRepetitions: 10},
		Secrets: secrets.Static{"snmp/lab": {"version": "2c", "community": "lab-ro"}},
	}
}

func act(typ, target string, dry bool, params ...string) automation.Action {
	p := map[string]string{}
	for i := 0; i+1 < len(params); i += 2 {
		p[params[i]] = params[i+1]
	}
	return automation.Action{Type: typ, Target: target, Params: p, DryRun: dry, RequestID: "req-1", CorrelationID: "corr-1"}
}

func TestSwitchDryRunUsesRealSNMPPreflightAndChangesNothing(t *testing.T) {
	_, devs, reader := lab(t)
	rec := &recorder{}
	sw := &adapters.Switch{Ctl: rec, Reader: reader}
	res, err := sw.Execute(context.Background(), devs["switch-01"], act("bounce_interface", "Gi0/2", true))
	if err != nil {
		t.Fatal(err)
	}
	if rec.n() != 0 || res.Changed {
		t.Fatal("a dry run must not touch the device")
	}
	if !strings.Contains(res.Message, "dry-run: would bounce interface Gi0/2") || !strings.Contains(res.Message, "oper=up") || res.Details["before_oper"] != "up" {
		t.Fatalf("the dry run should report what a real read found: %q %v", res.Message, res.Details)
	}
	// the preflight is a real check: a port that does not exist fails even in dry-run
	_, err = sw.Execute(context.Background(), devs["switch-01"], act("bounce_interface", "Gi9/9", true))
	if domain.CategoryOf(err) != domain.CategoryValidation || rec.n() != 0 {
		t.Fatalf("%v", err)
	}
}

func TestSwitchLiveBounceHealsTheSimulatedPortAndCanBeReadBackOverSNMP(t *testing.T) {
	w, devs, reader := lab(t)
	sw := &adapters.Switch{Ctl: sim.DeviceController{C: w.Control()}, Reader: reader}
	ctx := context.Background()

	if _, err := w.Apply(sim.Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/2"}}); err != nil {
		t.Fatal(err)
	}
	st, err := reader.Interface(ctx, devs["switch-01"], "Gi0/2")
	if err != nil || st.Oper != "down" {
		t.Fatalf("precondition: %+v %v", st, err)
	}
	res, err := sw.Execute(ctx, devs["switch-01"], act("bounce_interface", "Gi0/2", false))
	if err != nil || !res.Changed || res.Details["before_oper"] != "down" {
		t.Fatalf("%+v %v", res, err)
	}
	st, _ = reader.Interface(ctx, devs["switch-01"], "Gi0/2")
	if st.Oper != "up" || st.Admin != "up" {
		t.Fatalf("the remediation must be visible to the next SNMP poll: %+v", st)
	}

	// disable / enable
	if _, err := sw.Execute(ctx, devs["switch-01"], act("disable_interface", "Gi0/3", false)); err != nil {
		t.Fatal(err)
	}
	st, _ = reader.Interface(ctx, devs["switch-01"], "Gi0/3")
	if st.Admin != "down" || st.Oper != "down" {
		t.Fatalf("%+v", st)
	}
	if _, err := sw.Execute(ctx, devs["switch-01"], act("enable_interface", "Gi0/3", false)); err != nil {
		t.Fatal(err)
	}
	if st, _ = reader.Interface(ctx, devs["switch-01"], "Gi0/3"); st.Oper != "up" {
		t.Fatalf("%+v", st)
	}

	// description is mock configuration state
	res, err = sw.Execute(ctx, devs["switch-01"], act("set_interface_description", "Gi0/4", false, "description", "lab server"))
	if err != nil || res.Details["before_description"] != "" {
		t.Fatalf("%+v %v", res, err)
	}
	res, _ = sw.Execute(ctx, devs["switch-01"], act("set_interface_description", "Gi0/4", false, "description", "renamed"))
	if res.Details["before_description"] != "lab server" {
		t.Fatalf("%+v", res)
	}

	q, err := sw.Execute(ctx, devs["switch-01"], act("query_interface_state", "Gi0/2", false))
	if err != nil || q.Changed || q.Details["source"] != "snmp" || q.Details["oper_status"] != "up" {
		t.Fatalf("%+v %v", q, err)
	}
	if _, err := sw.Execute(ctx, devs["switch-01"], act("format_flash", "", false)); domain.CategoryOf(err) != domain.CategoryUnsupported {
		t.Fatalf("unknown actions are unsupported: %v", err)
	}
}

func TestSwitchControllerFailureIsReported(t *testing.T) {
	_, devs, reader := lab(t)
	rec := &recorder{fail: domain.Errorf(domain.CategoryDependency, "simulator not answering")}
	sw := &adapters.Switch{Ctl: rec, Reader: reader}
	if _, err := sw.Execute(context.Background(), devs["switch-01"], act("disable_interface", "Gi0/2", false)); err == nil || !domain.IsRetryable(err) {
		t.Fatalf("%v", err)
	}
}

func TestAccessPointActions(t *testing.T) {
	w, devs, reader := lab(t)
	ap := &adapters.AccessPoint{Ctl: sim.DeviceController{C: w.Control()}, Reader: reader}
	ctx := context.Background()

	res, err := ap.Execute(ctx, devs["ap-01"], act("query_ap_clients", "", false))
	if err != nil || res.Details["source"] != "snmp" || res.Changed || !strings.Contains(res.Message, "associated clients") {
		t.Fatalf("real SNMP read: %+v %v", res, err)
	}
	dry, _ := ap.Execute(ctx, devs["ap-01"], act("update_ap_config", "", true, "key", "channel", "value", "36"))
	live, _ := ap.Execute(ctx, devs["ap-01"], act("update_ap_config", "", false, "key", "channel", "value", "36"))
	again, _ := ap.Execute(ctx, devs["ap-01"], act("update_ap_config", "", false, "key", "channel", "value", "40"))
	if dry.Changed || !live.Changed || again.Details["before"] != "36" {
		t.Fatalf("mock config adapter: %+v %+v %+v", dry, live, again)
	}
	// a wedged AP comes back after a service restart
	_, _ = w.Apply(sim.Scenario{Device: "ap-01", Event: "offline"})
	dev, _ := w.Device("ap-01")
	offline := func() bool { var o bool; w.With(func() { o = dev.Offline }); return o }
	if _, err := ap.Execute(ctx, devs["ap-01"], act("restart_ap_service", "", true, "service", "wifi-radio")); err != nil || !offline() {
		t.Fatalf("dry-run must not restart anything: %v", err)
	}
	if _, err := ap.Execute(ctx, devs["ap-01"], act("restart_ap_service", "", false, "service", "wifi-radio")); err != nil || offline() {
		t.Fatalf("live restart should bring the AP back: %v", err)
	}
}

func TestHostActions(t *testing.T) {
	w, devs, reader := lab(t)
	h := &adapters.Host{Ctl: sim.DeviceController{C: w.Control()}, Reader: reader}
	ctx := context.Background()
	res, err := h.Execute(ctx, devs["server-01"], act("collect_diagnostics", "", false))
	if err != nil || res.Changed || res.Details["source"] != "snmp" || res.Details["hostname"] != "server-01" || res.Details["cpu_percent"] == "" || res.Details["uptime_seconds"] == "" {
		t.Fatalf("diagnostics over real SNMP: %+v %v", res, err)
	}
	_, _ = w.Apply(sim.Scenario{Device: "server-01", Event: "offline"})
	dev, _ := w.Device("server-01")
	if _, err := h.Execute(ctx, devs["server-01"], act("restart_service", "", false, "service", "observer-agent")); err != nil {
		t.Fatal(err)
	}
	var off bool
	w.With(func() { off = dev.Offline })
	if off {
		t.Fatal("restart_service should restore the wedged host")
	}
	// maintenance tasks are a closed list
	if r, err := h.Execute(ctx, devs["server-01"], act("run_maintenance_task", "", false, "task", "rotate-logs")); err != nil || !r.Changed {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := h.Execute(ctx, devs["server-01"], act("run_maintenance_task", "", false, "task", "format-disk")); domain.CategoryOf(err) != domain.CategoryUnsupported || !strings.Contains(err.Error(), "rotate-logs") {
		t.Fatalf("an unlisted task cannot run: %v", err)
	}
	if r, _ := h.Execute(ctx, devs["server-01"], act("run_maintenance_task", "", true, "task", "clear-tmp")); r.Changed || !strings.Contains(r.Message, "dry-run") {
		t.Fatalf("%+v", r)
	}
}

func TestSetRoutesByDeviceTypeAndActionKind(t *testing.T) {
	set := adapters.NewSet(&recorder{}, nil, &adapters.Generic{})
	sw := domain.Device{ID: "s", DeviceType: domain.DeviceSwitch}
	srv := domain.Device{ID: "h", DeviceType: domain.DeviceServer}
	printer := domain.Device{ID: "p", DeviceType: domain.DevicePrinter}
	if a, err := set.For(sw, "bounce_interface"); err != nil || a == nil {
		t.Fatal(err)
	}
	if a, _ := set.For(srv, "restart_service"); a == nil {
		t.Fatal("hosts")
	}
	if a, err := set.For(printer, "noop"); err != nil || a == nil {
		t.Fatalf("generic actions work for any device: %v", err)
	}
	if _, err := set.For(printer, "restart_service"); domain.CategoryOf(err) != domain.CategoryUnsupported {
		t.Fatalf("a device class without an adapter cannot be automated: %v", err)
	}
	if _, err := (adapters.Set{}).For(sw, "noop"); err == nil {
		t.Fatal("no generic adapter")
	}
}

// --- generic ---------------------------------------------------------------------

func TestWebhookAction(t *testing.T) {
	var got *http.Request
	var body string
	code := 202
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(code)
	}))
	defer srv.Close()
	g := &adapters.Generic{
		Endpoints: map[string]config.Endpoint{"alerts": {URLRef: "endpoints/alerts", Timeout: 300 * time.Millisecond}},
		Secrets:   secrets.Static{"endpoints/alerts": {"url": srv.URL + "/hooks", "token": "tok-123"}},
	}
	dev := domain.Device{ID: "ups-01"}
	dry, err := g.Execute(context.Background(), dev, act("send_webhook", "", true, "endpoint", "alerts", "message", "on battery"))
	if err != nil || got != nil || dry.Changed || strings.Contains(dry.Message, srv.URL) {
		t.Fatalf("dry-run sends nothing: %+v %v", dry, err)
	}
	res, err := g.Execute(context.Background(), dev, act("send_webhook", "", false, "endpoint", "alerts", "message", "on battery"))
	if err != nil || !res.Changed || got == nil {
		t.Fatalf("%+v %v", res, err)
	}
	if got.URL.Path != "/hooks/v1/automation" || got.Header.Get("Authorization") != "Bearer tok-123" || got.Header.Get("Idempotency-Key") != "req-1" {
		t.Fatalf("%s %v", got.URL.Path, got.Header)
	}
	if !strings.Contains(body, `"device":"ups-01"`) || !strings.Contains(body, `"correlation_id":"corr-1"`) {
		t.Fatalf("%s", body)
	}
	if strings.Contains(res.Message+" "+strings.Join(mapValues(res.Details), " "), "tok-123") || strings.Contains(res.Message, srv.URL) {
		t.Fatal("results are persisted and published: they must never contain the URL or token")
	}

	code = 503
	if _, err := g.Execute(context.Background(), dev, act("send_webhook", "", false, "endpoint", "alerts")); !domain.IsRetryable(err) {
		t.Fatalf("5xx is transient: %v", err)
	}
	code = 400
	if _, err := g.Execute(context.Background(), dev, act("send_webhook", "", false, "endpoint", "alerts")); err == nil || domain.IsRetryable(err) {
		t.Fatalf("4xx is permanent: %v", err)
	}
	if _, err := g.Execute(context.Background(), dev, act("send_webhook", "", false, "endpoint", "nowhere")); domain.CategoryOf(err) != domain.CategoryUnsupported {
		t.Fatalf("unknown endpoint: %v", err)
	}
	srv.Close()
	if _, err := g.Execute(context.Background(), dev, act("send_webhook", "", false, "endpoint", "alerts")); err == nil || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("connection errors must not leak the URL: %v", err)
	}
}

func mapValues(m map[string]string) []string {
	var out []string
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func TestEventRecordAndExtensionActions(t *testing.T) {
	var recorded []domain.Event
	var invoked []string
	g := &adapters.Generic{
		Now:    func() time.Time { return time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC) },
		Record: func(_ context.Context, ev domain.Event) error { recorded = append(recorded, ev); return nil },
		Invoke: func(_ context.Context, script string, _ domain.Device, _ automation.Action) (string, error) {
			invoked = append(invoked, script)
			return "extension ran", nil
		},
	}
	dev := domain.Device{ID: "srv"}
	a := act("create_event_record", "", false, "message", "operator note", "severity", "warning")
	r1, err := g.Execute(context.Background(), dev, a)
	r2, _ := g.Execute(context.Background(), dev, a)
	if err != nil || len(recorded) != 2 || recorded[0].EventID != recorded[1].EventID || recorded[0].Severity != domain.SeverityWarning || recorded[0].CorrelationID != "corr-1" || r1.Details["event_id"] != r2.Details["event_id"] {
		t.Fatalf("the event id derives from the request so a retried action records one event: %+v", recorded)
	}
	if _, err := g.Execute(context.Background(), dev, act("create_event_record", "", true, "message", "x")); err != nil || len(recorded) != 2 {
		t.Fatal("dry-run records nothing")
	}
	if res, err := g.Execute(context.Background(), dev, act("invoke_extension", "", false, "script", "webhook")); err != nil || res.Message != "extension ran" || invoked[0] != "webhook" {
		t.Fatalf("%+v %v", res, err)
	}
	if _, err := g.Execute(context.Background(), dev, act("invoke_extension", "", true, "script", "webhook")); err != nil || len(invoked) != 1 {
		t.Fatal("dry-run does not invoke")
	}
	failing := &adapters.Generic{Invoke: func(context.Context, string, domain.Device, automation.Action) (string, error) {
		return "", errors.New("script failed")
	}}
	if _, err := failing.Execute(context.Background(), dev, act("invoke_extension", "", false, "script", "x")); err == nil {
		t.Fatal("extension failure must be reported")
	}
	if r, err := g.Execute(context.Background(), dev, act("noop", "", true)); err != nil || r.Changed {
		t.Fatalf("%+v %v", r, err)
	}
	_ = io.Discard
}
