package scripting

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

var t0 = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func obs(metric string, v any) domain.Observation {
	return domain.Observation{
		ObservationID: "o1", CorrelationID: "c1", DeviceID: "ap-01", Source: "snmp", Metric: metric, Value: v,
		Labels: map[string]string{"interface": "wlan0"}, ObservedAt: t0, ReceivedAt: t0.Add(time.Second), Metadata: map[string]string{"site": "lab"},
	}
}

func meta(metrics string) string {
	return "export const meta = {version: \"1\", description: \"t\", metrics: " + metrics + "}\n"
}

func newExt(t *testing.T, files map[string]string) (*Extensions, *Service) {
	t.Helper()
	dir := t.TempDir()
	for name, src := range files {
		writeScript(t, dir, strings.Split(name, "/")[0], strings.Split(name, "/")[1], src)
	}
	cfg := config.Default().Scripting
	cfg.Directories, cfg.Workers, cfg.ExecutionTimeout, cfg.QuarantineAfter = []string{dir}, 2, 100*time.Millisecond, 1000
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, rep := New(cfg, nil, []runtime.Installer{api.Installer(api.Deps{Log: log})}, log)
	t.Cleanup(svc.Close)
	if rep.Failed != 0 {
		t.Fatalf("load: %v", rep.Errors)
	}
	return &Extensions{Svc: svc, Validator: schema.Validator{Limits: schema.DefaultLimits(), Now: func() time.Time { return t0.Add(time.Hour) }}, Log: log}, svc
}

func TestTransformRewritesMetricAndValue(t *testing.T) {
	ext, _ := newExt(t, map[string]string{"transforms/cpu": meta(`["vendor.cpu.load"]`) + `
export function transform(o) { return {...o, metric: "system.cpu.utilization", value: o.value / 100} }`})
	out, keep, err := ext.Transform(context.Background(), obs("vendor.cpu.load", 40.0))
	if err != nil || !keep || out.Metric != "system.cpu.utilization" || out.Value != 0.4 {
		t.Fatalf("%+v %v %v", out, keep, err)
	}
	if out.ObservationID != "o1" || out.DeviceID != "ap-01" || out.Labels["interface"] != "wlan0" || out.ReceivedAt.IsZero() {
		t.Fatalf("identity fields must survive: %+v", out)
	}
}

func TestTransformOnlyRunsForDeclaredMetrics(t *testing.T) {
	ext, svc := newExt(t, map[string]string{
		"transforms/a": meta(`["vendor.cpu.load"]`) + "export function transform(o) { return o }",
		"transforms/b": meta(`["network.*"]`) + "export function transform(o) { return o }",
	})
	before := svc.Pool.Stats().Calls
	_, _, _ = ext.Transform(context.Background(), obs("system.uptime", 1.0))
	if svc.Pool.Stats().Calls != before {
		t.Fatal("no script declared this metric: JavaScript must not be invoked at all")
	}
	_, _, _ = ext.Transform(context.Background(), obs("network.interface.operational", true))
	if svc.Pool.Stats().Calls != before+1 {
		t.Fatalf("prefix match should run exactly script b: %d calls", svc.Pool.Stats().Calls-before)
	}
}

func TestTransformsChainInIDOrderAndCanDrop(t *testing.T) {
	ext, _ := newExt(t, map[string]string{
		"transforms/10-first":  meta(`["m.x"]`) + `export function transform(o) { return {...o, value: o.value + 1} }`,
		"transforms/20-second": meta(`["m.x"]`) + `export function transform(o) { return {...o, value: o.value * 10} }`,
		"transforms/30-drop":   meta(`["m.drop"]`) + `export function transform(o) { return null }`,
	})
	out, _, _ := ext.Transform(context.Background(), obs("m.x", 1.0))
	if out.Value != 20.0 {
		t.Fatalf("(1+1)*10 = 20, got %v: scripts must run in id order", out.Value)
	}
	if _, keep, err := ext.Transform(context.Background(), obs("m.drop", 1.0)); err != nil || keep {
		t.Fatal("null must drop the observation")
	}
}

func TestTransformCannotChangeIdentityOrRouting(t *testing.T) {
	for field, body := range map[string]string{
		"device_id":      `{...o, device_id: "other-device"}`,
		"observation_id": `{...o, observation_id: "forged"}`,
		"correlation_id": `{...o, correlation_id: "forged"}`,
		"observed_at":    `{...o, observed_at: "2030-01-01T00:00:00Z"}`,
		"source":         `{...o, source: "script:evil"}`,
		"received_at":    `(function(){ const c = {...o}; delete c.received_at; return c })()`,
	} {
		ext, svc := newExt(t, map[string]string{"transforms/evil": meta(`["m.x"]`) + "export function transform(o) { return " + body + " }"})
		out, keep, err := ext.Transform(context.Background(), obs("m.x", 1.0))
		if err != nil || !keep || out.DeviceID != "ap-01" || out.ObservationID != "o1" {
			t.Fatalf("%s: a rejected result must leave the observation unchanged: %+v %v", field, out, err)
		}
		if s, _ := svc.Registry.Get("transforms/evil"); s.TotalFailures != 1 || !strings.Contains(s.LastError, field) {
			t.Fatalf("%s: the violation must be counted against the script: %+v", field, s)
		}
	}
}

func TestInvalidScriptOutputIsRejected(t *testing.T) {
	for name, body := range map[string]string{
		"not an object":   `"hello"`,
		"array":           `[1,2]`,
		"extra field":     `{...o, surprise: true}`,
		"bad metric":      `{...o, metric: "Has Spaces"}`,
		"object value":    `{...o, value: {a: 1}}`,
		"nan value":       `{...o, value: NaN}`,
		"metadata number": `{...o, metadata: {n: 5}}`,
		"missing value":   `(function(){ const c = {...o}; delete c.value; return c })()`,
	} {
		ext, svc := newExt(t, map[string]string{"transforms/bad": meta(`["m.x"]`) + "export function transform(o) { return " + body + " }"})
		out, keep, err := ext.Transform(context.Background(), obs("m.x", 1.0))
		if err != nil || !keep || out.Metric != "m.x" || out.Value != 1.0 {
			t.Errorf("%s: the observation must continue unchanged: %+v %v", name, out, err)
		}
		if s, _ := svc.Registry.Get("transforms/bad"); s.TotalFailures != 1 {
			t.Errorf("%s: invalid output must count as a script failure (%d)", name, s.TotalFailures)
		}
	}
}

func TestBrokenScriptIsIsolatedFromWorkingOnes(t *testing.T) {
	ext, svc := newExt(t, map[string]string{
		"transforms/10-throws": meta(`["m.x"]`) + `export function transform(o) { throw new Error("boom") }`,
		"transforms/20-spins":  meta(`["m.x"]`) + `export function transform(o) { for(;;){} }`,
		"transforms/30-works":  meta(`["m.x"]`) + `export function transform(o) { return {...o, value: 42} }`,
	})
	out, keep, err := ext.Transform(context.Background(), obs("m.x", 1.0))
	if err != nil || !keep || out.Value != 42.0 {
		t.Fatalf("working script must still apply after two broken ones: %+v %v %v", out, keep, err)
	}
	for _, k := range []string{"transforms/10-throws", "transforms/20-spins"} {
		if s, _ := svc.Registry.Get(k); s.TotalFailures != 1 {
			t.Fatalf("%s: %+v", k, s)
		}
	}
}

func TestSaturationPropagatesAsRetryableNotSwallowed(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "transforms", "slow", meta(`["m.x"]`)+`export function transform(o) { const e = Date.now() + 120; while (Date.now() < e) {} return o }`)
	cfg := config.Default().Scripting
	cfg.Directories, cfg.Workers, cfg.QueueSize, cfg.ExecutionTimeout, cfg.QuarantineAfter = []string{dir}, 1, 1, time.Second, 1000
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, _ := New(cfg, nil, nil, log)
	defer svc.Close()
	svc.Pool.Close() // replace with a pool whose queue wait is tiny
	svc.Pool = runtime.NewPool(svc.Registry, runtime.Options{Workers: 1, QueueSize: 1, Timeout: time.Second, QueueWait: 20 * time.Millisecond})
	ext := &Extensions{Svc: svc, Validator: schema.Validator{Limits: schema.DefaultLimits(), Now: func() time.Time { return t0.Add(time.Hour) }}, Log: log}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var saturated, ok int
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := ext.Transform(context.Background(), obs("m.x", 1.0))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, runtime.ErrSaturated) && domain.IsRetryable(err):
				saturated++
			default:
				t.Errorf("unexpected: %v", err)
			}
		}()
	}
	wg.Wait()
	if saturated == 0 || ok == 0 {
		t.Fatalf("overload must surface as retryable errors (backpressure) while capacity is still used: ok=%d saturated=%d", ok, saturated)
	}
	if s, _ := svc.Registry.Get("transforms/slow"); s.TotalFailures != 0 {
		t.Fatal("saturation must never count against the script")
	}
}

func TestEnrichMayOnlyChangeMetadata(t *testing.T) {
	ext, svc := newExt(t, map[string]string{"enrichers/role": meta(`["m.*"]`) + `
export function enrich(o, ctx) { return {...o, metadata: {...o.metadata, role: ctx.device.device_type}} }`})
	dev := domain.Device{ID: "ap-01", DeviceType: domain.DeviceAccessPoint}
	out, err := ext.Enrich(context.Background(), obs("m.x", 1.0), dev)
	if err != nil || out.Metadata["role"] != "access_point" || out.Metadata["site"] != "lab" || out.Value != 1.0 {
		t.Fatalf("%+v %v", out, err)
	}
	for name, body := range map[string]string{
		"value":  `{...o, value: 2}`,
		"metric": `{...o, metric: "m.y"}`,
		"labels": `{...o, labels: {interface: "other"}}`,
		"device": `{...o, device_id: "ap-02"}`,
		"null":   `null`,
	} {
		ext2, svc2 := newExt(t, map[string]string{"enrichers/evil": meta(`["m.*"]`) + "export function enrich(o) { return " + body + " }"})
		got, err := ext2.Enrich(context.Background(), obs("m.x", 1.0), dev)
		if err != nil || got.Value != 1.0 || got.Metric != "m.x" || got.Labels["interface"] != "wlan0" || got.DeviceID != "ap-01" {
			t.Errorf("%s: enrichers must not be able to change anything but metadata: %+v %v", name, got, err)
		}
		if s, _ := svc2.Registry.Get("enrichers/evil"); s.TotalFailures != 1 {
			t.Errorf("%s: not counted as failure", name)
		}
	}
	_ = svc
}

func TestTransformsMustDeclareMetrics(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "transforms", "greedy", `export const meta = {version: "1"}`+"\nexport function transform(o) { return o }\n")
	cfg := config.Default().Scripting
	cfg.Directories = []string{dir}
	svc, rep := New(cfg, nil, nil, slog.New(slog.DiscardHandler))
	defer svc.Close()
	if rep.Failed != 1 || !strings.Contains(strings.Join(rep.Errors, ";"), "meta.metrics") {
		t.Fatalf("a transform without a metric filter would run for every observation: %+v", rep)
	}
	s, _ := svc.Registry.Get("transforms/greedy")
	if s.Status != registry.StatusFailed {
		t.Fatalf("%+v", s)
	}
}

func TestMetricMatching(t *testing.T) {
	cases := []struct {
		patterns []string
		metric   string
		want     bool
	}{
		{nil, "anything", true}, {[]string{"a.b"}, "a.b", true}, {[]string{"a.b"}, "a.bc", false},
		{[]string{"a.*"}, "a.b.c", true}, {[]string{"a.*"}, "ab", false}, {[]string{"x", "a.*"}, "a.z", true},
	}
	for _, c := range cases {
		if got := matches(c.patterns, c.metric); got != c.want {
			t.Errorf("matches(%v, %q) = %v", c.patterns, c.metric, got)
		}
	}
}
