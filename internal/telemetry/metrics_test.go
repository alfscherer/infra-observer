package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/testutil"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("scrape: %d", rec.Code)
	}
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

func has(t *testing.T, out, line string) {
	t.Helper()
	if !strings.Contains(out, line+"\n") {
		t.Errorf("missing %q in:\n%s", line, grep(out, strings.SplitN(line, "{", 2)[0]))
	}
}

func grep(out, prefix string) string {
	var b strings.Builder
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, prefix) {
			b.WriteString(l + "\n")
		}
	}
	return b.String()
}

func TestEveryRequiredMetricIsRegistered(t *testing.T) {
	m := New()
	// touch each vector so it appears in the exposition
	m.OnPoll(collector.PollEvent{Duration: time.Millisecond})
	m.OnPoll(collector.PollEvent{Err: domain.Errorf(domain.CategoryTimeout, "x")})
	m.WorkerOutcome("processor")("processed", "s", time.Millisecond)
	m.WorkerOutcome("processor")("retry", "s", time.Millisecond)
	m.ScriptResult("transforms/a", time.Millisecond, nil)
	m.ScriptResult("transforms/a", time.Millisecond, &runtime.Error{Kind: runtime.KindTimeout})
	m.AutomationRequest("p")
	m.AutomationResult("p", domain.AutomationFailed, nil)
	m.DatabaseError(domain.Errorf(domain.CategoryDependency, "down"))
	m.QueueDepth.WithLabelValues("processor").Set(3)
	out := scrape(t, m)
	for _, name := range []string{
		"collector_requests_total", "collector_failures_total", "observation_queue_depth", "messages_processed_total", "messages_failed_total",
		"processing_duration_seconds_count", "javascript_executions_total", "javascript_failures_total", "javascript_timeouts_total",
		"javascript_execution_duration_seconds_count", "automation_requests_total", "automation_failures_total", "database_errors_total",
	} {
		if !strings.Contains(out, "\n"+name) && !strings.HasPrefix(out, name) {
			t.Errorf("metric %s from the specification is missing", name)
		}
	}
}

func TestHooksRecordTheRightSeries(t *testing.T) {
	m := New()
	m.OnPoll(collector.PollEvent{Duration: 10 * time.Millisecond})
	m.OnPoll(collector.PollEvent{Duration: 10 * time.Millisecond, Err: domain.Errorf(domain.CategoryTimeout, "no answer")})
	m.OnPoll(collector.PollEvent{Duration: 10 * time.Millisecond, Err: domain.Errorf(domain.CategoryAuthentication, "rejected")})
	out := scrape(t, m)
	has(t, out, `collector_requests_total{protocol="snmp",result="success"} 1`)
	has(t, out, `collector_requests_total{protocol="snmp",result="failure"} 2`)
	has(t, out, `collector_failures_total{category="timeout"} 1`)
	has(t, out, `collector_failures_total{category="authentication"} 1`)

	m.WorkerOutcome("normalizer")("processed", "s", time.Millisecond)
	m.WorkerOutcome("normalizer")("processed", "s", time.Millisecond)
	m.WorkerOutcome("normalizer")("dead_letter", "s", time.Millisecond)
	m.WorkerOutcome("normalizer")("retry", "s", time.Millisecond)
	out = scrape(t, m)
	has(t, out, `messages_processed_total{worker="normalizer"} 2`)
	has(t, out, `messages_failed_total{outcome="dead_letter",worker="normalizer"} 1`)
	has(t, out, `messages_failed_total{outcome="retry",worker="normalizer"} 1`)

	m.ScriptResult("transforms/x", time.Millisecond, nil)
	m.ScriptResult("transforms/x", time.Millisecond, &runtime.Error{Kind: runtime.KindException})
	m.ScriptResult("transforms/x", time.Millisecond, &runtime.Error{Kind: runtime.KindTimeout})
	m.ScriptResult("transforms/x", 0, runtime.ErrSaturated)
	out = scrape(t, m)
	has(t, out, `javascript_executions_total{script="transforms/x"} 4`)
	has(t, out, `javascript_failures_total{kind="exception",script="transforms/x"} 1`)
	has(t, out, `javascript_failures_total{kind="timeout",script="transforms/x"} 1`)
	has(t, out, `javascript_timeouts_total{script="transforms/x"} 1`)
	has(t, out, `javascript_saturations_total 1`)

	m.AutomationRequest("restart")
	m.AutomationResult("restart", domain.AutomationDryRun, nil)
	m.AutomationResult("restart", domain.AutomationFailed, nil)
	m.DatabaseError(domain.Errorf(domain.CategoryDependency, "x"))
	m.DatabaseError(errors.New("untyped"))
	out = scrape(t, m)
	has(t, out, `automation_requests_total{policy="restart"} 1`)
	has(t, out, `automation_results_total{policy="restart",status="dry_run"} 1`)
	has(t, out, `automation_failures_total{policy="restart"} 1`)
	has(t, out, `database_errors_total{category="dependency"} 1`)
	has(t, out, `database_errors_total{category="permanent"} 1`)
}

func TestLabelsAreLowCardinality(t *testing.T) {
	m := New()
	m.OnPoll(collector.PollEvent{Device: domain.Device{ID: "switch-01"}})
	m.WorkerOutcome("w")("processed", "telemetry.raw.snmp", time.Millisecond)
	out := scrape(t, m)
	for _, forbidden := range []string{"switch-01", "device_id", "observation_id", "telemetry.raw.snmp"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("metric output contains %q: per-device/per-message identity belongs in logs, not metric labels", forbidden)
		}
	}
}

func TestScriptingGaugesTrackQuarantine(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "transforms"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "transforms", "bad.js"), []byte("export const meta = {version: \"1\", metrics: [\"*\"]}\nexport function transform() { throw new Error(\"x\") }\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "transforms", "broken.js"), []byte("export function transform( {"), 0o644)
	cfg := config.Default().Scripting
	cfg.Directories, cfg.QuarantineAfter = []string{dir}, 2
	svc, _ := scripting.New(cfg, nil, nil, slog.New(slog.DiscardHandler))
	defer svc.Close()
	m := New()
	m.AttachScripting(svc)
	out := scrape(t, m)
	has(t, out, `javascript_scripts_now{status="active"} 1`)
	has(t, out, `javascript_scripts_now{status="failed"} 1`)
	for i := 0; i < 2; i++ {
		_, _ = svc.Call(context.Background(), "transforms/bad", "transform", nil, 1)
	}
	out = scrape(t, m)
	has(t, out, `javascript_scripts_now{status="quarantined"} 1`)
	has(t, out, `javascript_scripts_now{status="active"} 0`)
	has(t, out, `javascript_quarantines_total{script="transforms/bad"} 1`)
	has(t, out, `javascript_failures_total{kind="exception",script="transforms/bad"} 2`)
}

func TestBacklogAndWorkerGaugesAgainstRealJetStream(t *testing.T) {
	n := testutil.StartNATS(t)
	c, err := messaging.Connect(n.URL(), "test", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	m := New()
	release := make(chan struct{})
	opts := messaging.WorkerOptions{Name: "w", Stream: messaging.StreamTelemetryRaw, Durable: "obs-test", FilterSubject: messaging.SubjectRawSNMP,
		Shards: 1, QueueSize: 1, MaxDeliver: 3, AckWait: 30 * time.Second, OnOutcome: m.WorkerOutcome("w")}
	w := c.NewWorker(opts, func(context.Context, messaging.Message) error { <-release; return nil })
	m.AttachWorker("w", w)
	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(wctx) }()
	defer func() { close(release); wcancel(); <-done }()

	for i := 0; i < 30; i++ {
		if err := c.Publish(ctx, messaging.SubjectRawSNMP, string(rune('a'+i%26))+string(rune('0'+i/26)), []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	go m.WatchBacklog(ctx, c, 50*time.Millisecond, Consumer{Stream: messaging.StreamTelemetryRaw, Durable: "obs-test"})
	deadline := time.Now().Add(5 * time.Second)
	var out string
	for time.Now().Before(deadline) {
		out = scrape(t, m)
		if strings.Contains(out, `observation_queue_depth{consumer="obs-test"} 2`) || strings.Contains(out, `observation_queue_depth{consumer="obs-test"} 3`) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	depth := grep(out, "observation_queue_depth")
	if !strings.Contains(depth, `consumer="obs-test"`) || strings.Contains(depth, `} 0`) {
		t.Fatalf("a stalled consumer must show its backlog, not zero: %s", depth)
	}
	has(t, out, `worker_capacity_now{worker="w"} 2`)
	has(t, out, `worker_in_flight_now{worker="w"} 1`)
}
