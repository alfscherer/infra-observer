package scripting

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

func writeScript(t *testing.T, dir, kind, name, src string) string {
	t.Helper()
	p := filepath.Join(dir, kind, name+".js")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func script(fn, body string) string {
	return "export const meta = {version: \"1.0.0\", description: \"test\", metrics: [\"*\"]}\nexport function " + fn + "(x) {\n" + body + "\n}\n"
}

func newService(t *testing.T, dir string, settings map[string]map[string]config.ScriptSettings) (*Service, registry.Report) {
	t.Helper()
	cfg := config.Default().Scripting
	cfg.Directories = []string{dir}
	cfg.Workers, cfg.ExecutionTimeout, cfg.QuarantineAfter = 2, 100*time.Millisecond, 3
	s, rep := New(cfg, settings, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(s.Close)
	return s, rep
}

func TestDiscoveryValidationAndIsolationOfBrokenScripts(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "transforms", "good", script("transform", "return x"))
	writeScript(t, dir, "transforms", "syntax", "export function transform( {")
	writeScript(t, dir, "transforms", "wrong-contract", script("enrich", "return x"))
	writeScript(t, dir, "transforms", "no-meta", "export function transform(x) { return x }\n")
	writeScript(t, dir, "transforms", "bad-version", "export const meta = {}\nexport function transform(x) { return x }\n")
	writeScript(t, dir, "transforms", "throws-at-load", "export const meta = {version: \"1\", metrics: [\"*\"]}\nthrow new Error(\"nope\")\nexport function transform(x) { return x }\n")
	writeScript(t, dir, "transforms", "spins-at-load", "export const meta = {version: \"1\", metrics: [\"*\"]}\nfor(;;){}\nexport function transform(x) { return x }\n")
	writeScript(t, dir, "enrichers", "e1", script("enrich", "return x"))
	writeScript(t, dir, "unknown-kind", "x", script("transform", "return x"))
	off := false
	svc, rep := newService(t, dir, map[string]map[string]config.ScriptSettings{"enrichers": {"e1": {Enabled: &off}}})

	if rep.Loaded != 1 || rep.Failed != 6 || rep.Disabled != 1 {
		t.Fatalf("report: %+v", rep)
	}
	byKey := map[string]registry.Script{}
	for _, s := range svc.Registry.List() {
		byKey[s.Key] = s
	}
	if g := byKey["transforms/good"]; g.Status != registry.StatusActive || g.Version != "1.0.0" || g.Description != "test" || g.Hash == "" {
		t.Fatalf("good: %+v", g)
	}
	for _, id := range []string{"syntax", "wrong-contract", "no-meta", "bad-version", "throws-at-load", "spins-at-load"} {
		if s := byKey["transforms/"+id]; s.Status != registry.StatusFailed || s.Reason == "" {
			t.Errorf("%s should be failed with a reason: %+v", id, s)
		}
	}
	if byKey["enrichers/e1"].Status != registry.StatusDisabled {
		t.Fatalf("configuration must be able to disable a script: %+v", byKey["enrichers/e1"])
	}
	if _, ok := byKey["unknown-kind/x"]; ok {
		t.Fatal("directories that are not a known extension kind are ignored")
	}
	// the healthy script keeps working next to six broken ones
	out, err := svc.Call(context.Background(), "transforms/good", "transform", nil, map[string]any{"a": 1})
	if err != nil || !strings.Contains(string(out), `"a":1`) {
		t.Fatalf("%s %v", out, err)
	}
	if _, err := svc.Call(context.Background(), "transforms/syntax", "transform", nil, 1); !errors.Is(err, ErrNotActive) {
		t.Fatalf("calls to inactive scripts are refused: %v", err)
	}
	h := svc.Health()
	if h.Loaded != 1 || h.Failed != 6 || h.Disabled != 1 {
		t.Fatalf("health: %+v", h)
	}
}

func TestRepeatedFailuresQuarantineAndFixByEditing(t *testing.T) {
	dir := t.TempDir()
	path := writeScript(t, dir, "transforms", "flaky", script("transform", "if (x.bad) throw new Error('bad input'); return x"))
	svc, _ := newService(t, dir, nil)
	var quarantined atomic.Int32
	svc.OnQuarantine = func(string) { quarantined.Add(1) }
	ctx := context.Background()

	if _, err := svc.Call(ctx, "transforms/flaky", "transform", nil, map[string]any{"bad": true}); err == nil {
		t.Fatal("expected script error")
	}
	if _, err := svc.Call(ctx, "transforms/flaky", "transform", nil, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	// a success resets the streak, so two more failures are still not enough
	for i := 0; i < 2; i++ {
		_, _ = svc.Call(ctx, "transforms/flaky", "transform", nil, map[string]any{"bad": true})
	}
	if s, _ := svc.Registry.Get("transforms/flaky"); s.Status != registry.StatusActive {
		t.Fatalf("2 consecutive failures must not quarantine yet: %+v", s)
	}
	_, _ = svc.Call(ctx, "transforms/flaky", "transform", nil, map[string]any{"bad": true}) // third in a row
	s, _ := svc.Registry.Get("transforms/flaky")
	if s.Status != registry.StatusQuarantined || quarantined.Load() != 1 || !strings.Contains(s.Reason, "bad input") {
		t.Fatalf("expected quarantine: %+v", s)
	}
	if _, err := svc.Call(ctx, "transforms/flaky", "transform", nil, map[string]any{"ok": true}); !errors.Is(err, ErrNotActive) {
		t.Fatalf("quarantined scripts must not execute: %v", err)
	}

	// a reload of unchanged source keeps the quarantine...
	svc.Reload()
	if s, _ := svc.Registry.Get("transforms/flaky"); s.Status != registry.StatusQuarantined {
		t.Fatal("reloading identical source must not silently lift a quarantine")
	}
	// ...but the operator can fix the file, and the new version starts clean.
	if err := os.WriteFile(path, []byte(strings.Replace(script("transform", "return x"), "1.0.0", "1.0.1", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	svc.Reload()
	s, _ = svc.Registry.Get("transforms/flaky")
	if s.Status != registry.StatusActive || s.Version != "1.0.1" || s.ConsecutiveFailures != 0 {
		t.Fatalf("edited script should be active again: %+v", s)
	}
	if _, err := svc.Call(ctx, "transforms/flaky", "transform", nil, map[string]any{"bad": true}); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutsCountAsScriptFailuresButSaturationDoesNot(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "transforms", "spin", script("transform", "for(;;){}"))
	svc, _ := newService(t, dir, nil)
	for i := 0; i < 3; i++ {
		_, err := svc.Call(context.Background(), "transforms/spin", "transform", nil, 1)
		var se *runtime.Error
		if !errors.As(err, &se) || se.Kind != runtime.KindTimeout {
			t.Fatalf("%v", err)
		}
	}
	if s, _ := svc.Registry.Get("transforms/spin"); s.Status != registry.StatusQuarantined {
		t.Fatalf("a script that always times out must be quarantined: %+v", s)
	}
	if svc.Health().Stats.Timeouts != 3 {
		t.Fatalf("stats: %+v", svc.Health().Stats)
	}
	// platform-side conditions never count against a script
	dir2 := t.TempDir()
	writeScript(t, dir2, "transforms", "fine", script("transform", "return x"))
	svc2, _ := newService(t, dir2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 10; i++ {
		_, _ = svc2.Call(ctx, "transforms/fine", "transform", nil, 1)
	}
	if s, _ := svc2.Registry.Get("transforms/fine"); s.Status != registry.StatusActive || s.TotalFailures != 0 {
		t.Fatalf("caller cancellation must not be blamed on the script: %+v", s)
	}
}

func TestOperatorEnableDisable(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "transforms", "t", script("transform", "return x"))
	svc, _ := newService(t, dir, nil)
	if !svc.Registry.SetEnabled("transforms/t", false) {
		t.Fatal("disable failed")
	}
	if _, err := svc.Call(context.Background(), "transforms/t", "transform", nil, 1); !errors.Is(err, ErrNotActive) {
		t.Fatalf("%v", err)
	}
	if !svc.Registry.SetEnabled("transforms/t", true) {
		t.Fatal("enable failed")
	}
	if _, err := svc.Call(context.Background(), "transforms/t", "transform", nil, 1); err != nil {
		t.Fatalf("re-enabled script must run again on the executors: %v", err)
	}
	if svc.Registry.SetEnabled("transforms/missing", true) {
		t.Fatal("unknown script")
	}
}

func TestWatchReloadsChangedFiles(t *testing.T) {
	dir := t.TempDir()
	path := writeScript(t, dir, "transforms", "w", script("transform", "return 1"))
	svc, _ := newService(t, dir, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.Watch(ctx, 20*time.Millisecond)

	out, _ := svc.Call(ctx, "transforms/w", "transform", nil, 0)
	if string(out) != "1" {
		t.Fatal(string(out))
	}
	time.Sleep(30 * time.Millisecond)
	if err := os.WriteFile(path, []byte(script("transform", "return 2")), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if out, _ := svc.Call(ctx, "transforms/w", "transform", nil, 0); string(out) == "2" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("edited script was not hot-reloaded")
}

func TestBrokenEditDoesNotTakeDownARunningScript(t *testing.T) {
	dir := t.TempDir()
	path := writeScript(t, dir, "transforms", "w", script("transform", "return 1"))
	svc, _ := newService(t, dir, nil)
	if err := os.WriteFile(path, []byte("export function transform( {"), 0o644); err != nil {
		t.Fatal(err)
	}
	rep := svc.Reload()
	if rep.Failed != 1 {
		t.Fatalf("%+v", rep)
	}
	s, _ := svc.Registry.Get("transforms/w")
	if s.Status != registry.StatusFailed || !strings.Contains(s.Reason, "syntax") {
		t.Fatalf("%+v", s)
	}
	// The process keeps running; fixing the file brings the script back.
	if err := os.WriteFile(path, []byte(script("transform", "return 3")), 0o644); err != nil {
		t.Fatal(err)
	}
	svc.Reload()
	if out, err := svc.Call(context.Background(), "transforms/w", "transform", nil, 0); err != nil || string(out) != "3" {
		t.Fatalf("%s %v", out, err)
	}
}
