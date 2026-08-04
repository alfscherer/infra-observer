package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dop251/goja"

	"github.com/alfscherer/infra-observer/internal/domain"
)

type staticSource struct {
	mu    sync.Mutex
	progs []*Program
	gen   uint64
}

func (s *staticSource) Programs() []*Program { s.mu.Lock(); defer s.mu.Unlock(); return s.progs }
func (s *staticSource) Generation() uint64   { s.mu.Lock(); defer s.mu.Unlock(); return s.gen }
func (s *staticSource) Set(p ...*Program)    { s.mu.Lock(); s.progs = p; s.gen++; s.mu.Unlock() }

func mustCompile(t *testing.T, id, src string) *Program {
	t.Helper()
	p, err := Compile(id, src)
	if err != nil {
		t.Fatalf("compile %s: %v", id, err)
	}
	return p
}

func newPool(t *testing.T, opts Options, progs ...*Program) (*Pool, *staticSource) {
	t.Helper()
	src := &staticSource{progs: progs, gen: 1}
	if opts.Workers == 0 {
		opts.Workers = 2
	}
	if opts.Timeout == 0 {
		opts.Timeout = 200 * time.Millisecond
	}
	p := NewPool(src, opts)
	t.Cleanup(p.Close)
	return p, src
}

func call(p *Pool, id, fn string, args ...any) (json.RawMessage, error) {
	return p.Call(context.Background(), id, fn, nil, args...)
}

const transformSrc = `
export const meta = {version: "1.0.0"};
export function transform(o) {
    if (o.metric === "vendor.cpu.load") {
        return {...o, metric: "system.cpu.utilization", value: o.value / 100.0};
    }
    return o;
}`

func TestCallRoundTripsJSONAndSupportsSpread(t *testing.T) {
	p, _ := newPool(t, Options{}, mustCompile(t, "t", transformSrc))
	out, err := call(p, "t", "transform", map[string]any{"metric": "vendor.cpu.load", "value": 40.0, "labels": map[string]string{"a": "b"}})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(out, &got)
	if got["metric"] != "system.cpu.utilization" || got["value"] != 0.4 || got["labels"].(map[string]any)["a"] != "b" {
		t.Fatalf("%s", out)
	}
	// untouched observation comes back as-is
	out, _ = call(p, "t", "transform", map[string]any{"metric": "other", "value": 1.0})
	if !strings.Contains(string(out), `"other"`) {
		t.Fatalf("%s", out)
	}
}

func TestUndefinedAndNullReturnNull(t *testing.T) {
	p, _ := newPool(t, Options{}, mustCompile(t, "n", "export function a(){}\nexport function b(){return null}"))
	for _, fn := range []string{"a", "b"} {
		if out, err := call(p, "n", fn); err != nil || string(out) != "null" {
			t.Fatalf("%s: %q %v", fn, out, err)
		}
	}
}

func TestScriptCannotMutateCallerData(t *testing.T) {
	p, _ := newPool(t, Options{}, mustCompile(t, "m", `export function mutate(o){ o.labels.a = "changed"; o.extra = 1; return o }`))
	in := map[string]any{"labels": map[string]string{"a": "orig"}}
	if _, err := call(p, "m", "mutate", in); err != nil {
		t.Fatal(err)
	}
	if in["labels"].(map[string]string)["a"] != "orig" || in["extra"] != nil {
		t.Fatal("the script received a reference into Go memory instead of a copy")
	}
}

func TestExceptionsAreScriptErrorsNotCrashes(t *testing.T) {
	p, _ := newPool(t, Options{}, mustCompile(t, "e", `export function boom(){ throw new Error("kaput") }
export function ref(){ return nope.x }`))
	for fn, want := range map[string]string{"boom": "kaput", "ref": "nope"} {
		_, err := call(p, "e", fn)
		var se *Error
		if !errors.As(err, &se) || se.Kind != KindException || !strings.Contains(se.Message, want) {
			t.Fatalf("%s: %v", fn, err)
		}
		if domain.CategoryOf(err) != domain.CategoryScript || domain.IsRetryable(err) {
			t.Fatalf("script errors must be category script and not retryable, got %s", domain.CategoryOf(err))
		}
	}
	// the worker is still healthy afterwards
	if _, err := call(p, "e", "nope"); err == nil {
		t.Fatal("missing function must be an error")
	}
}

func TestInfiniteLoopHitsDeadlineAndPoolSurvives(t *testing.T) {
	p, _ := newPool(t, Options{Workers: 1, Timeout: 80 * time.Millisecond}, mustCompile(t, "l", `
export function spin(){ for(;;){} }
export function ok(){ return 1 }`))
	start := time.Now()
	_, err := call(p, "l", "spin")
	var se *Error
	if !errors.As(err, &se) || se.Kind != KindTimeout {
		t.Fatalf("expected timeout, got %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("deadline not enforced promptly: %v", took)
	}
	// the same single worker must serve the next call: the process did not hang or die
	out, err := call(p, "l", "ok")
	if err != nil || string(out) != "1" {
		t.Fatalf("pool did not recover: %q %v", out, err)
	}
	st := p.Stats()
	if st.Timeouts != 1 || st.Failures != 1 || st.Calls != 2 || st.Rebuilds < 2 {
		t.Fatalf("stats: %+v (an interrupted interpreter must be rebuilt)", st)
	}
}

func TestContextCancellationInterruptsScript(t *testing.T) {
	p, _ := newPool(t, Options{Timeout: 100 * time.Millisecond}, mustCompile(t, "l", `export function spin(){ while(true){} }`))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := p.Call(ctx, "l", "spin", nil)
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("caller cancellation must stop the script promptly: %v after %v", err, time.Since(start))
	}
	if domain.CategoryOf(err) != domain.CategoryTimeout {
		t.Fatalf("a caller's deadline is a platform condition, not a script fault: category %s", domain.CategoryOf(err))
	}
	// and the pool is healthy afterwards
	if _, err := p.Call(context.Background(), "l", "spin", nil); err == nil {
		t.Fatal("still the same infinite loop; only checking the worker was rebuilt and answers")
	}
}

func TestRunawayRecursionIsContained(t *testing.T) {
	p, _ := newPool(t, Options{}, mustCompile(t, "r", `export function f(n){ return f(n+1) + 1 }`))
	_, err := call(p, "r", "f", 0)
	var se *Error
	if !errors.As(err, &se) {
		t.Fatalf("stack overflow must surface as a script error, got %v", err)
	}
}

func TestHostPanicIsRecovered(t *testing.T) {
	inst := func(vm *goja.Runtime, _ *Current) {
		_ = vm.Set("explode", func(goja.FunctionCall) goja.Value { panic("host bug") })
	}
	p, _ := newPool(t, Options{Installers: []Installer{inst}}, mustCompile(t, "h", "export function f(){ explode() }\nexport function ok(){ return 2 }"))
	_, err := call(p, "h", "f")
	var se *Error
	if !errors.As(err, &se) || se.Kind != KindPanic {
		t.Fatalf("host panic must be a script-side failure, got %v", err)
	}
	if out, err := call(p, "h", "ok"); err != nil || string(out) != "2" {
		t.Fatalf("worker must survive a panic: %q %v", out, err)
	}
	if p.Stats().Panics != 1 {
		t.Fatal("panic not counted")
	}
}

func TestOutputLimitAndNonSerialisable(t *testing.T) {
	p, _ := newPool(t, Options{MaxOutput: 1000}, mustCompile(t, "o", `
export function big(){ return "x".repeat(5000) }
export function fn(){ return function(){} }`))
	for _, name := range []string{"big", "fn"} {
		_, err := call(p, "o", name)
		var se *Error
		if !errors.As(err, &se) || se.Kind != KindOutput {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestScriptsAreIsolatedFromEachOther(t *testing.T) {
	a := mustCompile(t, "a", `let counter = 0
var leaked = "a"
export function next(){ counter++; return counter }
export function seeLeak(){ return typeof otherLeak }`)
	b := mustCompile(t, "b", `var otherLeak = "b"
export function next(){ return "b" }`)
	p, _ := newPool(t, Options{Workers: 1}, a, b)
	if out, _ := call(p, "a", "next"); string(out) != "1" {
		t.Fatalf("%s", out)
	}
	if out, _ := call(p, "b", "next"); string(out) != `"b"` {
		t.Fatalf("same export name in two scripts must not collide: %s", out)
	}
	if out, _ := call(p, "a", "seeLeak"); string(out) != `"undefined"` {
		t.Fatalf("script b's variables must not be visible to script a: %s", out)
	}
}

func TestHostGlobalsAreFrozen(t *testing.T) {
	inst := func(vm *goja.Runtime, _ *Current) {
		o := vm.NewObject()
		_ = o.Set("info", func(goja.FunctionCall) goja.Value { return vm.ToValue("real") })
		_ = vm.Set("log", o)
	}
	p, _ := newPool(t, Options{Installers: []Installer{inst}}, mustCompile(t, "f", `
export function tamper(){ try { log.info = function(){ return "hijacked" } } catch(e) {} return log.info() }`))
	out, err := call(p, "f", "tamper")
	if err != nil || string(out) != `"real"` {
		t.Fatalf("scripts must not be able to replace host functions: %q %v", out, err)
	}
}

func TestNoAmbientAuthority(t *testing.T) {
	p, _ := newPool(t, Options{}, mustCompile(t, "g", `
export function probe(){
  return ["require","process","fetch","XMLHttpRequest","setTimeout","setInterval","Deno","Bun","module","__dirname","os","fs","child_process"]
    .filter(function(n){ return typeof globalThis[n] !== "undefined" });
}`))
	out, err := call(p, "g", "probe")
	if err != nil || string(out) != "[]" {
		t.Fatalf("the interpreter must expose no I/O, timers or module loader by default: %q %v", out, err)
	}
}

func TestConcurrentCallsUseSeparateInterpreters(t *testing.T) {
	p, _ := newPool(t, Options{Workers: 4, QueueSize: 64}, mustCompile(t, "c", `export function id(x){ let s = 0; for (let i = 0; i < 2000; i++) s += i; return {x: x, s: s} }`))
	var wg sync.WaitGroup
	var bad atomic.Int32
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := call(p, "c", "id", i)
			var r struct{ X, S int }
			if err != nil || json.Unmarshal(out, &r) != nil || r.X != i || r.S != 1999000 {
				bad.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d wrong results under concurrency (interpreter shared between goroutines?)", bad.Load())
	}
}

func TestSaturationIsVisibleAndBounded(t *testing.T) {
	p, _ := newPool(t, Options{Workers: 1, QueueSize: 1, Timeout: 400 * time.Millisecond, QueueWait: 30 * time.Millisecond},
		mustCompile(t, "s", `export function slow(){ const end = Date.now() + 150; while (Date.now() < end) {} return 1 }`))
	var wg sync.WaitGroup
	var saturated atomic.Int32
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := call(p, "s", "slow")
			if errors.Is(err, ErrSaturated) {
				saturated.Add(1)
				if !domain.IsRetryable(err) || domain.CategoryOf(err) == domain.CategoryScript {
					t.Errorf("saturation is a retryable platform condition, not a script fault: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if saturated.Load() == 0 || p.Stats().Saturations == 0 {
		t.Fatal("an overloaded executor must reject work visibly instead of queueing without bound")
	}
}

func TestReloadRebuildsInterpreters(t *testing.T) {
	p, src := newPool(t, Options{Workers: 1}, mustCompile(t, "v", `export function version(){ return 1 }`))
	if out, _ := call(p, "v", "version"); string(out) != "1" {
		t.Fatal(string(out))
	}
	src.Set(mustCompile(t, "v", `export function version(){ return 2 }`))
	if out, _ := call(p, "v", "version"); string(out) != "2" {
		t.Fatalf("new generation not picked up: %s", out)
	}
	src.Set() // script removed
	_, err := call(p, "v", "version")
	var se *Error
	if !errors.As(err, &se) || se.Kind != KindMissing {
		t.Fatalf("removed script: %v", err)
	}
}

func TestLoadTimeFailuresAreIsolatedPerScript(t *testing.T) {
	good := mustCompile(t, "good", `export function f(){ return "ok" }`)
	throws := mustCompile(t, "throws", "throw new Error(\"at load\")\nexport function f(){}")
	loops := mustCompile(t, "loops", "for(;;){}\nexport function f(){}")
	p, _ := newPool(t, Options{Workers: 1, Timeout: 60 * time.Millisecond}, throws, loops, good)
	if out, err := call(p, "good", "f"); err != nil || string(out) != `"ok"` {
		t.Fatalf("a script that breaks at load time must not take the others down: %q %v", out, err)
	}
	for _, id := range []string{"throws", "loops"} {
		_, err := call(p, id, "f")
		var se *Error
		if !errors.As(err, &se) || se.Kind != KindLoad {
			t.Fatalf("%s: %v", id, err)
		}
	}
}

func TestCompileRejectsUnsupportedSyntax(t *testing.T) {
	bad := map[string]string{
		"default export": `export default function(){}`,
		"export list":    `function a(){}; export { a }`,
		"import":         `import x from "y"; export function f(){}`,
		"nothing":        `function f(){}`,
		"same line":      `function a(){}; export function b(){}`,
		"syntax error":   `export function f( {`,
	}
	for name, src := range bad {
		if _, err := Compile("x", src); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	p, err := Compile("x", "export const meta = {}\nexport async function a(){}\n  export function b(){}")
	if err != nil || !p.HasExport("meta") || !p.HasExport("a") || !p.HasExport("b") {
		t.Fatalf("%v %v", p, err)
	}
}

func TestSyntaxErrorLineNumbersMatchTheFile(t *testing.T) {
	_, err := Compile("x", "export function f(){\n  return 1;\n}\nexport function g(){ return ) }\n")
	if err == nil || !strings.Contains(err.Error(), "Line 4") {
		t.Fatalf("error should point at line 4 of the original file: %v", err)
	}
}
