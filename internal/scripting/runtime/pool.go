package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Failure kinds carried by *Error.
const (
	KindException = "exception" // the script threw
	KindTimeout   = "timeout"   // deadline exceeded or cancelled
	KindPanic     = "panic"     // a Go panic inside the interpreter or a host function
	KindOutput    = "output"    // the result could not be serialised or was too large
	KindLoad      = "load"      // the script failed while being loaded
	KindMissing   = "missing"   // the script or function is not available
)

// Error is a failure of script code, as opposed to a failure of the platform.
// It always has domain category script so callers can tell them apart.
type Error struct {
	ScriptID string
	Kind     string
	Message  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("script %s: %s: %s", e.ScriptID, e.Kind, e.Message)
}

// Category marks every script failure as such, so it is never mistaken for a
// failure of the platform itself.
func (e *Error) Category() domain.Category { return domain.CategoryScript }

// ErrSaturated means every executor is busy and the queue is full for longer
// than the caller is willing to wait. It is a platform condition (retry later),
// not a script fault.
var ErrSaturated = domain.Errorf(domain.CategoryTransient, "script executor saturated")

// Installer adds host objects to a fresh interpreter. It runs once per
// interpreter build, on the goroutine that owns it. Host functions read the
// current invocation through the Invocation they are given.
type Installer func(vm *goja.Runtime, inv *Current)

// Current exposes the invocation in progress to host functions. It is only
// valid while a call is running on this worker.
type Current struct {
	Ctx      context.Context
	ScriptID string
	Function string
	// Data is caller-supplied per-call context (for example the host API
	// budget). Host functions must treat it as read-only.
	Data any
}

// Source provides the current set of programs and a version counter. The pool
// rebuilds an interpreter when the counter changes, which is how reload works
// without restarting or locking workers.
type Source interface {
	Programs() []*Program
	Generation() uint64
}

// Options configure a Pool.
type Options struct {
	Workers    int
	QueueSize  int
	Timeout    time.Duration // per-invocation deadline
	QueueWait  time.Duration // how long a caller waits for a free slot before ErrSaturated
	MaxOutput  int           // bytes of serialised result
	Installers []Installer
}

// Stats are cumulative counters, safe to read at any time.
type Stats struct {
	Calls       uint64
	Failures    uint64
	Timeouts    uint64
	Panics      uint64
	Saturations uint64
	Rebuilds    uint64
	QueueDepth  int
}

type job struct {
	ctx      context.Context
	scriptID string
	fn       string
	args     []any
	data     any
	reply    chan result
}

type result struct {
	out json.RawMessage
	err error
}

// Pool runs script calls on a fixed set of workers. Each worker owns one
// interpreter for its whole life: goja interpreters are not safe for
// concurrent use, and one interpreter behind a mutex would serialise every
// script call in the process. Runtime-per-worker gives parallelism with no
// locking around script execution; the cost is memory per worker and the rule
// that scripts must not rely on module-level state (each worker has its own).
type Pool struct {
	opts   Options
	src    Source
	jobs   chan *job
	wg     sync.WaitGroup
	closed atomic.Bool

	calls, failures, timeouts, panics, saturations, rebuilds atomic.Uint64
}

// NewPool starts the workers.
func NewPool(src Source, opts Options) *Pool {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	if opts.QueueSize < 1 {
		opts.QueueSize = 1
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 250 * time.Millisecond
	}
	if opts.QueueWait <= 0 {
		opts.QueueWait = 2 * time.Second
	}
	if opts.MaxOutput <= 0 {
		opts.MaxOutput = 1 << 20
	}
	p := &Pool{opts: opts, src: src, jobs: make(chan *job, opts.QueueSize)}
	for i := 0; i < opts.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

// Close stops the workers after in-flight jobs finish.
func (p *Pool) Close() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.jobs)
		p.wg.Wait()
	}
}

// Stats returns a snapshot of the counters.
func (p *Pool) Stats() Stats {
	return Stats{
		Calls: p.calls.Load(), Failures: p.failures.Load(), Timeouts: p.timeouts.Load(), Panics: p.panics.Load(),
		Saturations: p.saturations.Load(), Rebuilds: p.rebuilds.Load(), QueueDepth: len(p.jobs),
	}
}

// Call invokes an exported function of a script. args are serialised to JSON
// and parsed inside the interpreter, so the script sees plain JavaScript
// values. data is opaque per-call context for host functions. The result is
// the JSON serialisation of the return value ("null" for undefined).
func (p *Pool) Call(ctx context.Context, scriptID, fn string, data any, args ...any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, domain.Wrap(domain.CategoryTimeout, "script call", err)
	}
	j := &job{ctx: ctx, scriptID: scriptID, fn: fn, args: args, data: data, reply: make(chan result, 1)}
	wait := time.NewTimer(p.opts.QueueWait)
	defer wait.Stop()
	select {
	case p.jobs <- j:
	case <-ctx.Done():
		return nil, domain.Wrap(domain.CategoryTimeout, "script queue", ctx.Err())
	case <-wait.C:
		p.saturations.Add(1)
		return nil, ErrSaturated
	}
	r := <-j.reply
	return r.out, r.err
}

// worker owns one interpreter and serves jobs until the pool closes.
type worker struct {
	p        *Pool
	vm       *goja.Runtime
	gen      uint64
	exports  map[string]*goja.Object
	loadErr  map[string]string
	parse    goja.Callable
	stringfy goja.Callable
	cur      Current
	dirty    bool // the interpreter was interrupted or panicked: rebuild before reuse
}

func (p *Pool) worker() {
	defer p.wg.Done()
	w := &worker{p: p}
	for j := range p.jobs {
		w.run(j)
	}
}

func (w *worker) run(j *job) {
	p := w.p
	p.calls.Add(1)
	out, err := w.call(j)
	if err != nil {
		p.failures.Add(1)
		var se *Error
		if errors.As(err, &se) {
			switch se.Kind {
			case KindTimeout:
				p.timeouts.Add(1)
			case KindPanic:
				p.panics.Add(1)
			}
		}
	}
	j.reply <- result{out: out, err: err}
}

// build (re)creates the interpreter and loads every current program into it.
func (w *worker) build() {
	w.p.rebuilds.Add(1)
	w.dirty = false
	vm := goja.New()
	vm.SetMaxCallStackSize(512)
	w.vm, w.gen = vm, w.p.src.Generation()
	w.exports, w.loadErr = map[string]*goja.Object{}, map[string]string{}
	w.cur = Current{}
	cur := &w.cur
	for _, inst := range w.p.opts.Installers {
		inst(vm, cur)
	}
	// Freeze host globals so a script cannot replace `log` or `device`.
	_, _ = vm.RunString(`(function(){var g=this;["log","device","inventory","state","config","events","metrics","http"].forEach(function(n){if(g[n]&&typeof g[n]==="object")Object.freeze(g[n])})})()`)
	jsObj := vm.Get("JSON").ToObject(vm)
	w.parse, _ = goja.AssertFunction(jsObj.Get("parse"))
	w.stringfy, _ = goja.AssertFunction(jsObj.Get("stringify"))
	for _, prog := range w.p.src.Programs() {
		w.load(prog)
	}
}

// load runs the script's top level (under the same deadline as a call) and
// keeps its exports object.
func (w *worker) load(prog *Program) {
	err := w.guard(context.Background(), prog.ID, "<load>", func() error {
		v, err := w.vm.RunProgram(prog.prog)
		if err != nil {
			return err
		}
		obj := v.ToObject(w.vm)
		w.exports[prog.ID] = obj
		return nil
	})
	if err != nil {
		w.loadErr[prog.ID] = err.Error()
	}
}

// guard runs fn under the invocation deadline with panic recovery, translating
// every outcome into *Error.
func (w *worker) guard(ctx context.Context, scriptID, fn string, body func() error) (err error) {
	vm := w.vm // captured: the worker may drop w.vm after a failure while a callback is still running
	cctx, cancel := context.WithTimeout(ctx, w.p.opts.Timeout)
	defer cancel()
	fired := make(chan struct{})
	stop := context.AfterFunc(cctx, func() {
		defer close(fired)
		vm.Interrupt(cctx.Err())
	})
	defer func() {
		// If the deadline callback already started, let it finish before
		// clearing: otherwise its Interrupt could land after ClearInterrupt and
		// poison the next call on this interpreter.
		if !stop() {
			<-fired
		}
		vm.ClearInterrupt()
		if r := recover(); r != nil {
			w.dirty = true
			err = &Error{ScriptID: scriptID, Kind: KindPanic, Message: fmt.Sprint(r)}
		}
	}()
	if e := body(); e != nil {
		te := translate(scriptID, fn, e)
		var se *Error
		if errors.As(te, &se) && (se.Kind == KindTimeout || se.Kind == KindPanic) {
			w.dirty = true
		}
		// A deadline the caller imposed (or a cancellation) is a platform
		// condition, not a fault of the script; only the script's own
		// execution deadline counts against it.
		if errors.As(te, &se) && se.Kind == KindTimeout && ctx.Err() != nil {
			return domain.Wrap(domain.CategoryTimeout, "script call cancelled by caller", ctx.Err())
		}
		return te
	}
	return nil
}

func translate(scriptID, fn string, err error) error {
	var ie *goja.InterruptedError
	if errors.As(err, &ie) {
		return &Error{ScriptID: scriptID, Kind: KindTimeout, Message: fmt.Sprintf("%s exceeded its execution deadline (%v)", fn, ie.Value())}
	}
	var ex *goja.Exception
	if errors.As(err, &ex) {
		return &Error{ScriptID: scriptID, Kind: KindException, Message: firstLine(ex.String())}
	}
	var se *Error
	if errors.As(err, &se) {
		return se
	}
	return &Error{ScriptID: scriptID, Kind: KindException, Message: firstLine(err.Error())}
}

func firstLine(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			return s[:i]
		}
	}
	return s
}

func (w *worker) call(j *job) (json.RawMessage, error) {
	// State after an interrupt or panic is not trusted, so the interpreter is
	// rebuilt; a changed generation (reload, quarantine) rebuilds it too.
	if w.vm == nil || w.dirty || w.gen != w.p.src.Generation() {
		w.build()
	}
	if msg, bad := w.loadErr[j.scriptID]; bad {
		return nil, &Error{ScriptID: j.scriptID, Kind: KindLoad, Message: msg}
	}
	exp, ok := w.exports[j.scriptID]
	if !ok {
		return nil, &Error{ScriptID: j.scriptID, Kind: KindMissing, Message: "script is not loaded"}
	}
	fnVal := exp.Get(j.fn)
	fn, ok := goja.AssertFunction(fnVal)
	if !ok {
		return nil, &Error{ScriptID: j.scriptID, Kind: KindMissing, Message: fmt.Sprintf("script does not export a function %q", j.fn)}
	}
	w.cur = Current{Ctx: j.ctx, ScriptID: j.scriptID, Function: j.fn, Data: j.data}

	var raw json.RawMessage
	err := w.guard(j.ctx, j.scriptID, j.fn, func() error {
		args := make([]goja.Value, len(j.args))
		for i, a := range j.args {
			b, err := json.Marshal(a)
			if err != nil {
				return &Error{ScriptID: j.scriptID, Kind: KindOutput, Message: "argument is not serialisable: " + err.Error()}
			}
			v, err := w.parse(goja.Undefined(), w.vm.ToValue(string(b)))
			if err != nil {
				return err
			}
			args[i] = v
		}
		ret, err := fn(goja.Undefined(), args...)
		if err != nil {
			return err
		}
		if ret == nil || goja.IsUndefined(ret) {
			raw = json.RawMessage("null")
			return nil
		}
		s, err := w.stringfy(goja.Undefined(), ret)
		if err != nil {
			return err
		}
		if goja.IsUndefined(s) { // e.g. a function was returned
			return &Error{ScriptID: j.scriptID, Kind: KindOutput, Message: "return value is not JSON-serialisable"}
		}
		str := s.String()
		if len(str) > w.p.opts.MaxOutput {
			return &Error{ScriptID: j.scriptID, Kind: KindOutput, Message: fmt.Sprintf("result is %d bytes, limit %d", len(str), w.p.opts.MaxOutput)}
		}
		raw = json.RawMessage(str)
		return nil
	})
	w.cur = Current{}
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// ReadExport evaluates a program in a scratch interpreter and returns the JSON
// of one exported value (used to read `meta` without involving the pool). It
// also proves the script loads: top-level code runs under the deadline.
func ReadExport(p *Program, name string, timeout time.Duration) (json.RawMessage, error) {
	vm := goja.New()
	vm.SetMaxCallStackSize(512)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { vm.Interrupt(ctx.Err()) })
	defer stop()
	var raw json.RawMessage
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &Error{ScriptID: p.ID, Kind: KindPanic, Message: fmt.Sprint(r)}
			}
		}()
		v, err := vm.RunProgram(p.prog)
		if err != nil {
			return translate(p.ID, "<load>", err)
		}
		val := v.ToObject(vm).Get(name)
		if val == nil || goja.IsUndefined(val) {
			raw = json.RawMessage("null")
			return nil
		}
		stringify, _ := goja.AssertFunction(vm.Get("JSON").ToObject(vm).Get("stringify"))
		s, err := stringify(goja.Undefined(), val)
		if err != nil {
			return translate(p.ID, name, err)
		}
		raw = json.RawMessage(s.String())
		return nil
	}()
	return raw, err
}
