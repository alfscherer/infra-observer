// Package api is the entire surface JavaScript extensions can touch.
//
// The design rule is that a script gets a handful of typed functions and
// nothing else: no filesystem, no process execution, no raw network, no
// database handle, no NATS connection, no Go objects. Every function here
//
//   - takes and returns plain JSON-shaped values,
//   - is denied outright for extension kinds that have no business using it,
//   - draws on a per-invocation budget so a script cannot flood logs, events
//     or outbound requests,
//   - and validates what the script hands it before acting.
//
// Scripts can propose; the Go layer decides. Nothing in this package executes
// an automation action.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/dop251/goja"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

// Per-invocation limits.
const (
	MaxLogLines        = 20
	MaxHTTPCalls       = 3
	MaxEvents          = 10
	MaxObservations    = 100
	MaxLookupResults   = 200
	MaxRequestBody     = 64 << 10
	MaxResponseBody    = 256 << 10
	MaxLogMessage      = 512
	MaxLogFields       = 16
	MaxLogFieldValue   = 256
	defaultHTTPTimeout = 2 * time.Second
)

// StateReader gives read access to the latest stored value of a series.
type StateReader interface {
	Latest(ctx context.Context, deviceID, metric string, labels map[string]string) (*domain.Observation, error)
}

// Deps are the platform services the host API is allowed to use. The zero value
// of an optional dependency simply disables the functions that need it.
type Deps struct {
	Inventory inventory.Reader
	State     StateReader
	Endpoints map[string]config.Endpoint
	Secrets   secrets.Resolver
	HTTP      *http.Client
	Log       *slog.Logger
	Validator schema.Validator
	Now       func() time.Time

	// OnHTTP is a metrics hook for outbound requests.
	OnHTTP func(script, endpoint string, status int, d time.Duration, err error)
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// permissions says which optional capabilities each extension kind may use.
// Transforms and enrichers run once per observation and are deliberately pure:
// no I/O of any kind. Automation proposers are read-only. Only integrations
// and collectors, which exist to talk to the outside world, may call out.
var permissions = map[string]struct{ HTTP, Events, Metrics bool }{
	registry.KindTransform:   {},
	registry.KindEnricher:    {},
	registry.KindAutomation:  {},
	registry.KindIntegration: {HTTP: true, Events: true, Metrics: true},
	registry.KindCollector:   {HTTP: true, Metrics: true},
}

// ScriptInfo identifies the script an invocation belongs to.
type ScriptInfo struct {
	Key     string // kind/id
	Kind    string
	ID      string
	Version string
	Config  map[string]any
}

// Call carries per-invocation state: identity, correlation, budgets and the
// side effects a script requested. It is created by the caller for each
// invocation, passed to Pool.Call as its data argument, and read afterwards.
type Call struct {
	Script        ScriptInfo
	CorrelationID string
	// Trigger is the ID of the thing being processed (for example the event a
	// handler is reacting to). It makes emitted events and observations
	// deterministic, so a redelivered trigger cannot emit duplicates.
	Trigger  string
	DeviceID string

	logLines, httpCalls int
	events              []domain.Event
	observations        []domain.Observation
}

// NewCall starts the bookkeeping for one invocation.
func NewCall(info ScriptInfo, correlationID, trigger, deviceID string) *Call {
	return &Call{Script: info, CorrelationID: correlationID, Trigger: trigger, DeviceID: deviceID}
}

// Events returns the events the script asked to emit.
func (c *Call) Events() []domain.Event { return c.events }

// Observations returns the observations the script asked to emit.
func (c *Call) Observations() []domain.Observation { return c.observations }

var (
	eventTypeRe = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$`)
	metricRe    = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$`)
)

// Installer returns the runtime hook that installs the host objects into every
// interpreter the pool builds.
func Installer(d Deps) runtime.Installer {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Validator.Limits.MaxMessageBytes == 0 {
		d.Validator = schema.NewValidator()
	}
	if d.HTTP == nil {
		d.HTTP = &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return func(vm *goja.Runtime, cur *runtime.Current) {
		h := &host{d: d, vm: vm, cur: cur}
		h.parse, _ = goja.AssertFunction(vm.Get("JSON").ToObject(vm).Get("parse"))
		h.stringify, _ = goja.AssertFunction(vm.Get("JSON").ToObject(vm).Get("stringify"))
		h.installLog()
		h.installDevice()
		h.installInventory()
		h.installState()
		h.installConfig()
		h.installEvents()
		h.installMetrics()
		h.installHTTP()
	}
}

type host struct {
	d         Deps
	vm        *goja.Runtime
	cur       *runtime.Current
	parse     goja.Callable
	stringify goja.Callable
}

// call returns the invocation in progress, or throws: host functions do not
// work while a script's top-level code is being loaded, which keeps loading
// free of side effects.
func (h *host) call() *Call {
	c, ok := h.cur.Data.(*Call)
	if !ok || c == nil {
		panic(h.vm.NewTypeError("host API is only available while a function is being called"))
	}
	return c
}

func (h *host) throw(format string, args ...any) {
	panic(h.vm.NewTypeError("%s", fmt.Sprintf(format, args...)))
}

// toJS converts a Go value into a plain JavaScript value by way of JSON, so
// the script never receives a wrapper around Go memory.
func (h *host) toJS(v any) goja.Value {
	b, err := json.Marshal(v)
	if err != nil {
		h.throw("cannot convert value: %v", err)
	}
	out, err := h.parse(goja.Undefined(), h.vm.ToValue(string(b)))
	if err != nil {
		h.throw("cannot convert value: %v", err)
	}
	return out
}

// fromJS decodes a JavaScript value into dst via JSON.
func (h *host) fromJS(v goja.Value, dst any) error {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return fmt.Errorf("a value is required")
	}
	s, err := h.stringify(goja.Undefined(), v)
	if err != nil || goja.IsUndefined(s) {
		return fmt.Errorf("value is not JSON-serialisable")
	}
	dec := json.NewDecoder(stringsReader(s.String()))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func (h *host) object(name string, fns map[string]func(goja.FunctionCall) goja.Value) {
	o := h.vm.NewObject()
	for k, f := range fns {
		_ = o.Set(k, f)
	}
	_ = h.vm.Set(name, o)
}

func (h *host) require(allowed bool, fn string) *Call {
	c := h.call()
	if !allowed {
		h.throw("%s is not available to %s scripts", fn, c.Script.Kind)
	}
	return c
}

func contextWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}

func (h *host) ctx() context.Context {
	if h.cur.Ctx != nil {
		return h.cur.Ctx
	}
	return context.Background()
}
