// Package scripttest runs a script against JSON fixtures using the same
// interpreters, host API and contract validation as production.
//
// A fixture is a list of cases. Each case gives an input, and either the exact
// output (`expect`), part of it (`expect_subset`), `null` (the script drops
// the observation), `"unchanged"`, an error message fragment (`expect_error`),
// or `expect_timeout`. Every case is executed twice and the outputs compared:
// extensions must be deterministic.
package scripttest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

// Fixture is a set of test cases for one script.
type Fixture struct {
	Description    string         `json:"description"`
	Config         map[string]any `json:"config"`          // script config for every case, overridable per case
	AllowedActions []string       `json:"allowed_actions"` // automation scripts: the policy's allowed_actions
	Cases          []Case         `json:"cases"`
}

// HTTPMock scripts the responses of the endpoint an integration talks to.
type HTTPMock struct {
	Responses []struct {
		Method string `json:"method"`
		Path   string `json:"path"`
		Status int    `json:"status"`
		Body   string `json:"body"`
	} `json:"responses"`
}

// ExpectedRequest is a request the integration must have made.
type ExpectedRequest struct {
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	BodySubset json.RawMessage `json:"body_subset"`
}

// Case is one input and its expectation.
type Case struct {
	Name           string
	Device         string
	Config         map[string]any
	Input          json.RawMessage
	Context        json.RawMessage
	Expect         json.RawMessage
	HasExpect      bool
	ExpectSubset   json.RawMessage
	ExpectError    string
	ExpectTimeout  bool
	HTTP           *HTTPMock
	ExpectRequests []ExpectedRequest
	// HasExpectRequests is true when the key is present, even as an empty list
	// ("the script must make no requests").
	HasExpectRequests bool
}

// UnmarshalJSON records whether "expect" was present, because `"expect": null`
// (the script drops the observation) differs from no expectation at all.
func (c *Case) UnmarshalJSON(b []byte) error {
	var raw struct {
		Name           string            `json:"name"`
		Device         string            `json:"device"`
		Config         map[string]any    `json:"config"`
		Input          json.RawMessage   `json:"input"`
		Context        json.RawMessage   `json:"context"`
		ExpectSubset   json.RawMessage   `json:"expect_subset"`
		ExpectError    string            `json:"expect_error"`
		ExpectTimeout  bool              `json:"expect_timeout"`
		HTTP           *HTTPMock         `json:"http"`
		ExpectRequests []ExpectedRequest `json:"expect_requests"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var presence map[string]json.RawMessage
	_ = json.Unmarshal(b, &presence)
	c.Name, c.Device, c.Config, c.Input, c.Context = raw.Name, raw.Device, raw.Config, raw.Input, raw.Context
	c.ExpectSubset, c.ExpectError, c.ExpectTimeout = raw.ExpectSubset, raw.ExpectError, raw.ExpectTimeout
	c.HTTP, c.ExpectRequests = raw.HTTP, raw.ExpectRequests
	_, c.HasExpectRequests = presence["expect_requests"]
	c.Expect, c.HasExpect = presence["expect"], false
	if _, ok := presence["expect"]; ok {
		c.HasExpect = true
	}
	return nil
}

// Result is the outcome of one case.
type Result struct {
	Name     string
	Pass     bool
	Message  string
	Output   string
	Duration time.Duration
}

// Options configure a run.
type Options struct {
	Timeout   time.Duration
	Inventory inventory.Reader
	Log       *slog.Logger
}

// LoadFixture reads a fixture file. A file without a "cases" key is treated
// as a single raw input with no expectation (it is only run and validated).
func LoadFixture(path string) (Fixture, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Fixture{}, err
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(b, &probe); err != nil {
		return Fixture{}, fmt.Errorf("fixture %s: %w", path, err)
	}
	if _, ok := probe["cases"]; !ok {
		return Fixture{Description: "raw input " + filepath.Base(path), Cases: []Case{{Name: filepath.Base(path), Input: b}}}, nil
	}
	var fx Fixture
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&fx); err != nil {
		return Fixture{}, fmt.Errorf("fixture %s: %w", path, err)
	}
	return fx, nil
}

// KindAndID derives the extension kind and id from a path like
// scripts/transforms/normalize-cpu.js.
func KindAndID(path string) (kind, id string, err error) {
	id = strings.TrimSuffix(filepath.Base(path), ".js")
	kind = filepath.Base(filepath.Dir(path))
	if _, ok := registry.Contract[kind]; !ok {
		return "", "", fmt.Errorf("%s: parent directory %q is not a script kind (%v)", path, kind, kinds())
	}
	return kind, id, nil
}

func kinds() []string {
	var k []string
	for name := range registry.Contract {
		k = append(k, name)
	}
	return k
}

// httpMock is a local HTTP server standing in for an integration's endpoint.
type httpMock struct {
	mu        sync.Mutex
	responses []struct {
		Method string `json:"method"`
		Path   string `json:"path"`
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	requests []recordedRequest
	srv      *httptest.Server
}

type recordedRequest struct {
	Method string
	Path   string
	Body   json.RawMessage
}

func newHTTPMock() *httpMock {
	m := &httpMock{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		m.mu.Lock()
		defer m.mu.Unlock()
		m.requests = append(m.requests, recordedRequest{Method: r.Method, Path: r.URL.Path, Body: body})
		for _, resp := range m.responses {
			if strings.EqualFold(resp.Method, r.Method) && resp.Path == r.URL.Path {
				w.WriteHeader(resp.Status)
				_, _ = w.Write([]byte(resp.Body))
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("no mock for " + r.Method + " " + r.URL.Path))
	}))
	return m
}

func (m *httpMock) set(mock *HTTPMock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = nil
	m.responses = nil
	if mock != nil {
		m.responses = mock.Responses
	}
}

func (m *httpMock) reset() { m.mu.Lock(); m.requests = nil; m.mu.Unlock() }

func (m *httpMock) taken() []recordedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedRequest(nil), m.requests...)
}

// Run executes every case of fx against the script at scriptPath.
func Run(ctx context.Context, scriptPath string, fx Fixture, opts Options) ([]Result, error) {
	kind, id, err := KindAndID(scriptPath)
	if err != nil {
		return nil, err
	}
	if kind == registry.KindCollector {
		return nil, fmt.Errorf("script tests currently support transforms, enrichers, automation and integrations; got %s", kind)
	}
	src, err := os.ReadFile(scriptPath)
	if err != nil {
		return nil, err
	}
	// Load the script into an isolated scratch directory so the run cannot be
	// influenced by (or influence) any other script.
	dir, err := os.MkdirTemp("", "scripttest-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	if err := os.MkdirAll(filepath.Join(dir, kind), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, kind, id+".js"), src, 0o644); err != nil {
		return nil, err
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	// Every endpoint name the fixture grants is routed to one local mock server.
	mock := newHTTPMock()
	defer mock.srv.Close()
	sec := secrets.Static{}
	endpoints := map[string]config.Endpoint{}
	grant := func(cfg map[string]any) {
		for _, name := range api.AllowedEndpoints(cfg) {
			if name == "" {
				continue
			}
			sec["endpoints/"+name] = secrets.Secret{"url": mock.srv.URL}
			endpoints[name] = config.Endpoint{URLRef: "endpoints/" + name, Timeout: time.Second}
		}
	}
	grant(fx.Config)
	for _, c := range fx.Cases {
		grant(c.Config)
	}

	cfg := config.Default().Scripting
	cfg.Directories, cfg.Workers, cfg.QuarantineAfter = []string{dir}, 1, 1<<30
	if opts.Timeout > 0 {
		cfg.ExecutionTimeout = opts.Timeout
	}
	deps := api.Deps{Inventory: opts.Inventory, Log: log, Endpoints: endpoints, Secrets: sec}
	svc, rep := scripting.New(cfg, nil, []runtime.Installer{api.Installer(deps)}, log)
	defer svc.Close()
	if rep.Failed > 0 {
		return nil, fmt.Errorf("script does not load: %s", strings.Join(rep.Errors, "; "))
	}
	sc, _ := svc.Registry.Get(kind + "/" + id)
	ext := &scripting.Extensions{Svc: svc, Validator: schema.NewValidator(), Log: log}

	var out []Result
	for _, c := range fx.Cases {
		start := time.Now()
		r := runCase(ctx, ext, sc, fx, c, opts, mock)
		r.Duration = time.Since(start)
		out = append(out, r)
	}
	return out, nil
}

func decodeStrict(raw json.RawMessage, into any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(into)
}

func runCase(ctx context.Context, ext *scripting.Extensions, sc registry.Script, fx Fixture, c Case, opts Options, mock *httpMock) Result {
	res := Result{Name: c.Name}

	cfg := map[string]any{}
	for k, v := range fx.Config {
		cfg[k] = v
	}
	for k, v := range c.Config {
		cfg[k] = v
	}
	sc.Config = cfg
	mock.set(c.HTTP)

	var dev domain.Device
	if c.Device != "" {
		if opts.Inventory == nil {
			res.Message = "case names a device but no inventory was provided"
			return res
		}
		d, ok := opts.Inventory.Get(c.Device)
		if !ok {
			res.Message = fmt.Sprintf("unknown device %q", c.Device)
			return res
		}
		dev = d
	}

	// exec runs the script once and returns its validated result as JSON.
	var exec func() (json.RawMessage, error)
	var input any
	switch sc.Kind {
	case registry.KindTransform, registry.KindEnricher:
		var in domain.Observation
		if err := decodeStrict(c.Input, &in); err != nil {
			res.Message = "input is not an observation: " + err.Error()
			return res
		}
		input = in
		exec = func() (json.RawMessage, error) {
			if sc.Kind == registry.KindTransform {
				next, keep, err := ext.RunTransform(ctx, sc, in)
				if err != nil {
					return nil, err
				}
				if !keep {
					return json.RawMessage("null"), nil
				}
				return json.Marshal(next)
			}
			next, err := ext.RunEnrich(ctx, sc, in, dev)
			if err != nil {
				return nil, err
			}
			return json.Marshal(next)
		}
	case registry.KindAutomation:
		var ev domain.Event
		if err := decodeStrict(c.Input, &ev); err != nil {
			res.Message = "input is not an event: " + err.Error()
			return res
		}
		input = ev
		exec = func() (json.RawMessage, error) {
			prop, err := ext.RunPropose(ctx, sc, "test-policy", fx.AllowedActions, ev, dev)
			if err != nil {
				return nil, err
			}
			if prop == nil {
				return json.RawMessage("null"), nil
			}
			// A proposal the engine would reject is a bug in the script, and the
			// test says so: same validation the engine applies.
			if verr := automation.Validate(*prop, dev); verr != nil {
				return nil, fmt.Errorf("proposal would be rejected by the automation engine: %v", verr)
			}
			if len(fx.AllowedActions) > 0 && !slices.Contains(fx.AllowedActions, prop.Action) {
				return nil, fmt.Errorf("proposed action %q is outside allowed_actions %v", prop.Action, fx.AllowedActions)
			}
			return json.Marshal(prop)
		}
	case registry.KindIntegration:
		var ev domain.Event
		if err := decodeStrict(c.Input, &ev); err != nil {
			res.Message = "input is not an event: " + err.Error()
			return res
		}
		input = ev
		exec = func() (json.RawMessage, error) {
			r, _, err := ext.RunHandle(ctx, sc, ev, dev)
			if err != nil {
				return nil, err
			}
			return json.Marshal(r)
		}
	default:
		res.Message = "unsupported script kind " + sc.Kind
		return res
	}

	mock.reset()
	first, err := exec()
	if err != nil {
		return judgeError(res, c, err)
	}
	requests := mock.taken()
	if c.ExpectError != "" || c.ExpectTimeout {
		res.Message = "expected the script to fail, but it returned a valid result"
		res.Output = string(first)
		return res
	}
	mock.reset()
	second, err2 := exec()
	if err2 != nil || !jsonEqual(first, second) {
		res.Message = "output is not deterministic: two runs on the same input differ"
		res.Output = string(first)
		return res
	}
	res.Output = string(first)

	switch {
	case c.HasExpect && string(bytes.TrimSpace(c.Expect)) == `"unchanged"`:
		inJSON, _ := json.Marshal(input)
		if !jsonEqual(first, inJSON) {
			res.Message = "expected the input to be returned unchanged"
			return res
		}
	case c.HasExpect:
		if !jsonEqual(first, c.Expect) {
			res.Message = fmt.Sprintf("output differs from expectation\n      want: %s\n       got: %s", compact(c.Expect), first)
			return res
		}
	case len(c.ExpectSubset) > 0:
		var want, got any
		_ = json.Unmarshal(c.ExpectSubset, &want)
		_ = json.Unmarshal(first, &got)
		if !subset(want, got) {
			res.Message = fmt.Sprintf("output does not contain the expected fields\n      want ⊆: %s\n         got: %s", compact(c.ExpectSubset), first)
			return res
		}
	}
	if c.HasExpectRequests {
		if msg := checkRequests(c.ExpectRequests, requests); msg != "" {
			res.Message = msg
			return res
		}
	}
	res.Pass = true
	return res
}

func checkRequests(want []ExpectedRequest, got []recordedRequest) string {
	if len(want) != len(got) {
		return fmt.Sprintf("expected %d HTTP request(s), the script made %d", len(want), len(got))
	}
	for i, w := range want {
		g := got[i]
		if !strings.EqualFold(w.Method, g.Method) || w.Path != g.Path {
			return fmt.Sprintf("request %d: want %s %s, got %s %s", i+1, w.Method, w.Path, g.Method, g.Path)
		}
		if len(w.BodySubset) > 0 {
			var wantBody, gotBody any
			_ = json.Unmarshal(w.BodySubset, &wantBody)
			if err := json.Unmarshal(g.Body, &gotBody); err != nil || !subset(wantBody, gotBody) {
				return fmt.Sprintf("request %d body does not contain %s (got %s)", i+1, compact(w.BodySubset), g.Body)
			}
		}
	}
	return ""
}

func judgeError(res Result, c Case, err error) Result {
	var se *runtime.Error
	isTimeout := errors.As(err, &se) && se.Kind == runtime.KindTimeout
	switch {
	case c.ExpectTimeout:
		if isTimeout {
			res.Pass = true
			return res
		}
		res.Message = "expected an execution timeout, got: " + err.Error()
	case c.ExpectError != "":
		if strings.Contains(err.Error(), c.ExpectError) {
			res.Pass = true
			return res
		}
		res.Message = fmt.Sprintf("error %q does not contain %q", err.Error(), c.ExpectError)
	default:
		res.Message = "unexpected error: " + err.Error()
	}
	return res
}

func compact(b json.RawMessage) string {
	var buf bytes.Buffer
	if json.Compact(&buf, b) != nil {
		return string(b)
	}
	return buf.String()
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return equalNum(x, y)
}

// equalNum is reflect.DeepEqual with a tolerance for floating-point numbers.
func equalNum(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		return ok && math.Abs(x-y) <= 1e-9*math.Max(1, math.Abs(x))
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			if w, ok := y[k]; !ok || !equalNum(v, w) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !equalNum(x[i], y[i]) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

// subset reports whether every field of want appears, equal, in got.
func subset(want, got any) bool {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range w {
			gv, ok := g[k]
			if !ok || !subset(v, gv) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(w) != len(g) {
			return false
		}
		for i := range w {
			if !subset(w[i], g[i]) {
				return false
			}
		}
		return true
	}
	return equalNum(want, got)
}
