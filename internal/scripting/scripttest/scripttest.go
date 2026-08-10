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
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

// Fixture is a set of test cases for one script.
type Fixture struct {
	Description string         `json:"description"`
	Config      map[string]any `json:"config"` // script config for every case, overridable per case
	Cases       []Case         `json:"cases"`
}

// Case is one input and its expectation.
type Case struct {
	Name          string
	Device        string
	Config        map[string]any
	Input         json.RawMessage
	Context       json.RawMessage
	Expect        json.RawMessage
	HasExpect     bool
	ExpectSubset  json.RawMessage
	ExpectError   string
	ExpectTimeout bool
}

// UnmarshalJSON records whether "expect" was present, because `"expect": null`
// (the script drops the observation) differs from no expectation at all.
func (c *Case) UnmarshalJSON(b []byte) error {
	var raw struct {
		Name          string          `json:"name"`
		Device        string          `json:"device"`
		Config        map[string]any  `json:"config"`
		Input         json.RawMessage `json:"input"`
		Context       json.RawMessage `json:"context"`
		ExpectSubset  json.RawMessage `json:"expect_subset"`
		ExpectError   string          `json:"expect_error"`
		ExpectTimeout bool            `json:"expect_timeout"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var presence map[string]json.RawMessage
	_ = json.Unmarshal(b, &presence)
	c.Name, c.Device, c.Config, c.Input, c.Context = raw.Name, raw.Device, raw.Config, raw.Input, raw.Context
	c.ExpectSubset, c.ExpectError, c.ExpectTimeout = raw.ExpectSubset, raw.ExpectError, raw.ExpectTimeout
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

// Run executes every case of fx against the script at scriptPath.
func Run(ctx context.Context, scriptPath string, fx Fixture, opts Options) ([]Result, error) {
	kind, id, err := KindAndID(scriptPath)
	if err != nil {
		return nil, err
	}
	if kind != registry.KindTransform && kind != registry.KindEnricher {
		return nil, fmt.Errorf("script tests currently support transforms and enrichers; got %s", kind)
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
	cfg := config.Default().Scripting
	cfg.Directories, cfg.Workers, cfg.QuarantineAfter = []string{dir}, 1, 1<<30
	if opts.Timeout > 0 {
		cfg.ExecutionTimeout = opts.Timeout
	}
	deps := api.Deps{Inventory: opts.Inventory, Log: log}
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
		r := runCase(ctx, ext, sc, fx, c, opts)
		r.Duration = time.Since(start)
		out = append(out, r)
	}
	return out, nil
}

func runCase(ctx context.Context, ext *scripting.Extensions, sc registry.Script, fx Fixture, c Case, opts Options) Result {
	res := Result{Name: c.Name}

	cfg := map[string]any{}
	for k, v := range fx.Config {
		cfg[k] = v
	}
	for k, v := range c.Config {
		cfg[k] = v
	}
	sc.Config = cfg

	var in domain.Observation
	dec := json.NewDecoder(bytes.NewReader(c.Input))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		res.Message = "input is not an observation: " + err.Error()
		return res
	}
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

	exec := func() (json.RawMessage, error) {
		switch sc.Kind {
		case registry.KindTransform:
			next, keep, err := ext.RunTransform(ctx, sc, in)
			if err != nil {
				return nil, err
			}
			if !keep {
				return json.RawMessage("null"), nil
			}
			return json.Marshal(next)
		default:
			next, err := ext.RunEnrich(ctx, sc, in, dev)
			if err != nil {
				return nil, err
			}
			return json.Marshal(next)
		}
	}
	first, err := exec()
	if err != nil {
		return judgeError(res, c, err)
	}
	if c.ExpectError != "" || c.ExpectTimeout {
		res.Message = "expected the script to fail, but it returned a valid result"
		res.Output = string(first)
		return res
	}
	second, err2 := exec()
	if err2 != nil || !jsonEqual(first, second) {
		res.Message = "output is not deterministic: two runs on the same input differ"
		res.Output = string(first)
		return res
	}
	res.Output = string(first)

	switch {
	case c.HasExpect && string(bytes.TrimSpace(c.Expect)) == `"unchanged"`:
		inJSON, _ := json.Marshal(in)
		if !jsonEqual(first, inJSON) {
			res.Message = "expected the observation to be returned unchanged"
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
	res.Pass = true
	return res
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
