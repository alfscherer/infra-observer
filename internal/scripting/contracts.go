package scripting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

// Extensions applies transform and enrich scripts inside the pipeline. It is
// the only place script output re-enters the platform, and it does not trust
// it: results are decoded strictly, validated like any other message, and
// checked against what the extension point is allowed to change.
type Extensions struct {
	Svc       *Service
	Validator schema.Validator
	Log       *slog.Logger
}

func (e *Extensions) log() *slog.Logger {
	if e.Log != nil {
		return e.Log
	}
	return slog.Default()
}

// matches reports whether metric is covered by patterns: exact names, or
// prefixes ending in "*". An empty list matches everything.
func matches(patterns []string, metric string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if p == metric || (strings.HasSuffix(p, "*") && strings.HasPrefix(metric, strings.TrimSuffix(p, "*"))) {
			return true
		}
	}
	return false
}

func info(sc registry.Script) api.ScriptInfo {
	return api.ScriptInfo{Key: sc.Key, Kind: sc.Kind, ID: sc.ID, Version: sc.Version, Config: sc.Config}
}

// platformError reports whether err is a failure of the platform rather than
// of a script. Those must propagate (the message is retried); script failures
// are isolated.
func platformError(err error) bool {
	return err != nil && !errors.Is(err, ErrNotActive) && domain.CategoryOf(err) != domain.CategoryScript
}

// Transform runs the active transform scripts that handle obs.Metric, in
// script-id order. It returns keep=false when a script drops the observation
// (returns null). A failing script is skipped: the observation continues
// unchanged from before that script.
func (e *Extensions) Transform(ctx context.Context, obs domain.Observation) (domain.Observation, bool, error) {
	for _, sc := range e.Svc.Registry.ByKind(registry.KindTransform) {
		if !matches(sc.Metrics, obs.Metric) {
			continue
		}
		next, keep, err := e.RunTransform(ctx, sc, obs)
		if err != nil {
			if platformError(err) {
				return obs, true, err
			}
			e.logFailure(sc, obs, err)
			continue
		}
		if !keep {
			return obs, false, nil
		}
		obs = next
	}
	return obs, true, nil
}

// RunTransform invokes one transform script and validates its result. Unlike
// Transform it reports the script's failure to the caller instead of skipping
// it; the script test runner uses it so tests see exactly what production
// validates.
func (e *Extensions) RunTransform(ctx context.Context, sc registry.Script, obs domain.Observation) (domain.Observation, bool, error) {
	call := api.NewCall(info(sc), obs.CorrelationID, obs.ObservationID, obs.DeviceID)
	raw, err := e.Svc.Call(ctx, sc.Key, "transform", call, obs)
	if err != nil {
		return obs, true, err
	}
	next, keep, verr := e.decodeTransform(obs, raw)
	if verr != nil {
		e.Svc.Failed(sc.Key, verr)
		return obs, true, verr
	}
	return next, keep, nil
}

func (e *Extensions) logFailure(sc registry.Script, obs domain.Observation, err error) {
	e.log().Warn("script failed; observation continues unchanged",
		"component", "scripting", "script_id", sc.ID, "script_version", sc.Version,
		"observation_id", obs.ObservationID, "device_id", obs.DeviceID, "error", err)
}

// scriptOutputError marks a result that ran fine but violated the contract.
func scriptOutputError(id, format string, args ...any) error {
	return &runtime.Error{ScriptID: id, Kind: runtime.KindOutput, Message: fmt.Sprintf(format, args...)}
}

// Fields a transform must not change: they carry idempotency, tracing and
// routing. If a script could rewrite device_id it could file one device's data
// under another; if it could rewrite observation_id, redelivery would stop
// being recognisable.
var transformImmutable = []string{"observation_id", "correlation_id", "device_id", "source", "observed_at", "received_at"}

func (e *Extensions) decodeTransform(before domain.Observation, raw json.RawMessage) (domain.Observation, bool, error) {
	if string(raw) == "null" {
		return before, false, nil
	}
	after, err := schema.Decode[domain.Observation](raw, e.Validator.Limits.MaxMessageBytes)
	if err != nil {
		return before, false, scriptOutputError("transform", "returned a value that is not an observation: %v", err)
	}
	if err := e.Validator.Validate(after); err != nil {
		return before, false, scriptOutputError("transform", "returned an invalid observation: %v", err)
	}
	if field := changed(before, after, transformImmutable); field != "" {
		return before, false, scriptOutputError("transform", "changed %s, which transforms may not modify", field)
	}
	return after, true, nil
}

// Enrich runs the active enricher scripts for obs. Enrichers may only add or
// change metadata.
func (e *Extensions) Enrich(ctx context.Context, obs domain.Observation, dev domain.Device) (domain.Observation, error) {
	for _, sc := range e.Svc.Registry.ByKind(registry.KindEnricher) {
		if !matches(sc.Metrics, obs.Metric) {
			continue
		}
		next, err := e.RunEnrich(ctx, sc, obs, dev)
		if err != nil {
			if platformError(err) {
				return obs, err
			}
			e.logFailure(sc, obs, err)
			continue
		}
		obs = next
	}
	return obs, nil
}

// RunEnrich invokes one enricher and validates its result; see RunTransform.
func (e *Extensions) RunEnrich(ctx context.Context, sc registry.Script, obs domain.Observation, dev domain.Device) (domain.Observation, error) {
	call := api.NewCall(info(sc), obs.CorrelationID, obs.ObservationID, obs.DeviceID)
	raw, err := e.Svc.Call(ctx, sc.Key, "enrich", call, obs, map[string]any{"device": api.ViewOf(dev)})
	if err != nil {
		return obs, err
	}
	next, verr := e.decodeEnrich(obs, raw)
	if verr != nil {
		e.Svc.Failed(sc.Key, verr)
		return obs, verr
	}
	return next, nil
}

func (e *Extensions) decodeEnrich(before domain.Observation, raw json.RawMessage) (domain.Observation, error) {
	if string(raw) == "null" {
		return before, scriptOutputError("enrich", "returned null; enrichers must return the observation")
	}
	after, err := schema.Decode[domain.Observation](raw, e.Validator.Limits.MaxMessageBytes)
	if err != nil {
		return before, scriptOutputError("enrich", "returned a value that is not an observation: %v", err)
	}
	if err := e.Validator.Validate(after); err != nil {
		return before, scriptOutputError("enrich", "returned an invalid observation: %v", err)
	}
	if field := changedExcept(before, after, "metadata"); field != "" {
		return before, scriptOutputError("enrich", "changed %s; enrichers may only change metadata", field)
	}
	return after, nil
}

// asMap renders an observation as a generic JSON map, so comparisons do not
// depend on Go's typed values (an int64 and a float64 of the same number are equal).
func asMap(o domain.Observation) map[string]any {
	b, _ := json.Marshal(o)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

// changed returns the first of fields whose value differs between before and after.
func changed(before, after domain.Observation, fields []string) string {
	b, a := asMap(before), asMap(after)
	for _, f := range fields {
		if !reflect.DeepEqual(b[f], a[f]) {
			return f
		}
	}
	return ""
}

// changedExcept returns the first field other than allowed that differs.
func changedExcept(before, after domain.Observation, allowed ...string) string {
	b, a := asMap(before), asMap(after)
	skip := map[string]bool{}
	for _, f := range allowed {
		skip[f] = true
	}
	keys := map[string]bool{}
	for k := range b {
		keys[k] = true
	}
	for k := range a {
		keys[k] = true
	}
	for k := range keys {
		if !skip[k] && !reflect.DeepEqual(b[k], a[k]) {
			return k
		}
	}
	return ""
}
