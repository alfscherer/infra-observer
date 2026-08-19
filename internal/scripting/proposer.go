package scripting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
)

// Propose implements automation.Proposer by calling the policy's script.
//
// The script is handed the event, a read-only view of the device and a small
// context, and returns either null or a proposal. That is all it can do. What
// comes back is only decoded here; the automation engine then validates it
// against the action catalog, the device and the policy's allowed_actions, and
// applies every gate, exactly as for a static policy. A script that proposes
// something forbidden produces an audited denial, not an action.
func (e *Extensions) Propose(ctx context.Context, pol automation.Policy, ev domain.Event, dev domain.Device) (*automation.Proposal, error) {
	key := registry.KindAutomation + "/" + pol.Proposal.Script
	sc, ok := e.Svc.Registry.Get(key)
	if !ok || sc.Status != registry.StatusActive {
		return nil, fmt.Errorf("%w: %s", ErrNotActive, key)
	}
	return e.RunPropose(ctx, sc, pol.ID, pol.Proposal.AllowedActions, ev, dev)
}

// RunPropose invokes one automation script and decodes its proposal.
func (e *Extensions) RunPropose(ctx context.Context, sc registry.Script, policyID string, allowed []string, ev domain.Event, dev domain.Device) (*automation.Proposal, error) {
	call := api.NewCall(info(sc), ev.CorrelationID, ev.EventID, ev.DeviceID)
	raw, err := e.Svc.Call(ctx, sc.Key, "proposeAutomation", call, ev, api.ViewOf(dev),
		map[string]any{"policy_id": policyID, "allowed_actions": allowed})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return nil, nil
	}
	prop, err := schema.Decode[automation.Proposal](raw, 16<<10)
	if err != nil {
		verr := scriptOutputError(sc.ID, "returned something that is not a proposal (want {action, target?, params?, reason?} or null): %v", err)
		e.Svc.Failed(sc.Key, verr)
		return nil, verr
	}
	return &prop, nil
}

// IntegrationResult is what handleEvent returns.
type IntegrationResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// IntegrationOutcome reports one integration's handling of one event.
type IntegrationOutcome struct {
	Script  string
	Result  IntegrationResult
	Emitted []domain.Event
	Err     error // the script failed; other integrations are unaffected
}

// RunHandle invokes one integration script for an event.
func (e *Extensions) RunHandle(ctx context.Context, sc registry.Script, ev domain.Event, dev domain.Device) (IntegrationResult, []domain.Event, error) {
	call := api.NewCall(info(sc), ev.CorrelationID, ev.EventID, ev.DeviceID)
	raw, err := e.Svc.Call(ctx, sc.Key, "handleEvent", call, ev, map[string]any{"device": api.ViewOf(dev)})
	if err != nil {
		return IntegrationResult{}, nil, err
	}
	res, derr := schema.Decode[IntegrationResult](raw, 16<<10)
	if derr != nil || string(json.RawMessage(raw)) == "null" {
		verr := scriptOutputError(sc.ID, "must return {ok: boolean, message?: string}")
		e.Svc.Failed(sc.Key, verr)
		return IntegrationResult{}, nil, verr
	}
	return res, call.Events(), nil
}

// HandleEvent runs every active integration interested in ev. Script failures
// are reported in the outcomes and never stop the others; only platform
// conditions are returned as an error, so the delivery can be retried.
func (e *Extensions) HandleEvent(ctx context.Context, ev domain.Event, dev domain.Device) ([]IntegrationOutcome, error) {
	var out []IntegrationOutcome
	for _, sc := range e.Svc.Registry.ByKind(registry.KindIntegration) {
		if !matches(sc.Events, ev.Type) {
			continue
		}
		if sc.MinSeverity != "" && ev.Severity.Rank() < domain.Severity(sc.MinSeverity).Rank() {
			continue
		}
		res, emitted, err := e.RunHandle(ctx, sc, ev, dev)
		if err != nil {
			if platformError(err) {
				return out, err
			}
			e.log().Warn("integration failed", "component", "scripting", "script_id", sc.ID, "event_id", ev.EventID, "device_id", ev.DeviceID, "error", err)
			out = append(out, IntegrationOutcome{Script: sc.Key, Err: err})
			continue
		}
		out = append(out, IntegrationOutcome{Script: sc.Key, Result: res, Emitted: emitted})
	}
	return out, nil
}

// IntegrationRunner adapts Extensions to the automation worker: it runs the
// integrations for an event and persists any events they emitted.
type IntegrationRunner struct {
	Ext     *Extensions
	Persist func(ctx context.Context, ev domain.Event) error
	Log     *slog.Logger
}

// Run implements automation.Integrations.
func (r IntegrationRunner) Run(ctx context.Context, ev domain.Event, dev domain.Device) error {
	outcomes, err := r.Ext.HandleEvent(ctx, ev, dev)
	if err != nil {
		return err
	}
	log := r.Log
	if log == nil {
		log = slog.Default()
	}
	for _, o := range outcomes {
		if o.Err != nil {
			continue // already logged and counted against the script
		}
		log.Info("integration handled event", "component", "scripting", "script", o.Script, "event_id", ev.EventID,
			"device_id", ev.DeviceID, "correlation_id", ev.CorrelationID, "ok", o.Result.OK, "message", o.Result.Message)
		for _, emitted := range o.Emitted {
			if r.Persist == nil {
				continue
			}
			if err := r.Persist(ctx, emitted); err != nil {
				return err // storage trouble: retry the whole delivery; event ids make it idempotent
			}
		}
	}
	return nil
}

// Invoke runs an integration script on demand as the invoke_extension action.
// The automation engine has already checked the script is on the approved
// list. The script sees a synthetic automation.invoked event.
func (r IntegrationRunner) Invoke(ctx context.Context, script string, dev domain.Device, requestID, correlationID string, params map[string]string) (string, error) {
	sc, ok := r.Ext.Svc.Registry.Get(registry.KindIntegration + "/" + script)
	if !ok || sc.Status != registry.StatusActive {
		return "", fmt.Errorf("%w: integrations/%s", ErrNotActive, script)
	}
	ev := domain.Event{
		EventID: domain.StableID("evt", "invoke", requestID), CorrelationID: correlationID, DeviceID: dev.ID, Type: "automation.invoked",
		Severity: domain.SeverityInfo, Message: "extension invoked by an automation policy", Labels: params,
		OccurredAt: time.Now().UTC(), Source: "automation",
	}
	res, emitted, err := r.Ext.RunHandle(ctx, sc, ev, dev)
	if err != nil {
		return "", err
	}
	for _, e := range emitted {
		if r.Persist != nil {
			if perr := r.Persist(ctx, e); perr != nil {
				return "", perr
			}
		}
	}
	if !res.OK {
		return "", domain.Errorf(domain.CategoryPermanent, "extension %s reported failure: %s", script, res.Message)
	}
	if res.Message == "" {
		return "extension " + script + " completed", nil
	}
	return res.Message, nil
}
