package automation

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/schema"
)

// Engine turns alert events into audited automation outcomes.
type Engine struct {
	Policies *Policies
	Store    persistence.AutomationStore
	Devices  inventory.Reader
	Adapters AdapterSet
	Proposer Proposer // optional; needed only for policies with a proposal

	// GlobalDryRun is automation.default_dry_run. Live execution needs this to
	// be false AND the policy to say dry_run: false. Two independent switches
	// must both be turned before anything is mutated.
	GlobalDryRun bool
	Lease        time.Duration // how long a claimed request may run before it is considered abandoned
	BatchSize    int
	ExecTimeout  time.Duration
	Now          func() time.Time
	Log          *slog.Logger

	// OnRequest and OnResult are metrics hooks.
	OnRequest func(policyID string)
	OnResult  func(policyID string, status domain.AutomationStatus, err error)
}

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *Engine) log() *slog.Logger {
	if e.Log != nil {
		return e.Log.With("component", "automation")
	}
	return slog.Default().With("component", "automation")
}

func (e *Engine) lease() time.Duration {
	if e.Lease > 0 {
		return e.Lease
	}
	return 5 * time.Minute
}

func (e *Engine) dryRun(p Policy) bool { return e.GlobalDryRun || p.dryRun() }

func requestID(policyID, eventID string) string { return domain.StableID("auto", policyID, eventID) }

// HandleEvent is stage one: decide which policies want to react to an event
// and record a request for each. It executes nothing. The request ID is
// deterministic, so a redelivered event finds its request already recorded.
// Only alert-opening events trigger automation; a recovery event never does.
func (e *Engine) HandleEvent(ctx context.Context, ev domain.Event) ([]domain.AutomationRequest, error) {
	if ev.Resolves || ev.AlertKey == "" {
		return nil, nil
	}
	dev, ok := e.Devices.Get(ev.DeviceID)
	if !ok {
		e.log().Warn("event for unknown device; no automation", "event_id", ev.EventID, "device_id", ev.DeviceID)
		return nil, nil
	}
	var created []domain.AutomationRequest
	for _, pol := range e.Policies.Match(ev, dev) {
		prop, perr := e.propose(ctx, pol, ev, dev)
		if perr != nil && domain.IsRetryable(perr) {
			// The platform, not the script, failed (for example the script
			// executor is saturated). Fail the delivery so it is retried instead
			// of recording a denial that would swallow this event.
			return created, perr
		}
		if prop == nil && perr == nil {
			continue // the proposer had nothing to suggest
		}
		req := domain.AutomationRequest{
			RequestID: requestID(pol.ID, ev.EventID), CorrelationID: ev.CorrelationID, PolicyID: pol.ID, EventID: ev.EventID,
			AlertKey: ev.AlertKey, DeviceID: dev.ID, Proposer: "policy", DryRun: e.dryRun(pol),
			Status: domain.AutomationPending, NotBefore: ev.OccurredAt.Add(pol.Conditions.Duration), CreatedAt: e.now().UTC(),
		}
		if pol.Proposal != nil {
			req.Proposer = "script:" + pol.Proposal.Script
		}
		var denied string
		switch {
		case perr != nil:
			denied = "proposal failed: " + perr.Error()
		default:
			req.Action, req.Target, req.Params, req.Reason = prop.Action, prop.Target, prop.Params, prop.Reason
			if err := e.checkProposal(pol, *prop, dev); err != nil {
				denied = "proposal rejected: " + err.Error()
			}
		}
		if denied != "" {
			// A rejected proposal is still audited: operators can see that a
			// script asked for something the policy would not allow.
			req.Status, req.NotBefore = domain.AutomationDenied, e.now().UTC()
			if req.Action == "" {
				req.Action = "none"
			}
		}
		msg, err := requestMessage(req)
		if err != nil {
			return created, err
		}
		inserted, err := e.Store.InsertAutomationRequest(ctx, req, msg)
		if err != nil {
			return created, err
		}
		if !inserted {
			continue // already recorded: this event was delivered before
		}
		created = append(created, req)
		if e.OnRequest != nil {
			e.OnRequest(pol.ID)
		}
		e.log().Info("automation request recorded", "automation_id", req.RequestID, "policy_id", pol.ID, "device_id", dev.ID,
			"event_id", ev.EventID, "correlation_id", ev.CorrelationID, "action", req.Action, "target", req.Target, "proposer", req.Proposer, "dry_run", req.DryRun, "status", req.Status)
		if denied != "" {
			if _, err := e.finish(ctx, req, domain.AutomationDenied, denied, map[string]string{"gate": "proposal"}, e.now(), e.now()); err != nil {
				return created, err
			}
		}
	}
	return created, nil
}

func (e *Engine) propose(ctx context.Context, pol Policy, ev domain.Event, dev domain.Device) (*Proposal, error) {
	if pol.Action != nil {
		params := make(map[string]string, len(pol.Action.Params))
		for k, v := range pol.Action.Params {
			params[k] = expand(v, ev)
		}
		return &Proposal{Action: pol.Action.Type, Target: expand(pol.Action.Target, ev), Params: params,
			Reason: fmt.Sprintf("policy %s reacting to %s", pol.ID, ev.Type)}, nil
	}
	if e.Proposer == nil {
		return nil, domain.Errorf(domain.CategoryUnsupported, "policy %s needs a script proposer but none is configured", pol.ID)
	}
	return e.Proposer.Propose(ctx, pol, ev, dev)
}

// checkProposal is the validation every proposal, static or scripted, must
// pass: it is in the catalog, fits the device, and (for scripts) is one of the
// actions this policy allows.
func (e *Engine) checkProposal(pol Policy, pr Proposal, dev domain.Device) error {
	if pol.Proposal != nil && !containsStr(pol.Proposal.AllowedActions, pr.Action) {
		return domain.Errorf(domain.CategoryValidation, "action %q is not among the actions policy %s allows a script to propose", pr.Action, pol.ID)
	}
	return Validate(pr, dev)
}

func requestMessage(req domain.AutomationRequest) (persistence.OutboxMessage, error) {
	payload, err := schema.Encode(req)
	if err != nil {
		return persistence.OutboxMessage{}, err
	}
	return persistence.OutboxMessage{Subject: messaging.SubjectAutomationRequest, MsgID: req.RequestID, Payload: payload,
		Headers: map[string]string{messaging.HeaderSchema: schema.AutomationRequestV1, messaging.HeaderCorrelationID: req.CorrelationID}}, nil
}

func resultMessage(res domain.AutomationResult) (persistence.OutboxMessage, error) {
	payload, err := schema.Encode(res)
	if err != nil {
		return persistence.OutboxMessage{}, err
	}
	return persistence.OutboxMessage{Subject: messaging.SubjectAutomationResult, MsgID: res.RequestID + ".result", Payload: payload,
		Headers: map[string]string{messaging.HeaderSchema: schema.AutomationResultV1, messaging.HeaderCorrelationID: res.CorrelationID}}, nil
}

// finish records a terminal outcome and its notification atomically.
func (e *Engine) finish(ctx context.Context, req domain.AutomationRequest, status domain.AutomationStatus, message string, details map[string]string, started, finished time.Time) (bool, error) {
	res := domain.AutomationResult{
		RequestID: req.RequestID, CorrelationID: req.CorrelationID, Status: status, DryRun: req.DryRun || e.GlobalDryRun,
		Message: message, Details: details, StartedAt: started.UTC(), FinishedAt: finished.UTC(),
	}
	msg, err := resultMessage(res)
	if err != nil {
		return false, err
	}
	done, err := e.Store.CompleteAutomation(ctx, res, msg)
	if err == nil && done {
		if e.OnResult != nil {
			e.OnResult(req.PolicyID, status, nil)
		}
		e.log().Info("automation outcome", "automation_id", req.RequestID, "policy_id", req.PolicyID, "device_id", req.DeviceID,
			"correlation_id", req.CorrelationID, "status", status, "dry_run", res.DryRun, "message", message)
	}
	return done, err
}

// Tick is stage two: work through due requests. It is safe to run from
// several workers at once; the claim is exclusive.
func (e *Engine) Tick(ctx context.Context) (int, error) {
	now := e.now()
	if err := e.expireApprovals(ctx, now); err != nil {
		return 0, err
	}
	batch := e.BatchSize
	if batch <= 0 {
		batch = 50
	}
	due, err := e.Store.DueAutomation(ctx, now, e.lease(), batch)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, req := range due {
		ok, err := e.Store.ClaimAutomation(ctx, req.RequestID, now, e.lease())
		if err != nil {
			return n, err
		}
		if !ok {
			continue // another worker has it
		}
		if err := e.process(ctx, req); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (e *Engine) expireApprovals(ctx context.Context, now time.Time) error {
	waiting, err := e.Store.AwaitingApprovalBefore(ctx, now)
	if err != nil {
		return err
	}
	for _, req := range waiting {
		pol, ok := e.Policies.Get(req.PolicyID)
		timeout := DefaultApprovalTimeout
		if ok && pol.Safety.ApprovalTimeout > 0 {
			timeout = pol.Safety.ApprovalTimeout
		}
		if now.Sub(req.CreatedAt) >= timeout {
			if _, err := e.finish(ctx, req, domain.AutomationExpired, "approval not granted in time", map[string]string{"gate": "approval"}, now, now); err != nil {
				return err
			}
		}
	}
	return nil
}

// Approve records a human approval. Who is allowed to approve is decided by
// the caller (the API layer); the engine records who did.
func (e *Engine) Approve(ctx context.Context, id, by string) error {
	if strings.TrimSpace(by) == "" {
		return domain.Errorf(domain.CategoryValidation, "an approver identity is required")
	}
	ok, err := e.Store.ApproveAutomation(ctx, id, by)
	if err != nil {
		return err
	}
	if !ok {
		return domain.Errorf(domain.CategoryValidation, "request %s is not awaiting approval", id)
	}
	e.log().Info("automation approved", "automation_id", id, "approved_by", by)
	return nil
}

// process applies the gates and, if all pass, executes.
func (e *Engine) process(ctx context.Context, req domain.AutomationRequest) error {
	now := e.now()
	deny := func(gate, msg string) error {
		_, err := e.finish(ctx, req, domain.AutomationDenied, msg, map[string]string{"gate": gate}, now, e.now())
		return err
	}
	pol, ok := e.Policies.Get(req.PolicyID)
	if !ok {
		return deny("policy", "policy no longer exists")
	}
	dev, ok := e.Devices.Get(req.DeviceID)
	if !ok || !dev.Enabled {
		return deny("device", "device is unknown or disabled")
	}

	// The alert that justified acting must still be firing: most incidents heal
	// themselves, and remediating a resolved problem is pure risk.
	if req.AlertKey != "" {
		firing, err := e.Store.AlertFiring(ctx, req.AlertKey)
		if err != nil {
			return e.release(ctx, req, err)
		}
		if !firing {
			_, err := e.finish(ctx, req, domain.AutomationExpired, "the alert resolved before the action was due", map[string]string{"gate": "alert"}, now, e.now())
			return err
		}
	}

	prop := Proposal{Action: req.Action, Target: req.Target, Params: req.Params, Reason: req.Reason}
	if err := Validate(prop, dev); err != nil {
		return deny("validation", err.Error())
	}
	spec := Catalog[req.Action]
	if pol.Proposal != nil && !containsStr(pol.Proposal.AllowedActions, req.Action) {
		return deny("policy", fmt.Sprintf("policy no longer allows %s", req.Action))
	}
	if !dev.HasTag(pol.Safety.RequireTag) {
		return deny("tag", fmt.Sprintf("device lacks the required tag %q", pol.Safety.RequireTag))
	}
	if spec.Capability != "" && !dev.HasCapability(spec.Capability) {
		return deny("capability", fmt.Sprintf("device does not advertise capability %q", spec.Capability))
	}
	dry := req.DryRun || e.GlobalDryRun || pol.dryRun()
	if !dry && spec.Disruptive && !e.Policies.Allowlisted(dev.ID) {
		return deny("allowlist", "live mutation requires the device to be on the automation allowlist")
	}
	if id, active := e.Policies.InMaintenance(now, dev); active {
		return deny("maintenance", fmt.Sprintf("device is in maintenance window %q", id))
	}
	if pol.Safety.Cooldown > 0 {
		prior, err := e.Store.AutomationHistory(ctx, pol.ID, dev.ID, req.Target, now.Add(-pol.Safety.Cooldown))
		if err != nil {
			return e.release(ctx, req, err)
		}
		if len(prior) > 0 {
			return deny("cooldown", fmt.Sprintf("cooldown of %s has not elapsed since request %s", pol.Safety.Cooldown, prior[len(prior)-1].RequestID))
		}
	}
	if pol.Conditions.RetriesBelow > 0 && req.AlertKey != "" {
		n, err := e.Store.AttemptsForAlert(ctx, pol.ID, req.AlertKey)
		if err != nil {
			return e.release(ctx, req, err)
		}
		if n >= pol.Conditions.RetriesBelow {
			return deny("retries", fmt.Sprintf("already attempted %d time(s) for this alert (limit %d)", n, pol.Conditions.RetriesBelow))
		}
	}
	if pol.Safety.RequireApproval && req.ApprovedBy == "" {
		ok, err := e.Store.SetAutomationStatus(ctx, req.RequestID, []domain.AutomationStatus{domain.AutomationExecuting}, domain.AutomationAwaitingApproval)
		if err == nil && ok {
			e.log().Info("automation awaiting approval", "automation_id", req.RequestID, "policy_id", pol.ID, "device_id", dev.ID)
		}
		return err
	}
	return e.execute(ctx, req, dev, dry)
}

// release puts a claimed request back for a later tick after a platform
// failure (database trouble): the request is not lost and not denied.
func (e *Engine) release(ctx context.Context, req domain.AutomationRequest, cause error) error {
	back := domain.AutomationPending
	if req.ApprovedBy != "" {
		back = domain.AutomationApproved
	}
	_, _ = e.Store.SetAutomationStatus(ctx, req.RequestID, []domain.AutomationStatus{domain.AutomationExecuting}, back)
	return cause
}

func (e *Engine) execute(ctx context.Context, req domain.AutomationRequest, dev domain.Device, dry bool) error {
	started := e.now()
	adapter, err := e.Adapters.For(dev, req.Action)
	if err != nil {
		_, ferr := e.finish(ctx, req, domain.AutomationDenied, "no adapter: "+err.Error(), map[string]string{"gate": "adapter"}, started, e.now())
		return ferr
	}
	timeout := e.ExecTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ectx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, xerr := adapter.Execute(ectx, dev, Action{
		Type: req.Action, Target: req.Target, Params: req.Params, DryRun: dry, RequestID: req.RequestID, CorrelationID: req.CorrelationID,
	})
	finished := e.now()
	details := result.Details
	if details == nil {
		details = map[string]string{}
	}
	if xerr != nil {
		details["error_category"] = string(domain.CategoryOf(xerr))
		_, err := e.finish(ctx, req, domain.AutomationFailed, xerr.Error(), details, started, finished)
		return err
	}
	status := domain.AutomationSucceeded
	if dry {
		status = domain.AutomationDryRun
	}
	details["changed"] = fmt.Sprint(result.Changed)
	_, err = e.finish(ctx, req, status, result.Message, details, started, finished)
	return err
}
