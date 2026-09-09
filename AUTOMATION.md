# Automation

## Why automation is isolated from monitoring

Monitoring answers *what is true*. Automation answers *may we act on it, and how*. Coupling them produces the most dangerous pattern in operations:

> "monitoring detects failure → execute arbitrary script"

That pattern acts on the first bad sample (most incidents heal themselves), has no memory (it will fire again and again), no notion of *who* or *when* (maintenance windows, cooldowns), no audit trail, and turns any injection into remote code execution. This project instead follows:

```
observation
  → state transition
    → event
      → automation proposal
        → policy validation
          → authorized action
            → audited result
```

Each arrow is a place something can be refused, delayed or recorded.

## The flow

1. **Trigger.** Only alert-*opening* events (never a recovery) are considered; a policy matches on event type, device type, site, tags, minimum severity.
2. **Proposal.** Either the policy's static `action` (with `{interface}`-style templates from event labels) or a script's `proposeAutomation` (see below).
3. **Validation.** Same for every proposer: the action is in the **catalog**; target and parameters match strict patterns (no shell metacharacters — a test asserts no catalog parameter accepts `; | & \` $( \n > <`); the device type fits; for scripts, the action is among the policy's `allowed_actions`.
4. **Request.** Recorded with a **deterministic ID** = hash(policy, event) — a redelivered event finds it already there — and `not_before = event time + conditions.duration`. Rejected proposals are recorded too, as `denied` with the reason: operators can see that a script asked for something the policy forbids.
5. **Gates**, evaluated when the request is due, in this order: policy still exists → device exists and enabled → **alert still firing** (else `expired`; do not remediate a problem that healed) → validation again (the policy may have changed) → approved extension (for `invoke_extension`) → `require_tag` → device capability → **allowlist** for live disruptive actions → **maintenance window** → **cooldown** → **retry limit** → **approval**.
6. **Execution** through a device adapter, dry-run or live.
7. **Result** recorded with correlation ID, status, message, details; both request and result are published (`automation.request`, `automation.result`) through the outbox.

## The catalog

A closed list in Go (`internal/automation/actions.go`), each with device types, required capability, target/parameter patterns and a `Disruptive` flag:

- switch/router: `disable_interface`, `enable_interface`, `bounce_interface`, `set_interface_description`, `query_interface_state`, `query_vlan_state`
- access point: `restart_ap_service`, `query_ap_clients`, `update_ap_config`, `query_interface_state`
- server/workstation: `restart_service`, `collect_diagnostics`, `run_maintenance_task` (from a predefined task list)
- generic: `send_webhook`, `create_event_record`, `invoke_extension` (approved integration scripts only), `noop`

There is no action that runs a command line. Adding a capability means adding a catalog entry and an adapter method — reviewed Go code.

## Why JavaScript can propose but cannot bypass policy

The script receives the event, a read-only device view and `{policy_id, allowed_actions}` and returns `null` or a struct. It has no way to execute anything: the host API for the `automation` kind is read-only (no `http.request`, no `events.emit`). What it returns is only decoded; validation and every gate then run in Go exactly as for a static policy. Failure modes are all safe: a script that throws, loops, returns garbage, proposes a forbidden action or injects metacharacters produces an audited `denied` request; a saturated executor (a *platform* condition) is retried, not turned into a denial; a script that keeps misbehaving is quarantined and its policy simply records denials.

## Dry-run defaults

`dry_run` defaults to **true** at three levels and needs two independent switches to change:

- `automation.default_dry_run` (global; default `true`) — if true, everything is dry-run regardless of policy;
- `safety.dry_run` per policy (default `true`);
- live *disruptive* actions additionally require the device on the policy file's `allowlist.devices`.

A dry run is not a no-op: adapters perform a **read-only preflight** (a real SNMP read), so it verifies the target exists and reports current state — *"dry-run: would bounce interface Gi0/2 on switch-01 (currently oper=down admin=up)"*. A port that does not exist fails the dry run.

## Policies

```yaml
- id: restart-stale-agent
  trigger: {event: device.down, device_type: [server, workstation]}
  conditions: {duration: 10m, retries_below: 2}
  action: {type: restart_service, service: observer-agent}
  safety: {dry_run: true, cooldown: 30m, require_tag: automation-enabled}
```

Loaded strictly (unknown keys are errors). Fully static actions are validated at startup so a typo fails then, not at 3 a.m. Defaults: `require_tag: automation-enabled` and `cooldown: 15m` even if omitted; a policy cannot opt out of the tag gate.

- **Cooldown** is per (policy, device, target) and measured from when the action *executed* (`finished_at`), not when the request was created — a request may wait a long time for its duration condition (a bug the test suite found: measuring from creation let a second action slip inside the cooldown of one that had just run).
- **Retries** (`retries_below`) limits executed attempts per policy per alert; automation is not a retry loop, so a *failed* action is surfaced, not repeated.
- **Maintenance windows** (recurring by day/time/timezone, crossing midnight, or absolute) suppress automation for matching devices (by id, site, tag).
- **Approval** (`require_approval`, `approval_timeout`): the request waits in `awaiting_approval` and expires if nobody approves.

### Approval policy

`POST /api/automation/{id}/approve` with `Authorization: Bearer <token>`. Tokens map to identities in a secret (`api.approvers_ref`); the approver recorded is the token's identity, never a field in the body; comparison is constant-time; without configured tokens the endpoint returns 403 `approvals_disabled`. An unauthenticated call changes nothing. Approving twice or approving a request not awaiting approval returns 409 and does not overwrite the first approver. Approvals go through PostgreSQL; the worker picks them up on its next tick, so the API needs no NATS connection.

## Idempotency and concurrency

- Request ID = hash(policy, event) → duplicate events cannot create duplicate requests (`ON CONFLICT DO NOTHING`; the request's outbox row is enqueued only when inserted).
- **Claim**: a single conditional `UPDATE … WHERE status IN (pending, approved) …` moves a request to `executing`; of N racing workers exactly one wins (tested with 12 concurrent claimers on PostgreSQL).
- **Lease**: if an executor dies mid-action, the claim expires (default 5 min) and the request is picked up again. That is at-least-once execution, so **adapters must be idempotent**.
- Completing a request is idempotent (the second completion is a no-op and enqueues nothing).

## Auditability

`automation_requests` + `automation_results` are the audit trail: who/what proposed (`policy` or `script:<id>`), the exact action and parameters, dry-run flag, the gate that denied it, who approved, what the adapter found and did. `correlation_id` links a request to the event and telemetry that caused it. Every decision is a structured log line with `automation_id`, `policy_id`, `device_id`, `correlation_id`. Rejected proposals are audited as well as executed ones.

## Device adapters

```go
type DeviceAdapter interface {
    Capabilities(ctx context.Context, device domain.Device) ([]Capability, error)
    Execute(ctx context.Context, device domain.Device, action Action) (ActionResult, error)
}
```

`internal/automation/adapters`: mock adapters for switch, access point, server and workstation whose mutations go through a `Controller` (in the lab, the simulator's control channel, so a live bounce heals the simulated port and the loop closes), plus real SNMP reads for `query_interface_state`, `query_ap_clients`, `collect_diagnostics` and dry-run preflights. The generic adapter sends webhooks (URL and token stay in the secret store; results never contain them), records events (event ID derived from the request, so a retried action records one event) and invokes approved integration scripts.

## Secrets

Adapters and endpoints reference credentials by name; values are resolved by Go at the moment of use and never placed in results, logs, or script-visible data.

## Rollback considerations

Automation does not roll back. It is designed so it rarely needs to: dry-run first, conditions that wait for persistence, cooldowns and retry limits that bound repetition, an allowlist that bounds blast radius, and results that record `before_*` state so an operator can reverse a change by hand. Actions are chosen to be reversible where possible (`bounce` rather than `disable`; `enable_interface` exists as the inverse). Anything that cannot be undone should require approval.
