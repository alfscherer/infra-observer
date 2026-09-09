# Scripting

Embedded JavaScript is an **extension language**. It exists to make behaviour configurable and extensible without rebuilding the core binaries. It is not an unrestricted scripting environment, and the core process is not a script-execution server.

## Why JavaScript, why embedded, why not Node.js

- **Familiar.** Operators and network/systems engineers already write it; the ES2015+ subset goja implements (arrow functions, spread, destructuring, `??`, template strings) covers everything scripts here need.
- **Embedded (goja, pure Go).** One static binary, no cgo, no Node.js to install, patch or supervise, and — crucially — no ambient authority. A goja interpreter starts with *nothing*: no `require`, `process`, `fetch`, timers, filesystem or network. Every capability is something we chose to add (see the host API).
- **Not Node.js.** A Node subprocess or sidecar would mean a second runtime, a second deployment, IPC, and a process with the operating system's full authority that we would then have to restrain. Embedding inverts that: the sandbox is the default and capabilities are opt-in. (Comparison with Lua, Starlark, external executables and hardcoded Go: DECISIONS.md ADR 7.)

## Extension points and contracts

Scripts live in `scripts/<kind>/<id>.js`; the directory fixes the contract.

| Kind | Function | Returns | Runs in |
|---|---|---|---|
| `transforms` | `transform(observation)` | observation \| `null` (drop) | processor stage A |
| `enrichers` | `enrich(observation, context)` | observation (metadata changes only) | processor stage B |
| `automation` | `proposeAutomation(event, device, context)` | `{action, target?, params?, reason?}` \| `null` | automation worker |
| `integrations` | `handleEvent(event, context)` | `{ok, message?}` | automation worker |
| `collectors` | `collect(device, context)` | observations | contract + host API defined; scheduler integration not yet |

Every script also exports `meta`:

```js
export const meta = {
    version: "1.0.0",                 // required
    description: "…",
    metrics: ["vendor.cpu.load"],     // transforms: REQUIRED; enrichers: optional. Names or prefix*
    events: ["*"], min_severity: "warning",   // integrations: events REQUIRED
};
```

`metrics`/`events` are required so Go filters *before* any JavaScript runs; "call JavaScript for every observation" can never be the default (see benchmarks: a JS call is ~130× a native transform).

**Module format.** `export function name()` and `export const name` at the start of a line. This is a deliberate small rewrite, not an ES module loader: each script is wrapped in a private function scope (so scripts cannot clash or leak globals) and its exports are returned. `import`, `export default` and export lists are rejected with a clear error. Scripts cannot depend on each other. No npm, no package installation.

## Return values are not trusted

A script that loads and runs is not thereby correct. Everything returned is decoded strictly (unknown fields rejected), re-validated with the same validator as any message, and checked against what the extension point may change:

- transforms cannot change `observation_id`, `correlation_id`, `device_id`, `source`, `observed_at`, `received_at` (a script that could rewrite `device_id` could file one device's data under another; rewriting `observation_id` would break duplicate detection and tracing);
- enrichers can change only `metadata`, and metadata values must be strings;
- proposals are decoded into a typed struct and then validated by the automation engine (see AUTOMATION.md);
- integration results must be `{ok: boolean, message?: string}`.

A violation is a script failure (kind `output`), counted toward quarantine, and the observation continues **unchanged**.

## Host API

Deliberately small. Plain JSON-shaped values in and out; nothing is a wrapper around Go memory (values cross as JSON parsed *inside* the interpreter, so mutating an argument cannot affect Go state; host objects are frozen so scripts cannot replace `log`).

| Function | Purpose | Notes |
|---|---|---|
| `log.debug/info/warn/error(msg, fields?)` | structured logging | script fields are nested under `fields`, so a script cannot forge `component`, `device_id`, `script_id`; ≤ 20 lines per call |
| `device.get(id)` | device view or `null` | no `credentials_ref` |
| `inventory.lookup({site, device_type, tag, enabled})` | ≤ 200 devices | |
| `state.get(deviceID, metric, labels?)` | latest stored point or `null` | read-only |
| `config.get(key)` | the script's *own* configuration | cannot read other scripts' config |
| `events.emit({type, severity, message, device_id?, labels?})` | request an event | integrations only; validated; ≤ 10; deferred (Go persists idempotently) |
| `metrics.emit({metric, value, labels?, device_id?})` | request an observation | integrations/collectors; goes through the observation validator; ≤ 100 |
| `http.request({endpoint, method?, path, headers?, body?})` | outbound HTTP | integrations/collectors only; see below |

Not exposed: `os.exec`, shell, raw filesystem, raw database, raw NATS, Go reflection, timers, `require`.

**Permissions are by kind.** Transforms and enrichers run once per observation and are pure: calling `http.request` or `events.emit` from them throws. Automation proposers are read-only.

**Per-invocation budgets**: ≤ 3 HTTP requests, ≤ 10 events, ≤ 100 observations, ≤ 20 log lines. The host API is unavailable while a script's top-level code loads, so loading has no side effects.

### Outbound HTTP is policy-controlled

- Scripts never supply a URL. They name an **endpoint** defined in `scripting.endpoints`, and the endpoint must be granted to *that script* in its configuration (`endpoint_ref: alerts-webhook` or `endpoint_refs: [...]`). Authorisation lives beside the script, not in its source, and "not granted" and "does not exist" give the same error.
- The endpoint's URL and token live in the secret store (`url_ref`). Go resolves them, validates the request, and attaches `Authorization` *after* validation. The script never sees either, and error messages never contain the URL.
- Only `GET/POST/PUT`; path must be absolute (`/v1/alerts`), no scheme, no `//`; headers limited to `Content-Type`, `Accept`, `X-Request-Id` (`Authorization`, `Host` and the rest are refused); redirects are not followed; body ≤ 64 KiB; response ≤ 256 KiB (truncated flag); timeout = min(endpoint timeout, invocation deadline).
- Go adds `Idempotency-Key`, stable per (script, event) — delivery is at-least-once, so the receiver can de-duplicate.

## Script lifecycle

1. **Discovery** in `scripting.directories`, by kind directory.
2. **Validation** without executing: syntax, contract function present, `meta` present.
3. **Load** in a scratch interpreter under the deadline (top-level code runs), `meta` read; `meta.version` (and `metrics`/`events`) enforced.
4. **Configuration** applied: `scripts.<kind>.<id>.enabled|config` in `config.yaml`. Disabled scripts are not even compiled.
5. **Execution** on the executor pool.
6. **Hot reload**: the processor polls script files every 5 s; changed files are revalidated and swapped in. A broken edit marks that script `failed` and leaves the process and other scripts running; fixing the file brings it back.
7. **Quarantine**: `quarantine_after` (default 5) *consecutive* failures — exceptions, timeouts, panics, contract violations — disables the script. Platform conditions (executor saturation, caller cancellation) never count against a script. A reload of unchanged source does not lift quarantine; editing the file (new hash) or `SetEnabled` does.

`infra-observer script list|validate` shows status; `make validate-scripts` is a deploy gate.

## Execution limits and concurrency model

- **Deadline**: `execution_timeout` (default 250 ms, refuse > 10 s). Implemented with interpreter interrupt, so an infinite loop is stopped, and caller cancellation also interrupts. After a timeout or panic the worker's interpreter is **rebuilt** rather than trusted.
- **Concurrency: one interpreter per worker goroutine** (`scripting.workers`), fed by a bounded queue. A goja runtime is not goroutine-safe, and one runtime behind a mutex would serialise every script call in the process. Runtime-per-worker gives parallelism with no locking around execution; the costs are memory per worker and the rule that **scripts must not rely on module-level state** (each worker has its own copy). Compiled programs are immutable and shared.
- **Saturation** is visible: when the queue is full for longer than a bounded wait, `ErrSaturated` (a retryable *platform* error) propagates so the message is retried with backoff.
- **Recursion** is capped (call-stack limit). **Output** is capped at 1 MiB.
- **Memory**: goja cannot bound heap allocation inside a script. The mitigations are the deadline (a runaway allocator is stopped within `execution_timeout`), input/output size caps, interpreter rebuild after a timeout, and a process-level `GOMEMLIMIT`/cgroup limit (see the systemd unit). This is a real limitation and stated as such.
- **Panics** in host functions or the interpreter are recovered at the invocation boundary and reported as script failures.

## Secrets

Never in scripts. Scripts receive named references (`endpoint_ref: alerts-webhook`), never values. Credentials live in `secrets.Resolver` (env/file in the lab; Vault/KMS/systemd credentials in production). `device.get` omits `credentials_ref`.

## Observability

`javascript_executions_total{script}`, `javascript_failures_total{script,kind}`, `javascript_timeouts_total{script}`, `javascript_execution_duration_seconds{script}`, `javascript_quarantines_total`, `javascript_scripts_now{status}`, `javascript_executor_queue_depth`, `javascript_saturations_total`. Logs carry `script_id`, `script_version`, `script_kind`. `/health/dependencies` reports scripting as *degraded* (not unready) when scripts are quarantined or failed.

## Testing scripts

```bash
make test-scripts
infra-observer script test scripts/transforms/normalize-cpu.js \
    --fixture testdata/scripts/transforms/normalize-cpu.json
```

A fixture is a list of cases (`testdata/scripts/<kind>/<id>.json`): `input`, `device`, `config`, and one of `expect` (exact), `expect_subset`, `null` (drop), `"unchanged"`, `expect_error`, `expect_timeout`; integrations add an `http` mock and `expect_requests`. The runner uses the same interpreters, host API and contract validation as production and **runs every case twice** to catch non-determinism. Automation proposals are additionally checked against the action catalog and the policy's allowed actions. `make check` fails if a shipped script has no fixture.

## Complete examples

All four are shipped and pass their fixtures.

### 1. Normalization — `scripts/transforms/normalize-temperature.js`

```js
export const meta = {
    version: "1.1.0",
    description: "vendor.temperature.decic (tenths of C) -> environment.temperature.celsius",
    metrics: ["vendor.temperature.decic"],
};

export function transform(observation) {
    if (typeof observation.value !== "number") return null;
    const celsius = observation.value / 10.0;
    const min = config.get("min_celsius") ?? -40;
    const max = config.get("max_celsius") ?? 125;
    if (celsius < min || celsius > max) {
        log.warn("temperature outside plausible range; treating as a sensor fault", {celsius: String(celsius)});
        return null;                       // a sensor fault is not an overheating event
    }
    return {...observation, metric: "environment.temperature.celsius", value: celsius,
            metadata: {...observation.metadata, raw_metric: observation.metric, unit: "celsius"}};
}
```

The AP reports temperature in tenths of a degree under a vendor metric no Go mapping knows. This script is why the lab's AP `overheat` scenario raises `temperature.high` (the end-to-end test asserts it).

### 2. Enrichment — `scripts/enrichers/classify-interface.js`

```js
export const meta = {version: "1.0.0", description: "…", metrics: ["network.interface.*"]};
const NAME_ROLES = [[/^(lo|loopback)/i, "loopback"], [/^(vl|vlan|irb|bvi)/i, "virtual"],
                    [/^wlan/i, "wireless"], [/^(gi|te|fa|eth|xe|et)/i, "ethernet"]];

export function enrich(observation, context) {
    const name = observation.labels && observation.labels.interface;
    if (!name) return observation;
    let role = "unknown";
    for (const [pattern, r] of NAME_ROLES) { if (pattern.test(name)) { role = r; break; } }
    if (role === "wireless" && context.device.device_type === "access_point") role = "radio";
    const description = (observation.metadata && observation.metadata.interface_description) || "";
    if (/uplink/i.test(description)) role = "uplink";       // an operator's description beats a naming convention
    return {...observation, metadata: {...observation.metadata, interface_role: role}};
}
```

It runs *after* the built-in enrichment, so it can use `interface_description` that Go attached from inventory.

### 3. Event integration — `scripts/integrations/webhook.js`

```js
export const meta = {version: "1.0.0", description: "…", events: ["*"], min_severity: "warning"};

export function handleEvent(event, context) {
    const endpoint = config.get("endpoint_ref");
    if (!endpoint) return {ok: false, message: "no endpoint_ref configured"};
    const response = http.request({endpoint, method: "POST", path: "/v1/alerts", body: {
        summary: event.message, severity: event.severity, type: event.type, resolved: Boolean(event.resolves),
        device: event.device_id, hostname: context.device ? context.device.hostname : event.device_id,
        site: context.device ? context.device.site : "", occurred_at: event.occurred_at, correlation_id: event.correlation_id}});
    if (!response.ok) {
        log.warn("webhook rejected the event", {status: String(response.status)});
        return {ok: false, message: "webhook returned " + response.status};
    }
    return {ok: true};
}
```

Configuration (`config.yaml`) grants the endpoint; the URL/token are in the secret `endpoints/alerts-webhook`:

```yaml
scripts:
  integrations:
    webhook:
      enabled: true
      config: {endpoint_ref: alerts-webhook}
scripting:
  endpoints:
    alerts-webhook: {url_ref: endpoints/alerts-webhook, timeout: 2s}
```

### 4. Automation proposal — `scripts/automation/interface-remediation.js`

```js
export const meta = {version: "1.0.0", description: "Proposes bouncing a down lab interface, except uplinks"};

export function proposeAutomation(event, device, context) {
    const iface = event.labels && event.labels.interface;
    if (!iface) return null;
    const description = (device.attributes && device.attributes["interface." + iface + ".description"]) || "";
    if (/uplink/i.test(description)) { log.info("not proposing remediation for an uplink", {interface: iface}); return null; }
    if (device.site !== "lab" || !device.tags.includes("automation-enabled")) return null;
    const admin = state.get(device.id, "network.interface.admin_enabled", {interface: iface, if_index: event.labels.if_index});
    if (admin && admin.value === false) return null;      // someone disabled it on purpose; do not fight them
    return {action: "bounce_interface", target: iface,
            reason: "Interface " + iface + " on " + device.hostname + " remained down beyond the policy threshold"};
}
```

The script contributes *judgment* that is awkward as static configuration. Go still enforces the action catalog, the policy's `allowed_actions`, parameter validation, tag/capability/allowlist/maintenance/cooldown/retry gates, dry-run and audit — see [AUTOMATION.md](AUTOMATION.md). A proposal such as `{action: "bounce_interface", target: "Gi0/2; reboot"}` ends as an audited denial (`TestHostileProposalsEndAsAuditedDenialsAndNeverReachAnAdapter`).

## Security boundaries (summary)

| Boundary | Enforced by |
|---|---|
| No I/O, timers, modules, process access | interpreter starts empty; host API is opt-in |
| No Go memory shared | JSON across the boundary; frozen host objects |
| No arbitrary network | named, granted endpoints; secrets stay in Go; headers/redirects/sizes limited |
| No identity/routing tampering | immutable-field checks on script output |
| No unbounded resource use | deadline + interrupt, budgets, bounded queue, output cap, interpreter rebuild |
| No crash propagation | panic recovery, failure isolation, quarantine |
| No action execution | proposals only; Go policy engine decides (AUTOMATION.md) |

What this is **not**: a defence against a malicious operator with write access to the scripts directory who can also edit configuration. Scripts are operator-authored extensions running inside a constrained host API; treat write access to `scripts/` as code-deploy access.
