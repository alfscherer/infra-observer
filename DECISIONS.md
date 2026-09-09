# Architecture decisions

Lightweight ADRs. Each has Context, Decision, Alternatives, Consequences.

## ADR 1 — Go 1.26

**Context.** A long-running, concurrent, network-heavy system that must ship as a single artefact and be operated by people who did not write it. It embeds an interpreter, speaks SNMP and NATS, and needs bounded concurrency and cheap goroutines.

**Decision.** Go 1.26, standard library first (`net/http` with method patterns, `log/slog`, `context`, `embed`, generics where they remove duplication), a small set of mature dependencies (nats.go, pgx, gosnmp, goja, yaml.v3, prometheus client).

**Alternatives.** Rust (better memory guarantees, slower to build against this ecosystem, harder to hire for in an ops team); Java/Kotlin (heavier runtime and deployment); Python (fast to write, weak for CPU-bound pipelines and static deployment); Erlang/Elixir (excellent fit for message-passing, smaller pool of operators and libraries for SNMP/JS).

**Consequences.** Static `CGO_ENABLED=0` binaries (one file to deploy, distroless-friendly), explicit error handling that maps well onto our error taxonomy, a race detector used in every test run. Cost: generics-light domain modelling and verbosity.

## ADR 2 — NATS

**Context.** Collection and processing fail and scale differently. A slow database must delay events, not lose telemetry; consumers must restart mid-message; the backlog must be visible.

**Decision.** NATS as the internal message bus, used directly (subjects, streams, acks) rather than behind a generic event-bus abstraction. Subjects are part of the contract and documented.

**Alternatives.** Kafka (heavier to run for this scale; partition semantics not needed); RabbitMQ (fine, but streams/replay are weaker than JetStream's); Redis Streams (adequate, but consumer-group and retention semantics are less explicit); in-process channels only (no durability, no independent scaling); PostgreSQL as a queue (attractive simplicity, but couples ingest rate to the transactional store we most want to protect).

**Consequences.** One small server with built-in persistence; simple subject-based routing; at-least-once delivery that we design around instead of hiding. Cost: NATS-specific code in the messaging layer (deliberately not abstracted), and one more thing to operate.

## ADR 3 — JetStream, used selectively

**Context.** Durability and replay are valuable for telemetry and audit; they are useless for an operator's transient command.

**Decision.** JetStream for raw and normalized telemetry (replay/rebuild points), events, automation records, inventory facts and dead letters — each with time-bound retention and byte caps (`DiscardOld`). Core NATS for `sim.control`.

**Alternatives.** JetStream for everything (durable scenario commands that replay after restart are actively wrong); core NATS for everything (loses the backlog on a restart).

**Consequences.** Bounded storage, a defined replay window (2 days for telemetry), a visible backlog metric. JetStream de-duplicates per *stream* on `Nats-Msg-Id`, which forced distinct message IDs for `events.device` and `events.alert` (same stream). PostgreSQL, not JetStream, is the system of record.

## ADR 4 — Collectors only emit observations

**Context.** The easiest monitoring code puts a threshold next to the poll: `if cpu > 90 { alert() }`. That welds collection to business rules, makes it untestable without a device, and makes every protocol reimplement alerting.

**Decision.** Collectors produce observations and publish them; they never evaluate, alert, act, or mutate health. Poll failures are observations too (`collector.poll_success=false` with an error category).

**Alternatives.** Collector-side thresholds (fewer moving parts, no reusability); collectors writing state directly (simplest possible flow, no replay).

**Consequences.** New collection mechanisms (SNMP, scripts, agents) plug in without touching state or rules; the same observation can be replayed against new rules; collector tests need no rule fixtures. Cost: an extra hop for a simple deployment.

## ADR 5 — State evaluation is separate

**Context.** A single failed sample rarely means a device is down. Alerting per sample pages on packet loss, storms on flapping and cannot express "still failing after ten minutes".

**Decision.** A dedicated state engine: pure `(previous, sample) → (next, transitions, events)`, four-state machine, consecutive-sample thresholds, optional time debounce, hysteresis, stale-data detection through deterministic synthetic samples, duplicate/out-of-order protection. Windowed conditions live in a rules layer over stored samples.

**Alternatives.** Threshold alerts with "for" clauses in the alerting layer (Prometheus-style; would move state into the alert manager and lose per-entity history); sliding-window counters in memory (lost on restart, non-deterministic on replay).

**Consequences.** Deterministic, replayable, exhaustively unit-tested logic; events only on meaningful transitions. Cost: state must be stored and locked per key (advisory locks), and definitions need thought (`fail_after`).

## ADR 6 — Automation is isolated

**Context.** "Monitoring detects failure → run a script" is how monitoring systems cause outages: acts on the first sample, repeats forever, ignores maintenance, leaves no audit trail, and is remote code execution waiting for an injection.

**Decision.** A separate subsystem with its own process role, storage and lifecycle: closed action catalog, typed parameters, deterministic idempotent requests, ordered gates (tag, capability, allowlist, maintenance, cooldown, retries, approval), dry-run by default with two independent switches, an audit trail with correlation IDs.

**Alternatives.** Alert manager webhooks that call runbooks (unbounded blast radius, no shared audit); Ansible/Rundeck triggered directly (good executors, but the *decision* to act still needs this policy layer); scripts with shell access (rejected outright).

**Consequences.** Every automatic action is reviewable, bounded and reproducible; adding a capability means reviewed Go. Cost: more moving parts than "run this command", and a real limit on what can be automated.

## ADR 7 — Embedded JavaScript for runtime extensibility

**Context.** Operators need to add vendor quirks, site-specific enrichment, integrations and remediation judgment without rebuilding a compiled system.

**Decision.** goja (pure-Go ECMAScript) embedded in the Go processes, scripts on disk in kind directories, configured separately from source, contract-typed per extension point, hot-reloaded.

| Option | Familiarity | Expressiveness | Isolation by default | Deployment | Verdict |
|---|---|---|---|---|---|
| Hardcoded Go extensions | high for devs | full | n/a | rebuild + redeploy per change | defeats the purpose; keep for hot, stable logic |
| External executables | any language | full | none (full OS authority) | per-call process cost, arg/env plumbing | the opposite of a constrained boundary |
| Lua (gopher-lua) | low among ops | good | good | embedded | fine engine; smaller talent pool, 1-based indexing surprises |
| Starlark | Python-like, moderate | deliberately limited (no while, no recursion by default) | excellent, deterministic | embedded | strong runner-up; limited loops/IO make integrations and JSON-heavy transforms awkward |
| **Embedded JavaScript (goja)** | **very high** | **full for data munging** | **none by default: nothing is exposed until added** | **embedded, no Node** | **chosen** |
| Separate Node.js service | very high | full + npm | none (a full runtime with OS authority) | second runtime, IPC, supervision, supply chain | rejected: turns "extension" into "second application" |

**Consequences.** Familiar language, portable single binary, capabilities opt-in. Measured cost: ~135× a native call for a trivial transform (docs/BENCHMARKS.md), mitigated by Go-side metric/event filters. Goja cannot bound heap allocation (mitigated by deadline, caps, interpreter rebuild, cgroup limits — stated in SCRIPTING.md). Scripts are code: write access to the scripts directory is deploy access.

## ADR 8 — JavaScript has a constrained host API

**Context.** The value of embedding evaporates if extensions can reach the OS, the database or the network freely.

**Decision.** A deliberately small, typed, JSON-shaped host API (`log`, `device`, `inventory`, `state`, `config`, `events.emit`, `metrics.emit`, `http.request`); capabilities denied by extension kind; per-invocation budgets; outbound HTTP only to named, granted endpoints with secrets kept in Go; host objects frozen; the API unavailable while a script loads.

**Alternatives.** Expose more (a `db` handle, raw NATS) for convenience — each is a bypass around validation, idempotency and audit; a full Node-style module system — a dependency-management and supply-chain surface we do not want.

**Consequences.** Extensions are easy to reason about and to test in isolation (`script test`); some tasks need a small Go host function first. That friction is the point.

## ADR 9 — JavaScript may propose automation but cannot execute

**Context.** Remediation *judgment* ("do not touch uplinks", "someone disabled that port on purpose") is exactly what static policy expresses badly and scripts express well. Execution authority is exactly what scripts must never have.

**Decision.** `proposeAutomation` returns `null` or a typed proposal; the Go engine validates it against the catalog and the policy's `allowed_actions` and applies every gate. The `automation` kind is read-only in the host API. All failure modes (throw, timeout, garbage, forbidden or injected proposals) end as audited denials.

**Alternatives.** Let scripts call adapters (fast, and uncontrolled); forbid scripts in automation (safe, but loses the judgment layer).

**Consequences.** A compromised or buggy script can at worst *ask*; the worst outcome is a `denied` row. Tested adversarially. Cost: proposals are limited to the catalog.

## ADR 10 — PostgreSQL as the primary persistence layer

**Context.** We need transactions (the duplicate gate, state, events and outbox must commit together), constraints (one firing alert per key), unique keys (idempotency), and queries that operators and the API can ask.

**Decision.** PostgreSQL only, including time-series observations in an indexed table, with embedded forward-only migrations and per-key advisory locks for state. No second database.

**Alternatives.** TimescaleDB (a good growth path; adds an extension to install and operate before it is needed); InfluxDB/Prometheus for metrics plus PostgreSQL for state (two stores, two consistency stories, no cross-store transaction — the exact atomicity we rely on); SQLite (no concurrent writers across roles); a document store (weak constraints).

**Consequences.** One system to back up, one transactional boundary. `observations` needs a retention job and will need partitioning or Timescale at high volume — documented, not pre-built. Not chosen for raw ingest throughput; that is what NATS absorbs.

## ADR 11 — Kubernetes is deliberately not required

**Context.** The problem — a handful of cooperating processes, a broker and a database — does not need a scheduler, and a monitoring platform that requires a cluster to monitor a closet has its priorities inverted.

**Decision.** Run as one binary under systemd or containers; Compose for evaluation; document (not implement) orchestrator deployment. Health endpoints and environment-based configuration keep that door open.

**Alternatives.** Helm chart/operator (useful at scale; large surface to maintain and test for a portfolio-sized project); no containers at all.

**Consequences.** Simple, inspectable deployment with scripts. Cost: no autoscaling or self-healing scheduler out of the box; systemd `Restart=` and readiness-gated deploys cover the common case.

## ADR 12 — Hosted CI is deliberately not part of the repository

**Context.** The repository should be buildable, testable and deployable from scripts on any machine, and inspection of how it is verified should not depend on a vendor's YAML dialect.

**Decision.** No GitHub Actions or other hosted-CI configuration. `make check` (lint, unit, script fixtures, deployment-script tests), `make integration-test`/`make e2e` (throwaway PostgreSQL in Docker) and `make bench` are the whole verification story, runnable locally and by any CI a team chooses to wrap around them. Deployment is scripts, with a preflight gate and smoke test.

**Alternatives.** Hosted pipelines (convenient; couples the definition of "passing" to a platform and hides it from local runs).

**Consequences.** One source of truth for "does it pass", identical locally and anywhere else. Cost: nothing runs automatically on push; whoever integrates this into a team's CI wraps the same `make` targets.
