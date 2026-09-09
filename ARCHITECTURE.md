# Architecture

## Shape

A modular monolith shipped as one binary with several roles. Processes are split only where the failure or scaling behaviour genuinely differs:

| Role | Responsibility | Depends on |
|---|---|---|
| `collector` | poll devices, publish raw observations | NATS |
| `processor` | stage A (validate, normalize, JS transforms), stage B (enrich, JS enrich, state, rules, persist), outbox relay, stale sweeper, inventory consumer | NATS, PostgreSQL |
| `automation-worker` | react to alert events: policies, JS proposals, gates, adapters, integrations, own outbox | NATS, PostgreSQL |
| `api` | read API, approvals | PostgreSQL only |
| `simulator` / `webhook-sink` | the lab | NATS |

There is deliberately no process per pipeline stage. Stages A and B are separate NATS consumers *inside* the processor because the stream between them is a useful replay point, not because they need separate deployments.

## Package map

```
domain/            plain types + the error taxonomy (retryable or not)
config/ secrets/   YAML+env config; credential references, never values
inventory/         device registry (read model)
collector/         scheduler; snmp/ profiles, session abstraction, poller
schema/            wire formats, strict decoding, validation limits
normalize/ enrich/ pure stages
state/ rules/      pure evaluators: (previous, input) -> (next, events)
persistence/       Store/Tx contract; PostgreSQL + in-memory; migrations runner
pipeline/          orders the stages, owns the transaction, outbox relay, sweeper
messaging/         NATS/JetStream client, sharded worker, DLQ, replay
scripting/         runtime (goja pool), registry, host api, contracts, script test runner
automation/        catalog, policies, engine, adapters
api/ health/ telemetry/   HTTP, health, metrics
sim/               SNMP agents, world, scenarios
app/               role assembly shared by cmd and the e2e tests
```

Dependencies point inward: `state`, `rules`, `normalize`, `automation` policy logic do no I/O and read no clock, so they are deterministic and testable without infrastructure.

## Key design properties

**Collection is separate from meaning.** Collectors emit observations and nothing else: no thresholds, no health, no actions. A failed poll is published as `collector.poll_success=false` with an error category; it is the state engine that decides whether that matters.

**Idempotency is a database property.** Stage B's first statement is `INSERT observation ... ON CONFLICT DO NOTHING`. If the row exists the message is a duplicate and nothing else happens. State, transitions, events, alerts and outbox rows commit in the same transaction. Event IDs are deterministic, so even a bug that bypassed the gate could not create a second event.

**Transactional outbox.** Publishing after commit would lose events on a crash, and a redelivered message is skipped as a duplicate, so it would never re-publish. Events are therefore written to an `outbox` table in the same transaction and relayed to NATS (at-least-once, with a JetStream message ID for de-duplication).

**Ordering.** Messages are sharded by device: one device's messages are handled in order within a shard; devices run in parallel. Redelivery after a NAK can reorder, so the state engine ignores samples older than the last applied one.

**Backpressure is structural.** Every queue is bounded. A worker holds at most `shards × (queue+1)` messages and caps its fetch to match; the rest of the backlog waits in JetStream. The script executor has a bounded queue and reports `ErrSaturated` (retryable) rather than growing.

**Error taxonomy drives behaviour.** `transient`, `timeout`, `dependency` are retried with backoff; `validation`, `permanent`, `unsupported`, `authentication`, `script` are not. Unknown errors are permanent: an unknown failure is not assumed to heal.

**Extensions are at the edges.** JavaScript participates in stage A (transform), stage B before state (enrich), automation proposals and integrations. It never sits between state and persistence.

## Data model

PostgreSQL only. Tables: `devices`, `observations` (PK = idempotency gate; indexed for series/window queries), `state_current`, `state_transitions`, `events`, `alerts` (unique firing alert per key), `outbox`, `automation_requests`, `automation_results`. Time-series data lives in `observations` without a second database: at this scale a plain indexed table is simpler to run, back up and reason about, and the access patterns (latest per series, windows for rules) are index-friendly. Partitioning or TimescaleDB is the growth path (DECISIONS.md ADR 10).

## NATS subjects and streams

See [PIPELINE.md](PIPELINE.md#nats-subjects-and-streams).
