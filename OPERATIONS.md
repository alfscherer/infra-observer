# Operations

Runbook-style. Commands assume the compose lab (`make run`) unless noted; on hosts, prefix with the `current` release path and use `systemctl`/`journalctl`.

## Startup

Order does not matter for correctness: every role retries NATS/PostgreSQL connections at startup (up to two minutes) and reports *unready* until they are reachable. Recommended order: PostgreSQL, NATS, `migrate`, `processor`, `automation-worker`, `api`, `collector`. Migrations are idempotent and serialised by an advisory lock, so several roles may start with `--migrate` at once.

Each process logs one JSON line per event (fields below) and serves `/metrics`, `/healthz`, `/readyz`, `/health/dependencies` on `observability.listen` (`:9090`); the API serves the same under `/api/health`, `/api/readiness`, `/api/health/dependencies`, `/metrics`.

| Endpoint | Meaning | Use for |
|---|---|---|
| liveness (`/healthz`) | process runs | restart decisions |
| readiness (`/readyz`) | critical dependencies (PostgreSQL, NATS) reachable | load-balancer / rollout gating |
| dependencies | per-check status + latency; scripting is **degraded, not unready** | humans, dashboards |

## Shutdown

SIGTERM: workers stop fetching, wait up to 30 s for in-flight handlers, hand back queued messages (`NakWithDelay(0)`), then exit. Unfinished messages are redelivered — at-least-once — and handled idempotently. systemd's `TimeoutStopSec=45` leaves room. SIGKILL is safe too (that is the crash case the tests cover), just slower to recover (redelivery after `ack_wait`).

## Dependency failures

| Failure | What happens | What you see | Action |
|---|---|---|---|
| PostgreSQL down | stage B errors are `dependency` → NAK with backoff (2 s doubling, ≤ 60 s, `max_deliver` = 5 by default); relay retries; API 503 | `/readyz` 503 (`postgres` down); `messages_failed_total{outcome="retry"}` up; `database_errors_total` up; queue depth grows | restore PostgreSQL; nothing to do afterwards. If the outage outlasted the retry budget (~30 s at defaults), messages are in the DLQ: `make dlq`, then `make dlq ARGS="replay --all"`. Raise `processing.max_deliver`/`retry_delay` if your outages are usually longer |
| NATS down | collectors cannot publish (poll cycle dropped and logged; the next poll retries); consumers reconnect forever; outbox rows wait | `/readyz` 503 (`nats` down); gaps in telemetry | restore NATS; consumers resume by themselves. Events are safe in the outbox |
| Device unreachable | poll failure → `system.reachable=false` with `error_category` → SUSPECT → DOWN after 3 | `device.down`; `collector_failures_total{category}` | category tells *why*: `authentication` (credentials/ACL) vs `timeout` (dead or unreachable) |
| Script broken | skipped, counted, quarantined | `javascript_*` metrics; dependencies report *degraded* | see below |

## Queue inspection

```bash
curl -s localhost:9091/metrics | grep -E 'observation_queue_depth|worker_(capacity|in_flight|buffered)_now|messages_(processed|failed)_total'
curl -s localhost:8222/jsz?consumers=true | jq '.account_details[0].stream_detail[] | {name, state: .state.messages, consumers: [.consumer_detail[] | {name, pending: .num_pending, ack_pending: .num_ack_pending, redelivered: .num_redelivered}]}'
```

`observation_queue_depth{consumer="processor"}` is *the* saturation signal: collection outrunning processing accumulates here (durably), not in memory. Sustained growth ⇒ add processor replicas (same durable name), raise `processing.workers`, or check database latency.

## Dead letters

```bash
make dlq                                   # list: seq, consumer, subject, category, reason, attempts, error
bin/infra-observer deadletter list --payload
make dlq ARGS="replay 42"                  # replay one (after fixing the cause)
make dlq ARGS="replay --all"
make dlq ARGS="purge"
```

Categories tell you what to do: `validation`/`permanent` → the *producer* sent bad data (fix the collector or script; replaying will fail again); `dependency`/`timeout` with `max_deliveries` → an outage outlasted the retries (fix the dependency, then replay). `deadletter.v1` envelopes carry the original payload byte for byte.

## Replay

- **Dead letters**: above.
- **Stream window** (`infra-observer replay --stream TELEMETRY_RAW --since 1h [--dry-run]`): republishes retained messages so the pipeline processes them again. Safe because consumers are idempotent — already-applied observations are skipped. Bounded to the stream's last sequence at the start.
- **Rebuild an empty database**: point a processor at the empty database with new consumer names (the e2e test uses a suffix) and it reads the retained streams from the beginning; deterministic IDs reproduce the same events. Retention (`MaxAge` 2 d) bounds how far back you can rebuild.
- What replay does **not** do: re-evaluate history under *changed* state definitions in an existing database (the duplicate gate skips already-stored observations). Use a fresh database for that.

## JavaScript extensions

```bash
bin/infra-observer script list                 # status of every script (active / disabled / quarantined / failed) and why
make validate-scripts                          # load everything as the service would
make test-scripts                              # run fixtures
curl -s localhost:9091/health/dependencies | jq .checks.scripting
curl -s localhost:9091/metrics | grep javascript_
```

**Disable a faulty extension**: set `scripts.<kind>.<id>.enabled: false` in configuration and restart, or simply fix or remove the file — the processor hot-reloads within 5 s (a changed file starts with a clean failure record; unchanged source stays quarantined). Removing the file from the directory unloads it. A quarantined script logs one `script quarantined` error line with its last error.

**Script changes/reloads**: drop the new file in; watch for `scripts loaded` in the log. A syntax error marks only that script `failed` and leaves the previous behaviour running for other scripts. Always run `make test-scripts` first.

## Log investigation

JSON lines with consistent fields: `role`, `component`, `device_id`, `observation_id`, `event_id`, `automation_id`, `policy_id`, `correlation_id`, `script_id`, `script_version`, `subject`, `duration`, `error`, `category`.

```bash
docker compose logs processor | jq -c 'select(.level=="ERROR" or .level=="WARN")'
docker compose logs --no-log-prefix processor automation | jq -c 'select(.correlation_id=="673af520…")'
```

**Tracing one incident**: take `correlation_id` from `GET /api/events`; it is the same on the event, the automation request and result, the raw and normalized stream messages (`X-Correlation-Id`), and log lines.

## Database migrations

`infra-observer migrate` (or `--migrate` on a role). Each file runs in its own transaction, recorded in `schema_migrations`. **Forward-only.** Keep every migration backward compatible with the release before it (add columns nullable/defaulted; never drop or rename in the same release — expand, then contract later), so a code rollback does not need a schema rollback.

## Configuration changes

Config is read at startup; every scalar has an `INFRA_OBSERVER_<SECTION>_<KEY>` override. `infra-observer config validate` (part of `make validate` and the deploy preflight) fails on unknown keys and bad values. Inventory, profiles, normalization, states, rules and policies are files: change, validate, restart the role. Scripts hot-reload.

## Recovery procedures

- **Processor crashed / OOM**: restart; unacknowledged messages are redelivered and duplicates skipped.
- **Database lost**: restore from backup; then replay retained streams to fill the gap since the backup (idempotent); or rebuild from the streams into a fresh database if within retention.
- **NATS data lost**: telemetry within the retention window is gone; PostgreSQL (the system of record) is intact. Recreate streams (`EnsureStreams` runs on every start); outbox rows still unpublished are relayed.
- **Automation double-executed after a crash**: expected at-least-once behaviour after a lease expiry; adapters are idempotent and the audit trail shows both.
- **Event storm**: check `messages_failed_total`, dead letters, and the state definitions; a state machine should absorb flapping, so a storm usually means a `fail_after: 1` definition or a broken collector.

## Backup considerations

PostgreSQL is the system of record: `pg_dump`/base backups + WAL as usual. NATS JetStream is a bounded replay window and buffer, not a system of record; back it up only if you want longer replay than its retention. `observations` is the large table: apply a retention job (`DELETE … WHERE observed_at < now() - interval '30 days'`, or partition and drop partitions). `alerts`, `events`, `automation_*` are small and worth keeping for audit.

## Secrets

Credentials are references (`snmp/lab`, `endpoints/alerts-webhook`, `api/approvers`) resolved by a `secrets.Resolver`. The lab resolves from environment variables and JSON files under `secrets/`. **Production**: implement `Resolver` against your store (Vault, cloud secret manager) — one small interface, no caller changes — or use systemd `LoadCredential=` to place files under the directory the file resolver reads. Never put values in device records, policies, scripts or the compose file (the lab's credentials are lab-only). Secrets are redacted when printed (`Secret.String()` and its slog value).

## Deployment

**Compose** (this repository): evaluation only. It has no secret store, no TLS, single instances.

**systemd** (traditional hosts): `deployments/scripts/bootstrap.sh` (user, directories, environment template, units), then per release:

```bash
make package VERSION=1.4.0
make deploy PACKAGE=dist/infra-observer-1.4.0-linux-amd64.tar.gz HOST=ops@server     # or run on the host
make smoke API=http://server:8080
make rollback                                                                        # previous release
```

`deploy.sh`: verify checksum → unpack beside the live release → **preflight with the new binary** (`config validate`, `script validate`, with the real environment file) → migrate → atomic `current` symlink switch → restart services → smoke test → **automatic rollback** if the smoke test fails. Every step is a plain command; `--dry-run` prints them. The smoke test proves the data path: readiness green *and* a device seen recently. `deploy.sh` and its refusal paths (tampered package, invalid config, broken script, failed smoke) are tested (`make test-deploy`).

**Rollback** is a symlink switch plus restart. It does not roll back the database (see migrations).

**Configuration management** (Ansible/Puppet/Salt): treat `bootstrap.sh` + `deploy.sh` as the primitives — template `/etc/infra-observer/environment` and the secrets directory from your vault, drop the release tarball, run `deploy.sh`. **Containers/orchestrators**: the image runs every role by command with configuration from environment variables and credentials from mounted files; the health endpoints map onto liveness/readiness probes; NATS and PostgreSQL should be managed services or operators, not sidecars. Not implemented here beyond the compose example.

## Failure scenarios

Each is reproducible in the lab and asserted by the end-to-end tests.

| Do this | Expect |
|---|---|
| `make fail COMPONENT=database` (then `heal`) | processor logs `dependency` retries; `/readyz` 503 on API/processor; telemetry accumulates in JetStream; after `heal` it drains; no dead letters if shorter than the retry budget; events created once |
| `make fail COMPONENT=nats` / `MODE=kill` | collectors log dropped cycles; consumers reconnect on their own; after `heal` telemetry and events flow; outbox rows relayed |
| `make fail COMPONENT=processor MODE=kill` | in-flight messages redelivered after restart; no loss; duplicates skipped |
| `make fail COMPONENT=processor MODE=pause` | process is up but silent: `/healthz` may not answer; backlog grows; a stalled consumer shows on `observation_queue_depth` |
| `make inject KIND=malformed` / `poison` / `future` / `unknown-device` | dead-lettered on the first attempt with category `validation`; good data keeps flowing |
| `make inject KIND=duplicate COUNT=20` | exactly one stored observation |
| `make scenario DEVICE=server-01 EVENT=auth-failure` | polls fail with category `authentication` (not timeout); `device.down` after 3; `device.up` on `auth-ok` |
| `make fault-script SCRIPT=throws` | quarantined after 5 failures; pipeline unaffected; dependencies *degraded*, readiness still green |
| `make fault-script SCRIPT=spins` | each call stopped at the deadline; `javascript_timeouts_total` rises; interpreter rebuilt; quarantined |
| `make fault-script SCRIPT=invalid-output` | rejected by contract validation (`kind="output"`), observation unchanged, quarantined |
| `make fault-script SCRIPT=slow` | latency visible in `javascript_execution_duration_seconds`; under load, saturation and backpressure |
| `make fault-script-clear` | scripts unload within 5 s |

## Alerting on the platform itself

Suggested: `observation_queue_depth` rising for 5 min; `increase(messages_failed_total{outcome="dead_letter"}[5m]) > 0`; `outcome="dlq_failed"` > 0 (page: messages are stuck); readiness down > 1 min; `increase(javascript_quarantines_total[1h]) > 0`; `increase(database_errors_total[5m])`; `collector_failures_total` by category; automation `denied`/`failed` ratios.

## Performance

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md). Likely bottlenecks before any scaling work: PostgreSQL write latency (one transaction per observation), then JavaScript on hot metrics, then NATS. Scale by adding processor replicas (same durable = consumer group) and raising `processing.workers`; batch the observation insert if PostgreSQL becomes the limit.
