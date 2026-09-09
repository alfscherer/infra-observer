# Benchmarks

**Local micro-benchmarks. Not capacity claims.** They exist to quantify tradeoffs and to point at likely bottlenecks, not to promise production throughput.

```
local benchmark
hardware:      11th Gen Intel Core i7-1165G7 @ 2.80 GHz, 8 threads, 15 GiB RAM, Linux 7.1
go:            1.26.0 linux/amd64
configuration: go test -bench . -benchmem -benchtime=1s; NATS is an embedded JetStream server
               (file storage, tmpdir, loopback); GOMAXPROCS=8; default configs/
dataset:       one synthetic observation per iteration (see each benchmark); no real devices
run:           make bench
```

| Benchmark | Result | What it measures |
|---|---|---|
| `normalize` `NormalizeMapped` | 668 ns/op, 6 allocs | one mapped metric (`ifOperStatus` → bool, label mapping) |
| `state` `EvaluateReachability` | 100 ns/op, 1 alloc | one state-machine step (pure) |
| `rules` `EvaluateAverage30Samples` | 459 ns/op, 5 allocs | windowed average over 30 samples (excludes fetching the samples) |
| `pipeline` `NormalizeMessageStageA` | 8.7 µs/op, 42 allocs | decode + strict validation + normalize + validate + encode |
| `pipeline` `ProcessStageBMemStore` | 6.2 µs/op, 49 allocs | enrich + duplicate gate + state + persist against the **in-memory** store, i.e. pipeline logic without database cost |
| `messaging` `PublishSync` | 33.7 µs/op | one JetStream publish waiting for its ack |
| `messaging` `PublishBatch50` | 362 µs / 50 msgs ≈ **138 k msgs/s** | pipelined publishes, ack-waited per batch (what the collector does per poll) |
| `scripting` `TransformNativeGo` | 401 ns/op, 5 allocs | the native-Go equivalent of `normalize-cpu.js` |
| `scripting` `TransformJavaScript1Worker` | 54.4 µs/op, 587 allocs | the same transform through the full extension path |
| `scripting` `TransformJavaScript4WorkersParallel` | 29.0 µs/op (≈ 34 k/s) | same, 4 executors under `RunParallel` |

## Reading them

- **Native Go vs embedded JavaScript**: ~135× per call (401 ns vs 54 µs) for a trivial transform. That price buys behaviour change without a rebuild, isolation, deadlines and contract validation — the JS number includes JSON in/out, interpreter call, strict decode, validation and immutability checks, not just the arithmetic. It does **not** mean either should replace the other: hot, stable, vendor-independent logic belongs in Go (normalization mappings, state, rules); quirks and site-specific behaviour belong in scripts. This is why transforms must declare `meta.metrics`: Go filters first, so only the few observations a script cares about pay the JS cost. Parallel scaling is sub-linear (1.9× with 4 workers) because JSON marshalling and allocation dominate.
- **Stage costs without I/O are microseconds** — normalization, state and rules are effectively free next to the database. At the lab's rate (6 devices × ~40 observations per 10 s ≈ 24 obs/s) CPU is irrelevant.
- **The likely bottleneck is PostgreSQL**: stage B does one transaction per observation (insert + touch + state lookups). A PostgreSQL-backed stage-B benchmark is not included; measure it against your own instance and hardware. If it limits you, batch the observation insert or move the duplicate gate to a bulk `INSERT … ON CONFLICT` before per-key work.
- **Publishing is not the constraint**: batched publish reaches six figures per second on loopback; a real network and a real broker change this.
- **Then JavaScript on hot metrics**, then NATS. Address in that order before scaling out; scaling out is adding processor replicas (a consumer group) and raising `processing.workers`.

Do not extrapolate these to production scale.
