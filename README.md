# infra-observer

`infra-observer` is a message-driven infrastructure monitoring and automation
platform written in Go. It treats telemetry collection, normalization, state
evaluation, alerting, automation, and extensibility as separate concerns so each
can be tested, replayed, and reasoned about independently. Embedded JavaScript
provides controlled runtime extensibility without requiring changes to the core
binaries.

> Work in progress: the full README lands with the final documentation commit.

```bash
make setup build test
```
