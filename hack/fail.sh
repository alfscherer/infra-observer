#!/usr/bin/env bash
# Failure injection for the compose lab.
#
#   hack/fail.sh fail COMPONENT [stop|pause|kill]
#   hack/fail.sh heal COMPONENT
#
# COMPONENT: database (postgres) | nats | processor | collector | automation | api | simulator | webhook
#
#   stop   graceful stop (SIGTERM): the process drains and exits
#   kill   SIGKILL: a crash in the middle of whatever it was doing
#   pause  freeze the process (SIGSTOP semantics): it stays "up" but stops answering,
#          the nastiest case for timeouts and health checks
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="${COMPOSE:-docker compose}"

action="${1:-}"; component="${2:-}"; mode="${3:-stop}"
[[ -n "$action" && -n "$component" ]] || { sed -n '2,15p' "$0"; exit 2; }
case "$component" in
  database|db|postgres) svc=postgres ;;
  broker|nats) svc=nats ;;
  processor|collector|automation|api|simulator|webhook) svc="$component" ;;
  *) echo "unknown component $component" >&2; exit 2 ;;
esac

case "$action" in
  fail)
    case "$mode" in
      stop)  $COMPOSE stop "$svc" ;;
      kill)  $COMPOSE kill "$svc" ;;
      pause) $COMPOSE pause "$svc" ;;
      *) echo "unknown mode $mode (stop, kill, pause)" >&2; exit 2 ;;
    esac
    echo "$svc is down ($mode). Watch it: make logs SERVICE=processor; curl localhost:8080/api/readiness"
    echo "Recover with: make heal COMPONENT=$component" ;;
  heal)
    $COMPOSE unpause "$svc" 2>/dev/null || true
    $COMPOSE start "$svc"
    echo "$svc is back." ;;
  *) echo "unknown action $action" >&2; exit 2 ;;
esac
