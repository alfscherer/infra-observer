#!/usr/bin/env bash
# Smoke test a running deployment through its API.
#
#   deployments/scripts/smoke.sh [--api URL] [--timeout SECONDS] [--min-devices N] [--max-age SECONDS]
#
# It proves the whole data path, not just that processes are up: readiness must
# be green, devices must be registered, and at least one device must have been
# seen recently, which can only be true if collection, NATS, the processor and
# PostgreSQL all did their jobs.
set -euo pipefail
SCRIPT_NAME=smoke
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
need curl

API="${API:-http://localhost:8080}"; TIMEOUT=90; MIN_DEVICES=1; MAX_AGE=180
while [[ $# -gt 0 ]]; do
  case "$1" in
    --api) API="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --min-devices) MIN_DEVICES="$2"; shift 2 ;;
    --max-age) MAX_AGE="$2"; shift 2 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) die "unknown argument $1" ;;
  esac
done

deadline=$(( $(date +%s) + TIMEOUT ))
last="not started"
while :; do
  if code=$(curl -s -o /tmp/smoke.readiness -w '%{http_code}' --max-time 5 "$API/api/readiness") && [[ "$code" == 200 ]]; then
    if devices=$(curl -sf --max-time 5 "$API/api/devices?limit=500"); then
      # Tolerant of whitespace in the JSON; `|| true` because grep exits 1 on no match.
      count=$(printf '%s' "$devices" | { grep -o '"id" *:' || true; } | wc -l)
      newest=$(printf '%s' "$devices" | { grep -o '"last_seen" *: *"[^"]*"' || true; } | sed 's/.*: *"\(.*\)"/\1/' | sort | tail -1)
      if [[ "$count" -ge "$MIN_DEVICES" && -n "$newest" ]]; then
        age=$(( $(date +%s) - $(date -d "$newest" +%s) ))
        if [[ "$age" -le "$MAX_AGE" ]]; then
          log "ok: ready, $count devices, freshest data ${age}s old"
          exit 0
        fi
        last="data is stale: freshest device was seen ${age}s ago (limit ${MAX_AGE}s)"
      else
        last="$count devices registered, none with recent data yet"
      fi
    else
      last="readiness ok but /api/devices failed"
    fi
  else
    last="readiness returned ${code:-no response}: $(head -c 300 /tmp/smoke.readiness 2>/dev/null || true)"
  fi
  [[ $(date +%s) -lt $deadline ]] || die "smoke test failed after ${TIMEOUT}s: $last"
  sleep 3
done
