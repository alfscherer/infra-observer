#!/usr/bin/env bash
# Validate configuration and JavaScript extensions without starting anything.
# This is the gate that runs before a deploy and before every commit:
#
#   1. the configuration loads and every referenced file parses
#   2. every script compiles, satisfies its contract and loads
#   3. every script passes its fixtures
#
#   deployments/scripts/validate.sh [--bin PATH] [--config FILE]
set -euo pipefail
SCRIPT_NAME=validate
cd "$(dirname "$0")/../.."
# shellcheck source=lib.sh
source deployments/scripts/lib.sh

BIN="bin/infra-observer"; CONFIG="configs/config.yaml"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) die "unknown argument $1" ;;
  esac
done
[[ -x "$BIN" ]] || die "binary $BIN not found; run 'make build' first"

log "validating configuration ($CONFIG)"
"$BIN" config validate --config "$CONFIG"
log "validating JavaScript extensions"
"$BIN" script validate --config "$CONFIG"
log "running extension fixtures"
"$BIN" script test-all --config "$CONFIG" >/dev/null
log "ok"
