#!/usr/bin/env bash
# Point the deployment back at the previous release and restart.
#
#   deployments/scripts/rollback.sh [--prefix DIR] [--to VERSION] [--no-systemd] [--dry-run]
#
# Rolling back is safe for the code, but NOT automatically for the database:
# migrations are forward-only. That is why every migration must stay compatible
# with the release before it (expand, then contract in a later release). See
# OPERATIONS.md, "Rollback".
set -euo pipefail
SCRIPT_NAME=rollback
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

DRY_RUN="${DRY_RUN:-0}"; PREFIX="${PREFIX:-/opt/infra-observer}"; TO=""; SYSTEMD=1
SERVICES="${SERVICES:-api processor automation-worker collector}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --to) TO="$2"; shift 2 ;;
    --no-systemd) SYSTEMD=0; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) die "unknown argument $1" ;;
  esac
done

current="$(readlink "$PREFIX/current" 2>/dev/null || true)"
if [[ -n "$TO" ]]; then
  target="releases/$TO"
else
  [[ -f "$PREFIX/.previous" ]] || die "no previous release recorded in $PREFIX/.previous; use --to VERSION"
  target="$(cat "$PREFIX/.previous")"
fi
[[ -d "$PREFIX/$target" ]] || die "release $target does not exist under $PREFIX"
[[ "$target" != "$current" ]] || die "already on $target"

log "rolling back $current -> $target"
run switch_link "$target" "$PREFIX/current"
[[ "${DRY_RUN:-0}" == 1 ]] || printf '%s\n' "$current" > "$PREFIX/.previous"
if [[ "$SYSTEMD" == 1 ]]; then
  for s in $SERVICES; do run systemctl restart "infra-observer@$s.service"; done
else
  log "--no-systemd: restart the services yourself"
fi
log "done. If the newer release applied database migrations, they remain applied."
