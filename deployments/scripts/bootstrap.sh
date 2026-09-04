#!/usr/bin/env bash
# Prepare a host for infra-observer: service user, directories, systemd units
# and an environment file template. Idempotent; safe to re-run.
#
#   sudo deployments/scripts/bootstrap.sh [--prefix /opt/infra-observer] [--etc /etc/infra-observer]
#        [--unit-dir /etc/systemd/system] [--no-user] [--dry-run]
#
# It installs nothing but structure. PostgreSQL and NATS (with JetStream) are
# expected to exist already; see OPERATIONS.md for how to provision them.
set -euo pipefail
SCRIPT_NAME=bootstrap
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

PREFIX="${PREFIX:-/opt/infra-observer}"; ETC="${ETC:-/etc/infra-observer}"; UNITS="${UNITS:-/etc/systemd/system}"; MAKE_USER=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --etc) ETC="$2"; shift 2 ;;
    --unit-dir) UNITS="$2"; shift 2 ;;
    --no-user) MAKE_USER=0; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) die "unknown argument $1" ;;
  esac
done

if [[ $MAKE_USER == 1 ]]; then
  if id observer >/dev/null 2>&1; then log "user observer exists"
  else run useradd --system --home-dir "$PREFIX" --shell /usr/sbin/nologin observer; fi
fi
run mkdir -p "$PREFIX/releases" "$ETC/secrets"
if [[ $MAKE_USER == 1 && "${DRY_RUN:-0}" != 1 ]]; then
  chown -R observer:observer "$PREFIX"
fi
# Secrets are readable by the service user only.
run chmod 0750 "$ETC"; run chmod 0700 "$ETC/secrets"
[[ $MAKE_USER == 1 && "${DRY_RUN:-0}" != 1 ]] && chown -R root:observer "$ETC" && chown -R observer:observer "$ETC/secrets"

if [[ ! -f "$ETC/environment" ]]; then
  log "writing $ETC/environment template (mode 0640): edit it before starting services"
  if [[ "${DRY_RUN:-0}" != 1 ]]; then
    cp "$HERE/../systemd/environment.example" "$ETC/environment"
    chmod 0640 "$ETC/environment"
    [[ $MAKE_USER == 1 ]] && chown root:observer "$ETC/environment"
  fi
else
  log "$ETC/environment exists; leaving it alone"
fi

log "installing systemd units into $UNITS"
run mkdir -p "$UNITS"
for f in "$HERE"/../systemd/*.service "$HERE"/../systemd/*.target; do
  run install -m 0644 "$f" "$UNITS/$(basename "$f")"
done
if command -v systemctl >/dev/null 2>&1 && [[ "$UNITS" == /etc/systemd/system ]]; then run systemctl daemon-reload; fi
log "next: create secrets under $ETC/secrets, edit $ETC/environment, then deploy a release with deploy.sh"
