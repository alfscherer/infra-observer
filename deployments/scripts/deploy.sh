#!/usr/bin/env bash
# Deploy a release tarball to this host, or to another over ssh.
#
#   deployments/scripts/deploy.sh --package dist/infra-observer-1.2.0-linux-amd64.tar.gz \
#       [--prefix /opt/infra-observer] [--env /etc/infra-observer/environment] \
#       [--host user@server] [--no-systemd] [--skip-migrate] [--skip-smoke] \
#       [--smoke-api http://localhost:8080] [--dry-run]
#
# Order of operations, and why:
#   1. verify the checksum            - never run bytes we cannot vouch for
#   2. unpack beside the live release - nothing live is touched yet
#   3. preflight with the NEW binary  - config + extensions validated against the
#                                       real environment BEFORE anything changes
#   4. migrate the database           - forward-only; must stay compatible with the
#                                       release that is still running
#   5. switch the `current` symlink   - atomic
#   6. restart services               - api, processor, automation, collector
#   7. smoke test                     - the whole data path, via the API
#   8. on smoke failure: roll back    - automatically, to the previous release
#
# Every step is a plain command; --dry-run prints them without running.
set -euo pipefail
SCRIPT_NAME=deploy
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

PACKAGE=""; PREFIX="${PREFIX:-/opt/infra-observer}"; ENVFILE="${ENVFILE:-/etc/infra-observer/environment}"
HOST=""; SYSTEMD=1; MIGRATE=1; SMOKE=1; SMOKE_API="${SMOKE_API:-http://localhost:8080}"; KEEP=5
SERVICES="${SERVICES:-api processor automation-worker collector}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --package) PACKAGE="$2"; shift 2 ;;
    --prefix) PREFIX="$2"; shift 2 ;;
    --env) ENVFILE="$2"; shift 2 ;;
    --host) HOST="$2"; shift 2 ;;
    --no-systemd) SYSTEMD=0; shift ;;
    --skip-migrate) MIGRATE=0; shift ;;
    --skip-smoke) SMOKE=0; shift ;;
    --smoke-api) SMOKE_API="$2"; shift 2 ;;
    --keep) KEEP="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) die "unknown argument $1" ;;
  esac
done
[[ -n "$PACKAGE" && -f "$PACKAGE" ]] || die "--package FILE is required and must exist"
need tar; need sha256sum

# Remote mode: copy the package and this script's directory, run it there.
if [[ -n "$HOST" ]]; then
  need ssh; need scp
  remote_dir="/tmp/infra-observer-deploy.$$"
  log "deploying to $HOST"
  run ssh "$HOST" "mkdir -p $remote_dir"
  run scp -q "$PACKAGE" "$PACKAGE.sha256" "$HERE"/*.sh "$HOST:$remote_dir/"
  args=(--package "$remote_dir/$(basename "$PACKAGE")" --prefix "$PREFIX" --env "$ENVFILE" --smoke-api "$SMOKE_API" --keep "$KEEP")
  [[ $SYSTEMD == 1 ]] || args+=(--no-systemd); [[ $MIGRATE == 1 ]] || args+=(--skip-migrate)
  [[ $SMOKE == 1 ]] || args+=(--skip-smoke); [[ "${DRY_RUN:-0}" != 1 ]] || args+=(--dry-run)
  run ssh "$HOST" "bash $remote_dir/deploy.sh ${args[*]}; rc=\$?; rm -rf $remote_dir; exit \$rc"
  exit 0
fi

# 1. checksum
if [[ -f "$PACKAGE.sha256" ]]; then
  log "verifying checksum"
  ( cd "$(dirname "$PACKAGE")" && sha256sum -c --quiet "$(basename "$PACKAGE").sha256" ) || die "checksum mismatch: refusing to deploy"
else
  log "WARNING: no $PACKAGE.sha256 next to the package; skipping verification"
fi

# 2. unpack
# sed reads the whole listing, so tar never gets SIGPIPE under `set -o pipefail`
name="$(tar -tzf "$PACKAGE" | sed -n '1{s#/.*##;p}')"
version="$(tar -xzOf "$PACKAGE" "$name/VERSION" | tr -d '[:space:]')"
[[ -n "$version" ]] || die "package has no VERSION file"
release="$PREFIX/releases/$version"
log "release $version -> $release"
run mkdir -p "$PREFIX/releases"
if [[ -d "$release" ]]; then
  log "release directory exists; reusing it"
else
  tmp="$PREFIX/releases/.unpack.$$"
  run mkdir -p "$tmp"
  run tar -xzf "$PACKAGE" -C "$tmp"
  run mv "$tmp/$name" "$release"
  run rmdir "$tmp"
fi

# 3. preflight: validate with the new binary, in the real environment
if [[ "${DRY_RUN:-0}" != 1 ]]; then
  log "preflight: validating configuration and extensions with the new binary"
  (
    cd "$release"
    load_env "$ENVFILE"
    bin/infra-observer config validate --config configs/config.yaml
    bin/infra-observer script validate --config configs/config.yaml >/dev/null
  ) || { log "preflight failed; the live release was not touched"; [[ -d "$release" && "$(readlink "$PREFIX/current" 2>/dev/null)" != "releases/$version" ]] && rm -rf "$release"; exit 1; }
else
  log "[dry-run] preflight: config validate, script validate"
fi

# 4. migrate
if [[ $MIGRATE == 1 ]]; then
  log "applying database migrations"
  if [[ "${DRY_RUN:-0}" == 1 ]]; then log "[dry-run] bin/infra-observer migrate"; else
    ( cd "$release" && load_env "$ENVFILE" && bin/infra-observer migrate --config configs/config.yaml ); fi
else
  log "skipping migrations (--skip-migrate)"
fi

# 5. switch
previous="$(readlink "$PREFIX/current" 2>/dev/null || true)"
if [[ "$previous" == "releases/$version" ]]; then
  log "already running $version"
else
  [[ -z "$previous" || "${DRY_RUN:-0}" == 1 ]] || printf '%s\n' "$previous" > "$PREFIX/.previous"
  run switch_link "releases/$version" "$PREFIX/current"
fi

# 6. restart
if [[ $SYSTEMD == 1 ]]; then
  for s in $SERVICES; do run systemctl restart "infra-observer@$s.service"; done
else
  log "--no-systemd: not restarting services"
fi

# 7 & 8. smoke, with automatic rollback
if [[ $SMOKE == 1 ]]; then
  log "smoke test"
  if ! run "$HERE/smoke.sh" --api "$SMOKE_API"; then
    log "smoke test FAILED"
    if [[ -n "$previous" ]]; then
      log "rolling back to $previous"
      "$HERE/rollback.sh" --prefix "$PREFIX" $([[ $SYSTEMD == 1 ]] || echo --no-systemd)
    fi
    die "deploy of $version failed and was rolled back"
  fi
else
  log "skipping smoke test (--skip-smoke)"
fi

# prune old releases, never the current or previous one
if [[ "${DRY_RUN:-0}" != 1 ]]; then
  keep_cur="$(readlink "$PREFIX/current" 2>/dev/null | sed 's#.*/##')"
  keep_prev="$(sed 's#.*/##' "$PREFIX/.previous" 2>/dev/null || true)"
  # shellcheck disable=SC2012
  ls -1t "$PREFIX/releases" 2>/dev/null | tail -n +"$((KEEP+1))" | while read -r old; do
    [[ "$old" == "$keep_cur" || "$old" == "$keep_prev" || "$old" == .* ]] && continue
    log "pruning old release $old"; rm -rf "${PREFIX:?}/releases/$old"
  done
fi
log "deployed $version"
