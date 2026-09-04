#!/usr/bin/env bash
# Shared helpers for the deployment scripts. Sourced, not executed.

log()  { printf '%s [%s] %s\n' "$(date -u +%H:%M:%S)" "${SCRIPT_NAME:-deploy}" "$*" >&2; }
die()  { log "ERROR: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

# run prints the command in dry-run mode instead of executing it.
run() {
  if [[ "${DRY_RUN:-0}" == 1 ]]; then log "[dry-run] $*"; else "$@"; fi
}

# load_env exports the variables of an environment file into the current shell
# so validation and migration see the same settings the services will. It parses
# KEY=VALUE lines the way systemd's EnvironmentFile does (no shell expansion),
# so a database URL containing '&' or '$' is taken literally.
load_env() {
  local file="$1" line key val
  [[ -f "$file" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*(#|;|$) ]] && continue
    key="${line%%=*}"; val="${line#*=}"
    key="${key//[[:space:]]/}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    if [[ "$val" =~ ^\"(.*)\"$ || "$val" =~ ^\'(.*)\'$ ]]; then val="${BASH_REMATCH[1]}"; fi
    export "$key=$val"
  done < "$file"
}

# switch_link atomically points LINK at TARGET: build the new link beside the
# old one, then rename over it, so there is never a moment without a valid link.
switch_link() {
  local target="$1" link="$2"
  ln -sfn "$target" "${link}.new"
  mv -Tf "${link}.new" "$link"
}
