#!/usr/bin/env bash
# Build a release tarball: binary, configuration, JavaScript extensions,
# systemd units and the deployment scripts, plus a sha256 checksum.
#
#   VERSION=1.2.0 GOOS=linux GOARCH=amd64 deployments/scripts/package.sh
#
# The tarball is reproducible for a given source tree and version: entries are
# sorted and carry no timestamps or owners.
set -euo pipefail
SCRIPT_NAME=package
cd "$(dirname "$0")/../.."
# shellcheck source=lib.sh
source deployments/scripts/lib.sh
need go; need tar; need sha256sum

VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
GOOS="${GOOS:-linux}"; GOARCH="${GOARCH:-amd64}"
NAME="infra-observer-${VERSION}-${GOOS}-${GOARCH}"
OUT="${OUT:-dist}"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
ROOT="$STAGE/$NAME"

log "building $NAME"
mkdir -p "$ROOT/bin" "$OUT"
CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" go build -trimpath \
  -ldflags "-s -w -X main.version=${VERSION}" -o "$ROOT/bin/infra-observer" ./cmd/infra-observer

cp -r configs scripts "$ROOT/"
mkdir -p "$ROOT/deployments"
cp -r deployments/systemd deployments/scripts "$ROOT/deployments/"
printf '%s\n' "$VERSION" > "$ROOT/VERSION"

tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -C "$STAGE" -czf "$OUT/$NAME.tar.gz" "$NAME"
( cd "$OUT" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256" )
log "wrote $OUT/$NAME.tar.gz ($(du -h "$OUT/$NAME.tar.gz" | cut -f1))"
log "sha256 $(cut -d' ' -f1 "$OUT/$NAME.tar.gz.sha256")"
echo "$OUT/$NAME.tar.gz"
