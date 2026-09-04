#!/usr/bin/env bash
# Exercise the deployment scripts in a temporary prefix, without systemd, a
# database or root: package, deploy, upgrade, rollback, and every refusal path.
#
#   deployments/scripts/test-deploy.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
HERE=deployments/scripts
export SCRIPT_NAME=test-deploy

pass=0; fail=0
ok()   { pass=$((pass+1)); printf '  ok    %s\n' "$1"; }
bad()  { fail=$((fail+1)); printf '  FAIL  %s\n' "$1"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1"; fi; }

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
PREFIX="$WORK/opt"; DIST="$WORK/dist"
DEPLOY=("$HERE/deploy.sh" --prefix "$PREFIX" --env "$WORK/none.env" --no-systemd --skip-migrate --skip-smoke)

echo "packaging"
p1="$(VERSION=v1 OUT="$DIST" "$HERE/package.sh" 2>/dev/null)"
p2="$(VERSION=v2 OUT="$DIST" "$HERE/package.sh" 2>/dev/null)"
check "package produces a tarball and a checksum" "[[ -f $p1 && -f $p1.sha256 ]]"
listing="$(tar -tzf "$p1")"
for want in bin/infra-observer configs/config.yaml scripts/transforms/normalize-cpu.js systemd/infra-observer@.service deployments/scripts/deploy.sh VERSION; do
  check "tarball contains $want" "grep -q '$want' <<<\"\$listing\""
done
sum_a="$(sha256sum "$p1" | cut -d' ' -f1)"
p1b="$(VERSION=v1 OUT="$WORK/dist2" "$HERE/package.sh" 2>/dev/null)"
check "the same source and version produce the same archive layout" "[[ \$(tar -tzf $p1 | sort | md5sum) == \$(tar -tzf $p1b | sort | md5sum) ]]"

echo "deploy"
"${DEPLOY[@]}" --package "$p1" >/dev/null 2>&1
check "first deploy creates current -> releases/v1" "[[ \$(readlink $PREFIX/current) == releases/v1 ]]"
check "the deployed binary runs" "$PREFIX/current/bin/infra-observer version | grep -q v1"
"${DEPLOY[@]}" --package "$p2" >/dev/null 2>&1
check "upgrade switches current to v2" "[[ \$(readlink $PREFIX/current) == releases/v2 ]]"
check "the previous release is recorded" "[[ \$(cat $PREFIX/.previous) == releases/v1 ]]"
check "the old release stays on disk for rollback" "[[ -d $PREFIX/releases/v1 ]]"
"${DEPLOY[@]}" --package "$p2" >/dev/null 2>&1
check "deploying the running version again is a no-op" "[[ \$(readlink $PREFIX/current) == releases/v2 && \$(cat $PREFIX/.previous) == releases/v1 ]]"

echo "rollback"
"$HERE/rollback.sh" --prefix "$PREFIX" --no-systemd >/dev/null 2>&1
check "rollback restores v1" "[[ \$(readlink $PREFIX/current) == releases/v1 ]]"
check "rollback records v2 as the release to return to" "[[ \$(cat $PREFIX/.previous) == releases/v2 ]]"
check "rollback to an unknown release is refused" "! $HERE/rollback.sh --prefix $PREFIX --no-systemd --to v99 >/dev/null 2>&1"

echo "refusals"
before="$(readlink "$PREFIX/current")"
cp "$p2" "$WORK/tampered.tar.gz"; cp "$p2.sha256" "$WORK/tampered.tar.gz.sha256"
sed -i "s#$(basename "$p2")#tampered.tar.gz#" "$WORK/tampered.tar.gz.sha256"
printf 'x' >> "$WORK/tampered.tar.gz"
check "a tampered package is refused" "! ${DEPLOY[*]} --package $WORK/tampered.tar.gz >/dev/null 2>&1"

# a package whose configuration is invalid must fail preflight and leave everything untouched
bad="$WORK/bad"; mkdir -p "$bad"; tar -xzf "$p2" -C "$bad"
d="$(ls "$bad")"; printf 'v3-bad\n' > "$bad/$d/VERSION"
printf 'processing:\n  workers: 0\n' > "$bad/$d/configs/config.yaml"
tar -czf "$WORK/bad.tar.gz" -C "$bad" "$d"
check "invalid configuration fails preflight" "! ${DEPLOY[*]} --package $WORK/bad.tar.gz >/dev/null 2>&1"
check "a failed preflight leaves the live release untouched" "[[ \$(readlink $PREFIX/current) == $before ]]"
check "a failed preflight leaves no half-installed release behind" "[[ ! -d $PREFIX/releases/v3-bad ]]"

# a broken extension is caught by the same gate
bs="$WORK/badscript"; mkdir -p "$bs"; tar -xzf "$p2" -C "$bs"
d="$(ls "$bs")"; printf 'v4-badscript\n' > "$bs/$d/VERSION"
printf 'export function transform( {\n' > "$bs/$d/scripts/transforms/broken.js"
tar -czf "$WORK/badscript.tar.gz" -C "$bs" "$d"
check "a broken JavaScript extension fails preflight" "! ${DEPLOY[*]} --package $WORK/badscript.tar.gz >/dev/null 2>&1"

echo "dry run"
before="$(readlink "$PREFIX/current")"
"$HERE/deploy.sh" --prefix "$PREFIX" --no-systemd --dry-run --package "$p2" >/dev/null 2>&1 || true
check "--dry-run changes nothing" "[[ \$(readlink $PREFIX/current) == $before ]]"

echo "smoke (against a stub API)"
port=$((20000 + RANDOM % 20000))
cat > "$WORK/stub.py" <<PY
import http.server, json, datetime, sys
fresh = sys.argv[2] == "fresh"
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path.startswith("/api/readiness"): body = json.dumps({"ready": True})
        elif self.path.startswith("/api/devices"):
            t = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=5 if fresh else 3600)
            body = json.dumps({"items": [{"id": "switch-01", "last_seen": t.strftime("%Y-%m-%dT%H:%M:%SZ")}]})
        else: self.send_response(404); self.end_headers(); return
        self.send_response(200); self.send_header("Content-Type","application/json"); self.end_headers(); self.wfile.write(body.encode())
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
if command -v python3 >/dev/null; then
  python3 "$WORK/stub.py" "$port" fresh & spid=$!
  sleep 0.7
  check "smoke passes when the data path is alive" "$HERE/smoke.sh --api http://127.0.0.1:$port --timeout 5 >/dev/null 2>&1"
  kill $spid 2>/dev/null || true; wait $spid 2>/dev/null || true
  python3 "$WORK/stub.py" "$port" stale & spid=$!
  sleep 0.7
  check "smoke fails when devices exist but data is stale" "! $HERE/smoke.sh --api http://127.0.0.1:$port --timeout 4 >/dev/null 2>&1"
  kill $spid 2>/dev/null || true; wait $spid 2>/dev/null || true
  # deploy + failing smoke => automatic rollback
  python3 "$WORK/stub.py" "$port" stale & spid=$!
  sleep 0.7
  "$HERE/rollback.sh" --prefix "$PREFIX" --no-systemd --to v2 >/dev/null 2>&1 || true   # start from v2
  p5="$(VERSION=v5 OUT="$DIST" "$HERE/package.sh" 2>/dev/null)"
  check "a failing smoke test rolls the deploy back automatically" \
    "! $HERE/deploy.sh --prefix $PREFIX --env $WORK/none.env --no-systemd --skip-migrate --smoke-api http://127.0.0.1:$port --package $p5 >/dev/null 2>&1 && [[ \$(readlink $PREFIX/current) == releases/v2 ]]"
  kill $spid 2>/dev/null || true; wait $spid 2>/dev/null || true
else
  echo "  skip  smoke tests (python3 not available)"
fi

echo "bootstrap"
"$HERE/bootstrap.sh" --no-user --prefix "$WORK/boot/opt" --etc "$WORK/boot/etc" --unit-dir "$WORK/boot/units" >/dev/null 2>&1
check "bootstrap creates directories, the environment template and units" \
  "[[ -d $WORK/boot/opt/releases && -f $WORK/boot/etc/environment && -f $WORK/boot/units/infra-observer@.service && -f $WORK/boot/units/infra-observer.target ]]"
echo "EDITED" >> "$WORK/boot/etc/environment"
"$HERE/bootstrap.sh" --no-user --prefix "$WORK/boot/opt" --etc "$WORK/boot/etc" --unit-dir "$WORK/boot/units" >/dev/null 2>&1
check "re-running bootstrap never overwrites an edited environment file" "grep -q EDITED $WORK/boot/etc/environment"

echo "env file parsing"
cat > "$WORK/t.env" <<'ENVFILE'
A=1
# comment
URL=postgres://u:p@h/db?sslmode=require&x=$y
Q="quoted value"

BAD LINE
ENVFILE
got="$(bash -c 'source "$1"; load_env "$2"; echo "$A|$URL|$Q"' _ "$HERE/lib.sh" "$WORK/t.env")"
expected='1|postgres://u:p@h/db?sslmode=require&x=$y|quoted value'
check "environment files are parsed like systemd's, without shell expansion" '[[ "$got" == "$expected" ]]'

echo
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
