#!/usr/bin/env bash
# Runs the Go test suite with a throwaway PostgreSQL so the database-backed
# integration tests execute instead of skipping. NATS needs nothing external:
# tests embed a JetStream-enabled nats-server.
#
#   hack/integration-test.sh [go test args...]
#
# Set INFRA_OBSERVER_TEST_DATABASE_URL to use an existing database instead.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -n "${INFRA_OBSERVER_TEST_DATABASE_URL:-}" ]]; then
  exec go test -race -count=1 "${@:-./...}"
fi

command -v docker >/dev/null || { echo "docker is required (or set INFRA_OBSERVER_TEST_DATABASE_URL)" >&2; exit 1; }
name="infra-observer-it-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$name" -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test \
  -p 127.0.0.1::5432 --tmpfs /var/lib/postgresql/data "${POSTGRES_IMAGE:-postgres:16-alpine}" >/dev/null
port="$(docker port "$name" 5432/tcp | head -n1 | awk -F: '{print $NF}')"
for _ in $(seq 1 60); do
  docker exec "$name" pg_isready -U postgres -d test >/dev/null 2>&1 && break
  sleep 0.5
done
docker exec "$name" pg_isready -U postgres -d test >/dev/null

export INFRA_OBSERVER_TEST_DATABASE_URL="postgres://postgres:test@127.0.0.1:${port}/test?sslmode=disable"
echo "integration database: 127.0.0.1:${port}"
go test -race -count=1 "${@:-./...}"
