#!/usr/bin/env bash
# scripts/e2e.sh — the "Make It Real" end-to-end proof. Boots the full stack, drives ONE real event
# through it, and asserts the seeded sequence_engine_key reaches the viz SSE stream. No simulated logs.
#
# Runs in GitHub Actions (see .github/workflows/e2e.yml) or on any host with Docker + Go. It requires
# an Enterprise license for CockroachDB's Kafka-sink changefeeds:
#   CRDB_LICENSE  (required)   free tier: https://www.cockroachlabs.com/get-cockroachdb/enterprise/
#   CRDB_ORG      (required)
#
# Exit 0 == the golden path ran for real end to end.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SSE_URL="${SSE_URL:-http://localhost:8081/api/stream}"
SSE_OUT="$(mktemp)"
SSE_PID=""

log()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
fail() { printf '\n\033[1;31m✗ %s\033[0m\n' "$*"; }

cleanup() {
  local code=$?
  [ -n "$SSE_PID" ] && kill "$SSE_PID" 2>/dev/null || true
  if [ "$code" -ne 0 ]; then
    fail "E2E failed (exit $code) — dumping container logs"
    docker compose logs --no-color --tail=120 || true
  fi
  log "Tearing down stack"
  docker compose down -v --remove-orphans || true
  rm -f "$SSE_OUT"
  exit "$code"
}
trap cleanup EXIT

# CRDB_LICENSE is OPTIONAL: a single-node CockroachDB v24.3+ cluster runs changefeeds license-free.
# If a key is set it flows through to crdb-init; if not, we still run and let crdb-init's changefeed
# creation be the real test. No silent skip — a licensing failure surfaces as a hard non-zero exit.

log "Building + starting the stack"
docker compose up -d --build

log "Waiting for crdb-init to complete (schema + changefeeds)"
INIT_CODE="$(docker compose wait crdb-init)"
if [ "$INIT_CODE" != "0" ]; then
  fail "crdb-init exited non-zero ($INIT_CODE) — schema/changefeed bootstrap failed"
  exit 1
fi

log "Asserting at least 3 changefeeds are running"
RUNNING="$(docker compose exec -T cockroachdb \
  cockroach sql --insecure -d omniflow --format=csv \
  -e "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'" | tail -n 1 | tr -d '[:space:]')"
echo "running changefeeds: ${RUNNING}"
if [ "${RUNNING:-0}" -lt 3 ]; then
  fail "expected >=3 running changefeeds, found ${RUNNING}"
  exit 1
fi

# Connect the SSE consumer BEFORE seeding. The gateway broadcasts live to connected clients only
# (no server-side backlog), so a late connect would miss the projection.
log "Opening SSE stream: $SSE_URL"
curl -sN --max-time 45 "$SSE_URL" > "$SSE_OUT" &
SSE_PID=$!
sleep 3  # allow the SSE client to register with the broker

log "Seeding one event end to end"
SEED_LOG="$(mktemp)"
KAFKA_BROKERS="localhost:9092" \
CRDB_DSN="postgres://root@localhost:26257/omniflow?sslmode=disable" \
SEED_MODE="${SEED_MODE:-outbox}" \
  go run ./tools/seed | tee "$SEED_LOG"

SEQ_KEY="$(grep '^SEED_SEQUENCE_ENGINE_KEY=' "$SEED_LOG" | cut -d= -f2)"
rm -f "$SEED_LOG"
if [ -z "$SEQ_KEY" ]; then
  fail "seeder did not report a sequence_engine_key"
  exit 1
fi
log "Seeded sequence_engine_key = $SEQ_KEY — waiting for it on the SSE stream"

# The p2p.completed changefeed fires ~1-2s after approval; poll the captured stream for up to 20s.
FOUND=0
for _ in $(seq 1 20); do
  if grep -q "$SEQ_KEY" "$SSE_OUT"; then FOUND=1; break; fi
  sleep 1
done

if [ "$FOUND" -ne 1 ]; then
  fail "sequence_engine_key $SEQ_KEY never reached the SSE stream"
  echo "---- captured SSE output ----"
  cat "$SSE_OUT" || true
  exit 1
fi

log "✓ E2E PASSED — projection $SEQ_KEY reached the viz SSE stream"
echo "---- matching SSE line ----"
grep "$SEQ_KEY" "$SSE_OUT" | head -n 1
exit 0
