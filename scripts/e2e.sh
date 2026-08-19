#!/usr/bin/env bash
# scripts/e2e.sh — the "Make It Real" end-to-end proof. Boots the full stack, drives ONE real event
# through it, and asserts the seeded sequence_engine_key reaches the viz SSE stream. No simulated logs.
#
# Runs in GitHub Actions (see .github/workflows/e2e.yml) or on any host with Docker + Go. It needs NO
# license: a single-node CockroachDB v24.3+ cluster runs Kafka-sink changefeeds license-free.
#   CRDB_LICENSE  (optional)   only for a multi-node cluster; free tier:
#                              https://www.cockroachlabs.com/get-cockroachdb/enterprise/
#   CRDB_ORG      (optional)   accompanies CRDB_LICENSE
#
# To test against a remote CockroachDB Serverless instance instead of the local container:
#   CRDB_DSN      (optional)   e.g. "postgresql://user:pass@serverless-host:26257/omniflow"
#
# Exit 0 == the golden path ran for real end to end.
set -euo pipefail

# Git Bash / MSYS2 on Windows rewrites arguments that look like absolute POSIX paths into Windows
# paths before the process ever sees them, so `docker compose exec -T kafka /opt/kafka/bin/...`
# arrives inside the container as `C:/Program Files/Git/opt/kafka/bin/...` and exits 127. The path
# is meaningful in the CONTAINER, not on the host, so the rewrite is always wrong here. Exporting
# this disables it. On Linux and macOS the variable is simply ignored, so CI is unaffected — which
# is exactly why this never showed up until the stack was first booted on Windows.
export MSYS_NO_PATHCONV=1

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

log "Waiting for kafka-init to complete (topic bootstrap)"
# Same wait/inspect pattern as crdb-init below: `docker compose wait` adopts the container's exit
# code as its own, which `set -e` would turn into a silent abort before we can report anything.
docker compose wait kafka-init >/dev/null 2>&1 || true
KINIT_CID="$(docker compose ps -aq kafka-init)"
KINIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$KINIT_CID" 2>/dev/null || echo unknown)"
log "kafka-init exit code: ${KINIT_CODE}"
if [ "$KINIT_CODE" != "0" ]; then
  fail "kafka-init exited non-zero ($KINIT_CODE) — topic bootstrap failed"
  docker compose logs kafka-init || true
  exit 1
fi

# Assert the full topology up front. Without this a missing topic resurfaces much later as an opaque
# "UNKNOWN_TOPIC_OR_PARTITION" mid-seed (the original CI failure) instead of naming what's absent.
log "Asserting all expected topics exist"
EXPECTED_TOPICS="omniflow.communication.v1
omniflow.communication.v1.dlq
omniflow.orchestration.v1
omniflow.orchestration.v1.dlq
omniflow.p2p.approval.v1
omniflow.p2p.completed.v1
omniflow.inventory.movement.v1
omniflow.inventory.movement.v1.dlq
omniflow.inventory.fact_inventory_movement
omniflow.inventory.fact_inventory_snapshot"

ACTUAL_TOPICS="$(docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list | tr -d '\r')"

MISSING=""
while IFS= read -r t; do
  [ -z "$t" ] && continue
  printf '%s\n' "$ACTUAL_TOPICS" | grep -qx "$t" || MISSING="${MISSING} ${t}"
done <<< "$EXPECTED_TOPICS"

if [ -n "$MISSING" ]; then
  fail "missing Kafka topics:${MISSING}"
  echo "actual topics:"; printf '%s\n' "$ACTUAL_TOPICS"
  exit 1
fi
echo "all 10 expected topics present"

log "Waiting for crdb-init to complete (schema + changefeeds)"
# `docker compose wait` adopts the container's exit code as its OWN status, so under `set -e` a
# non-zero crdb-init aborts here before we can report it. Block for the stop, then read the true
# exit code with docker inspect (unambiguous) and branch on that.
docker compose wait crdb-init >/dev/null 2>&1 || true
INIT_CID="$(docker compose ps -aq crdb-init)"
INIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$INIT_CID" 2>/dev/null || echo unknown)"
log "crdb-init exit code: ${INIT_CODE}"
if [ "$INIT_CODE" != "0" ]; then
  fail "crdb-init exited non-zero ($INIT_CODE) — schema/changefeed bootstrap failed"
  docker compose logs crdb-init || true
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

log "Waiting for the four app services to report READY (not merely started)"
# Until the services grew a healthcheck this step did not exist, and nothing here waited for them
# at all: the run went straight from crdb-init to seeding and relied on the seeder's own 60s poll
# for SUSPENDED to absorb the startup race. That hid the difference between "the orchestrator was
# slow to connect" and "the orchestrator never came up", because both surfaced identically as the
# seeder timing out on a workflow that never suspended.
#
# The images are distroless, so the healthcheck runs the service binary's own -probe mode against
# its /readyz — which is 503 until the pgx pool is live AND the Kafka consumers are wired.
for svc in commbot inventory-intelligence p2p-orchestrator viz-gateway; do
  DEADLINE=$(( $(date +%s) + 120 ))
  while :; do
    CID="$(docker compose ps -q "$svc")"
    STATUS="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$CID" 2>/dev/null || echo unknown)"
    case "$STATUS" in
      healthy) echo "  $svc: healthy"; break ;;
      none|unknown)
        fail "$svc reports no healthcheck — the compose healthcheck is missing or the image has no /svc"
        exit 1 ;;
    esac
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
      fail "$svc never became healthy (last status: $STATUS)"
      docker compose logs --tail 50 "$svc" || true
      exit 1
    fi
    sleep 2
  done
done

# Connect the SSE consumer BEFORE seeding. The gateway broadcasts live to connected clients only
# (no server-side backlog), so a late connect would miss the projection.
log "Opening SSE stream: $SSE_URL"
# --max-time must comfortably exceed (seed duration + assertion window). The seeder alone may poll up
# to 60s waiting for the workflow to reach SUSPENDED, so the old 45s ceiling could kill this capture
# mid-assertion and report "never reached the SSE stream" for a stream that was simply no longer being
# read. The cleanup trap kills this process regardless, so a generous ceiling costs nothing.
curl -sN --max-time 180 "$SSE_URL" > "$SSE_OUT" &
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

# The p2p.completed changefeed fires ~1-2s after approval, but the resumed DAG has several nodes to
# execute first and CI runners are slow, so allow real margin rather than flaking at the boundary.
FOUND=0
for _ in $(seq 1 40); do
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
