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
# Exit 0 == the golden path ran for real end to end:
#   seed → commbot_outbox → changefeed → orchestration.v1 → DAG → SUSPEND → approval → resume →
#   orchestrator_outbox → changefeed → p2p.completed.v1 → viz-gateway → SSE /api/stream
export COMPOSE_PROJECT_NAME="omniflow-e2e"
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

log "Building + starting the stack"
docker compose up -d --build
wait_init kafka-init

# Assert the full topology up front. Without this a missing topic resurfaces much later as an opaque
# "UNKNOWN_TOPIC_OR_PARTITION" mid-seed (the original CI failure) instead of naming what's absent.
# franz-go never requests auto-topic-creation, so every topic must come from kafka-init — keep this
# list identical to the TOPICS array there.
log "Asserting all expected topics exist"
EXPECTED_TOPICS="omniflow.communication.v1
omniflow.communication.v1.dlq
omniflow.orchestration.v1
omniflow.orchestration.v1.dlq
omniflow.p2p.approval.v1
omniflow.p2p.approval.v1.dlq
omniflow.p2p.completed.v1
omniflow.inventory.movement.v1
omniflow.inventory.movement.v1.dlq
omniflow.inventory.fact_inventory_movement
omniflow.inventory.fact_inventory_snapshot"

ACTUAL_TOPICS="$(timeout 60s docker compose exec -T kafka \
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
echo "all $(printf '%s
' "$EXPECTED_TOPICS" | wc -l | tr -d ' ') expected topics present"

wait_init crdb-init
CRDB_CID="$(docker compose ps -q cockroachdb)"
[[ -n "$CRDB_CID" ]] || { fail "could not resolve the cockroachdb container id"; exit 1; }

log "Asserting at least 3 changefeeds are running"
RUNNING="$(crdb "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'")"
echo "running changefeeds: ${RUNNING}"
if [ "${RUNNING:-0}" -lt 3 ]; then
  fail "expected >=3 running changefeeds, found ${RUNNING}"
  exit 1
fi

log "Waiting for the four app services to report READY (not merely started)"
wait_healthy commbot inventory-intelligence p2p-orchestrator viz-gateway

# Connect the SSE consumer BEFORE seeding — the gateway has no backlog.
SSE_OUT="$(tmpfile)"
open_sse "$SSE_OUT"

log "Seeding one event end to end"
SEED_LOG="$(tmpfile)"
KAFKA_BROKERS="localhost:9092" \
CRDB_DSN="postgres://root@localhost:26257/omniflow?sslmode=disable" \
SEED_MODE="${SEED_MODE:-outbox}" \
  go run ./tools/seed | tee "$SEED_LOG"

SEQ_KEY="$(grep '^SEED_SEQUENCE_ENGINE_KEY=' "$SEED_LOG" | cut -d= -f2)"
if [ -z "$SEQ_KEY" ]; then
  fail "seeder did not report a sequence_engine_key"
  exit 1
fi
log "Seeded sequence_engine_key = $SEQ_KEY — waiting for it on the SSE stream"

# The p2p.completed changefeed fires ~1-2s after approval, but the resumed DAG has several nodes to
# execute first and CI runners are slow, so allow real margin rather than flaking at the boundary.
wait_sse "$SSE_OUT" "$SEQ_KEY" 40 || exit 1

log "✓ E2E PASSED — projection $SEQ_KEY reached the viz SSE stream"
echo "---- matching SSE line ----"
grep -- "$SEQ_KEY" "$SSE_OUT" | head -n 1
exit 0
