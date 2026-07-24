#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# CRDB_LICENSE is OPTIONAL (single-node v24.3+ runs changefeeds license-free). We do NOT skip when it
# is unset — the stack boots and any real licensing error surfaces as a hard failure, never a silent pass.

export COMPOSE_PROJECT_NAME="omniflow-failtest-exactly-once"

cleanup() {
    local code=$?
    if [[ "$code" -ne 0 ]]; then
        echo "===== failtest failed (exit $code) — docker compose logs ====="
        docker compose logs --no-color --tail=200 || true
        echo "===== end docker compose logs ====="
    fi
    echo "Tearing down..."
    docker compose down -v
}
trap cleanup EXIT

echo "Booting stack..."
docker compose up -d --build

echo "Waiting for CRDB initialization..."
# crdb-init has restart:no and exits 0 only on full success; wait on its exit code (log-grep is
# fragile — its completion line is "crdb-init complete.", not "Init complete").
# docker compose wait adopts the container's exit code as its own → set -e would abort before we
# can report it. Block, then read the true code via docker inspect.
docker compose wait crdb-init >/dev/null 2>&1 || true
INIT_CID="$(docker compose ps -aq crdb-init)"
INIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$INIT_CID" 2>/dev/null || echo unknown)"
echo "crdb-init exit code: ${INIT_CODE}"
[[ "$INIT_CODE" == "0" ]] || { echo "crdb-init failed (exit $INIT_CODE)"; docker compose logs crdb-init || true; exit 1; }

sleep 5 # Allow seeders and orchestrator to become fully healthy

echo "Seeding workflow (full)..."
SEED_OUT=$(SEED_ACTION=full go run ./tools/seed)
echo "$SEED_OUT"

export SEED_EVENT_ID=$(echo "$SEED_OUT" | grep SEED_EVENT_ID= | cut -d= -f2)
export SEED_SEQUENCE_ENGINE_KEY=$(echo "$SEED_OUT" | grep SEED_SEQUENCE_ENGINE_KEY= | cut -d= -f2)

if [[ -z "$SEED_EVENT_ID" || -z "$SEED_SEQUENCE_ENGINE_KEY" ]]; then
    echo "Failed to extract keys from seed output"
    exit 1
fi

# Wait for completion
sleep 3

echo "Sending duplicate approval..."
SEED_ACTION=approve-only go run ./tools/seed

sleep 5

echo "Checking orchestrator_outbox rows..."
# orchestrator_outbox.aggregate_id is the workflow UUID (wf.ID), NOT the event_id — resolve it via
# the workflows row (same subquery the ledger check uses below).
OUTBOX_COUNT=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id = (SELECT id FROM workflows WHERE event_id='${SEED_EVENT_ID}');" | tail -n 1)

if [[ "$OUTBOX_COUNT" != "2" ]]; then
    echo "Expected exactly 2 rows in outbox (approved + completed), got ${OUTBOX_COUNT}"
    exit 1
fi

LEDGER_COUNT=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM node_execution_ledger WHERE node_id='final_step' AND workflow_id = (SELECT id FROM workflows WHERE event_id='${SEED_EVENT_ID}');" | tail -n 1)

if [[ "$LEDGER_COUNT" != "1" ]]; then
    echo "Expected exactly 1 completed final_step row in ledger, got ${LEDGER_COUNT}"
    exit 1
fi

WORKFLOW_COUNT=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM workflows WHERE event_id='${SEED_EVENT_ID}';" | tail -n 1)

if [[ "$WORKFLOW_COUNT" != "1" ]]; then
    echo "Expected exactly 1 workflow row, got ${WORKFLOW_COUNT}"
    exit 1
fi

echo "Success: Exactly-once delivery semantics passed."
