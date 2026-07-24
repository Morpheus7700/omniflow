#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# CRDB_LICENSE is OPTIONAL (single-node v24.3+ runs changefeeds license-free). We do NOT skip when it
# is unset — the stack boots and any real licensing error surfaces as a hard failure, never a silent pass.

export COMPOSE_PROJECT_NAME="omniflow-failtest-pod"
SSE_PID=""  # so the EXIT trap is safe under set -u even if we fail before opening the SSE stream

cleanup() {
    local code=$?
    [[ -n "$SSE_PID" ]] && kill "$SSE_PID" 2>/dev/null || true
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
INIT_CODE="$(docker compose wait crdb-init)"
[[ "$INIT_CODE" == "0" ]] || { echo "crdb-init failed (exit $INIT_CODE)"; exit 1; }

sleep 5 # Allow seeders and orchestrator to become fully healthy

echo "Seeding workflow (suspend-only)..."
SEED_OUT=$(SEED_ACTION=suspend-only go run ./tools/seed)
echo "$SEED_OUT"

export SEED_EVENT_ID=$(echo "$SEED_OUT" | grep SEED_EVENT_ID= | cut -d= -f2)
export SEED_SEQUENCE_ENGINE_KEY=$(echo "$SEED_OUT" | grep SEED_SEQUENCE_ENGINE_KEY= | cut -d= -f2)

if [[ -z "$SEED_EVENT_ID" || -z "$SEED_SEQUENCE_ENGINE_KEY" ]]; then
    echo "Failed to extract keys from seed output"
    exit 1
fi

echo "Verifying workflow suspended..."
docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow -e "SELECT state FROM workflows WHERE event_id = '${SEED_EVENT_ID}';" | grep "SUSPENDED"

echo "Killing orchestrator..."
docker compose kill p2p-orchestrator
echo "Restarting orchestrator..."
docker compose up -d p2p-orchestrator

sleep 5

echo "Listening to SSE in background..."
curl -s -N http://localhost:8081/api/stream > sse_out.log &
SSE_PID=$!
sleep 2

echo "Approving workflow..."
SEED_ACTION=approve-only go run ./tools/seed

echo "Waiting for SSE event with sequence key ${SEED_SEQUENCE_ENGINE_KEY}..."
timeout 20s bash -c 'until grep -q "'"${SEED_SEQUENCE_ENGINE_KEY}"'" sse_out.log; do sleep 1; done'
echo "SSE event received!"

kill $SSE_PID || true

echo "Checking ledger exactly-once state..."
LEDGER_COUNT=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM node_execution_ledger WHERE node_id='final_step' AND workflow_id = (SELECT id FROM workflows WHERE event_id='${SEED_EVENT_ID}');" | tail -n 1)

if [[ "$LEDGER_COUNT" != "1" ]]; then
    echo "Expected 1 completed final_step row in ledger, got ${LEDGER_COUNT}"
    exit 1
fi

STATE=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT state FROM workflows WHERE event_id='${SEED_EVENT_ID}';" | tail -n 1)
if [[ "$STATE" != "COMPLETED" ]]; then
    echo "Expected workflow state COMPLETED, got ${STATE}"
    exit 1
fi

echo "Success: Pod survival test passed."
