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
# kafka-init creates every topic — including omniflow.p2p.approval.v1, which the approval replay
# below produces to, and the .dlq sinks. franz-go never requests auto-topic-creation, so the
# broker's KAFKA_AUTO_CREATE_TOPICS_ENABLE does nothing for our Go clients. Check it first:
# crdb-init gates on kafka-init, so a topic failure would otherwise surface as a confusing
# crdb-init stall rather than naming itself.
docker compose wait kafka-init >/dev/null 2>&1 || true
KINIT_CID="$(docker compose ps -aq kafka-init)"
KINIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$KINIT_CID" 2>/dev/null || echo unknown)"
echo "kafka-init exit code: ${KINIT_CODE}"
[[ "$KINIT_CODE" == "0" ]] || { echo "kafka-init failed (exit $KINIT_CODE)"; docker compose logs kafka-init || true; exit 1; }

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

# Wait for the workflow to actually COMPLETE before duplicating the approval.
#
# `sleep 3` was not a wait, it was a guess — and the wrong one. The DAG needs longer than that on a CI
# runner, so the duplicate approval was arriving mid-flight and the assertion below read a
# half-finished workflow (1 outbox row instead of 2). Worse, it meant the test never actually
# exercised what it claims: a duplicate approval arriving AFTER completion. Poll for the terminal
# state so the duplicate is genuinely a duplicate.
echo "Waiting for the workflow to reach COMPLETED..."
timeout 90s bash -c 'until [[ "$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT state FROM workflows WHERE event_id='"'"'"'"${SEED_EVENT_ID}"'"'"'"';" | tail -n 1)" == "COMPLETED" ]]; do sleep 2; done' || {
    STATE=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT state FROM workflows WHERE event_id='${SEED_EVENT_ID}';" | tail -n 1)
    echo "FAIL: workflow never reached COMPLETED before the duplicate approval (last state: ${STATE})"
    exit 1
}

# Record the post-completion baseline, so the duplicate is measured against a settled workflow.
OUTBOX_BEFORE=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id = (SELECT id::STRING FROM workflows WHERE event_id='${SEED_EVENT_ID}');" | tail -n 1)
echo "Outbox rows before duplicate approval: ${OUTBOX_BEFORE}"

echo "Sending duplicate approval..."
SEED_ACTION=approve-only go run ./tools/seed

# Give the orchestrator time to consume and (correctly) suppress the duplicate. If it were going to
# wrongly re-run the DAG, this is the window in which extra rows would appear.
sleep 10

echo "Checking orchestrator_outbox rows..."
# orchestrator_outbox.aggregate_id is the workflow UUID (wf.ID), NOT the event_id — resolve it via
# the workflows row (same subquery the ledger check uses below).
# The `::STRING` cast is required, not cosmetic: aggregate_id is declared STRING (it doubles as the
# Kafka partition key) while workflows.id is UUID, and CockroachDB will not silently compare the two.
# The ledger check below needs no cast — node_execution_ledger.workflow_id is itself UUID.
OUTBOX_COUNT=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id = (SELECT id::STRING FROM workflows WHERE event_id='${SEED_EVENT_ID}');" | tail -n 1)

# Two assertions, because they fail for different reasons and the distinction is diagnostic:
#   (a) the duplicate added nothing  -> suppression actually happened
#   (b) the absolute count is 2      -> the DAG emitted the expected outbox rows (two payload-bearing
#                                       checkpoints: the approved transition and the final one)
# Without (a) a passing (b) could still hide a duplicate that replaced rather than added.
if [[ "$OUTBOX_COUNT" != "$OUTBOX_BEFORE" ]]; then
    echo "Duplicate approval was NOT suppressed: outbox went from ${OUTBOX_BEFORE} to ${OUTBOX_COUNT} rows"
    exit 1
fi

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
