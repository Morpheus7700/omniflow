#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Proves the central claim of the agent layer: node execution is at-least-once, but the agent's
# SIDE EFFECT is exactly-once.
#
# The drafting agent calls a model OUTSIDE the workflow transaction — it must, because holding a row
# lock across a multi-second LLM call would serialise every other worker behind a third party. That
# makes a duplicate CALL unavoidable when a pod dies between the call and the checkpoint. We do not
# try to prevent it. Instead purchase_orders carries a deterministic key (workflow, node, attempt)
# and the insert is ON CONFLICT DO NOTHING, so N executions yield exactly ONE purchase order.
#
# This test kills the orchestrator mid-workflow, lets it resume, and asserts:
#   1. exactly one purchase_orders row exists for the workflow            (no duplicate PO)
#   2. its po_number is the deterministic one                             (the key really is stable)
#   3. the workflow still reaches COMPLETED                               (resume actually works)
#   4. agent_decisions holds at least one accepted row                    (the trail survived)
#   5. the PO's total equals the sum of its lines                         (validation was enforced)
#
# Assertion 1 is the one that would fail if the idempotency key were dropped, and it is the reason
# this script exists rather than trusting the unit tests.

export COMPOSE_PROJECT_NAME="omniflow-failtest-agent"

CRDB_CID=""

# See failtest_killed_pod.sh for the full history behind this shape. Short version: every call needs
# its OWN timeout (a deadline loop only re-checks between iterations, so one stuck call pins it
# forever); use plain `docker exec` on a pre-resolved id (compose exec burned 20s per call); and
# `tail -n +2` must strip the CSV header BEFORE taking the last row, or a zero-row result returns the
# header text and reads as data.
crdb() {
    [[ -n "$CRDB_CID" ]] || return 0
    timeout 20s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv -e "$1" 2>/dev/null \
      | tail -n +2 | tail -n 1 | tr -d '[:space:]' || true
}

cleanup() {
    local code=$?
    if [[ "$code" -ne 0 ]]; then
        echo "===== agent failtest failed (exit $code) — logs ====="
        docker compose logs --no-color --tail=200 p2p-orchestrator mock-llm || true
        echo "===== end logs ====="
    fi
    echo "Tearing down..."
    docker compose down -v
}
trap cleanup EXIT

echo "Booting stack..."
docker compose up -d --build

echo "Waiting for kafka-init..."
docker compose wait kafka-init >/dev/null 2>&1 || true
KINIT_CID="$(docker compose ps -aq kafka-init)"
KINIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$KINIT_CID" 2>/dev/null || echo unknown)"
echo "kafka-init exit code: ${KINIT_CODE}"
[[ "$KINIT_CODE" == "0" ]] || { echo "kafka-init failed"; docker compose logs kafka-init || true; exit 1; }

echo "Waiting for crdb-init..."
docker compose wait crdb-init >/dev/null 2>&1 || true
INIT_CID="$(docker compose ps -aq crdb-init)"
INIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$INIT_CID" 2>/dev/null || echo unknown)"
echo "crdb-init exit code: ${INIT_CODE}"

CRDB_CID="$(docker compose ps -q cockroachdb)"
[[ -n "$CRDB_CID" ]] || { echo "FAIL: could not resolve the cockroachdb container id"; docker compose ps || true; exit 1; }
echo "cockroachdb container: ${CRDB_CID:0:12}"
[[ "$INIT_CODE" == "0" ]] || { echo "crdb-init failed"; docker compose logs crdb-init || true; exit 1; }

sleep 5

echo "Seeding workflow (suspend-only) — the drafting agent runs before the approval gate..."
SEED_OUT=$(SEED_ACTION=suspend-only go run ./tools/seed)
echo "$SEED_OUT"

export SEED_EVENT_ID=$(echo "$SEED_OUT" | grep SEED_EVENT_ID= | cut -d= -f2)
[[ -n "$SEED_EVENT_ID" ]] || { echo "FAIL: no SEED_EVENT_ID in seed output"; exit 1; }

# The workflow suspends at human_approval, which is AFTER draft_po — so reaching SUSPENDED proves
# the agent node already ran and produced its purchase order.
echo "Waiting for the workflow to suspend (i.e. for draft_po to have executed)..."
DEADLINE=$(( SECONDS + 120 ))
STATE=""
while (( SECONDS < DEADLINE )); do
    STATE=$(crdb "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id = '${SEED_EVENT_ID}';")
    [[ "$STATE" == "SUSPENDED" ]] && break
    sleep 2
done
[[ "$STATE" == "SUSPENDED" ]] || {
    echo "FAIL: expected SUSPENDED, got '${STATE}'"
    docker compose logs --no-color --tail=100 p2p-orchestrator mock-llm || true
    exit 1
}

WF_ID=$(crdb "SELECT id FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id = '${SEED_EVENT_ID}';")
echo "workflow id: ${WF_ID}"

PO_BEFORE=$(crdb "SELECT count(*) FROM purchase_orders AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}';")
echo "purchase orders after the first execution: ${PO_BEFORE}"
[[ "$PO_BEFORE" == "1" ]] || { echo "FAIL: expected exactly 1 purchase order after draft_po, got '${PO_BEFORE}'"; exit 1; }

# Kill and restart. On resume the orchestrator re-reads the workflow, and because the node ledger
# already records draft_po it must NOT issue a second purchase order.
echo "Killing orchestrator mid-workflow..."
docker compose kill p2p-orchestrator
echo "Restarting orchestrator..."
docker compose up -d p2p-orchestrator
sleep 8

echo "Approving to resume the DAG..."
SEED_ACTION=approve-only go run ./tools/seed

echo "Waiting for the workflow to reach COMPLETED..."
DEADLINE=$(( SECONDS + 180 ))
FINAL=""
while (( SECONDS < DEADLINE )); do
    FINAL=$(crdb "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id = '${SEED_EVENT_ID}';")
    [[ "$FINAL" == "COMPLETED" ]] && break
    sleep 3
done
[[ "$FINAL" == "COMPLETED" ]] || {
    echo "FAIL: workflow did not complete after resume, state='${FINAL}'"
    docker compose logs --no-color --tail=150 p2p-orchestrator || true
    exit 1
}

# ---- THE ASSERTION THIS TEST EXISTS FOR ----
PO_AFTER=$(crdb "SELECT count(*) FROM purchase_orders AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}';")
echo "purchase orders after kill + resume + completion: ${PO_AFTER}"
if [[ "$PO_AFTER" != "1" ]]; then
    echo "FAIL: expected exactly 1 purchase order across the whole workflow, got '${PO_AFTER}'"
    echo "      A second row means the idempotency key on purchase_orders is not suppressing"
    echo "      re-execution — the agent issued a duplicate PO, which is real money."
    crdb "SELECT po_number, attempt, total_amount FROM purchase_orders WHERE workflow_id='${WF_ID}';" || true
    exit 1
fi

PO_NUMBER=$(crdb "SELECT po_number FROM purchase_orders AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}';")
EXPECTED_PO="PO-${WF_ID:0:8}-1"
echo "po_number: ${PO_NUMBER} (expected ${EXPECTED_PO})"
[[ "$PO_NUMBER" == "$EXPECTED_PO" ]] || {
    echo "FAIL: po_number is not the deterministic one; a non-deterministic number cannot be"
    echo "      deduplicated by the unique constraint."
    exit 1
}

# The governance trail must have survived the crash, and must record an accepted decision.
ACCEPTED=$(crdb "SELECT count(*) FROM agent_decisions AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}' AND outcome = 'accepted';")
echo "accepted agent decisions: ${ACCEPTED}"
[[ "${ACCEPTED:-0}" -ge 1 ]] || { echo "FAIL: no accepted agent_decisions row for this workflow"; exit 1; }

# Validation actually ran: the persisted total equals the sum of the line extended amounts. Compared
# numerically IN SQL — string comparison of DECIMAL is a known trap in this repo (2.00 vs 2.0000).
MISMATCH=$(crdb "SELECT count(*) FROM purchase_orders AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}' AND total_amount != (SELECT coalesce(sum((l->>'extended_amount')::DECIMAL), 0) FROM jsonb_array_elements(lines) AS l);")
echo "purchase orders whose total disagrees with their lines: ${MISMATCH}"
[[ "${MISMATCH:-1}" == "0" ]] || { echo "FAIL: a persisted PO's total does not equal the sum of its lines"; exit 1; }

echo
echo "PASS: exactly one purchase order across a kill and resume."
echo "      at-least-once execution, exactly-once effect."
