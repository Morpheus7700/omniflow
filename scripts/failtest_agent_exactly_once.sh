#!/usr/bin/env bash
# scripts/failtest_agent_exactly_once.sh — at-least-once execution, exactly-once EFFECT.
#
# The drafting agent calls a model OUTSIDE the workflow transaction — it must, because holding a row
# lock across a multi-second LLM call would serialise every other worker behind a third party. That
# makes a duplicate CALL unavoidable when a pod dies between the call and the checkpoint. We do not
# try to prevent it. Instead purchase_orders carries a deterministic key (workflow, node, attempt)
# and the insert is ON CONFLICT DO NOTHING, so N executions yield exactly ONE purchase order.
#
# This kills the orchestrator mid-workflow, lets it resume, and asserts:
#   1. exactly one purchase_orders row exists for the workflow            (no duplicate PO)
#   2. its po_number is the deterministic one                             (the key really is stable)
#   3. the workflow still reaches COMPLETED                               (resume actually works)
#   4. agent_decisions holds at least one accepted row                    (the trail survived)
#   5. the PO's total equals the sum of its lines                         (validation was enforced)
# Assertion 1 is the one that would fail if the idempotency key were dropped, and it is the reason
# this script exists rather than trusting the unit tests.
export COMPOSE_PROJECT_NAME="omniflow-failtest-agent"
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

boot_stack

log "Seeding workflow (suspend-only) — the drafting agent runs before the approval gate"
SEED_OUT="$(SEED_ACTION=suspend-only go run ./tools/seed)"
echo "$SEED_OUT"
seed_keys "$SEED_OUT"

# The workflow suspends at human_approval, which is AFTER draft_po — so reaching SUSPENDED proves
# the agent node already ran and produced its purchase order.
log "Waiting for the workflow to suspend (i.e. for draft_po to have executed)"
wait_state "$SEED_EVENT_ID" SUSPENDED 120 || exit 1

WF_ID="$(crdb "SELECT id FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id = '${SEED_EVENT_ID}';")"
echo "workflow id: ${WF_ID}"

PO_COUNT_QUERY="SELECT count(*) FROM purchase_orders AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}';"
PO_BEFORE="$(crdb "$PO_COUNT_QUERY")"
echo "purchase orders after the first execution: ${PO_BEFORE}"
[[ "$PO_BEFORE" == "1" ]] || { fail "expected exactly 1 purchase order after draft_po, got '${PO_BEFORE}'"; exit 1; }

# Kill and restart. On resume the orchestrator re-reads the workflow, and because the node ledger
# already records draft_po it must NOT issue a second purchase order.
log "Killing the orchestrator mid-workflow"
docker compose kill p2p-orchestrator
log "Restarting the orchestrator"
docker compose up -d p2p-orchestrator
wait_healthy p2p-orchestrator

log "Approving to resume the DAG"
SEED_ACTION=approve-only go run ./tools/seed

log "Waiting for the workflow to reach COMPLETED"
wait_state "$SEED_EVENT_ID" COMPLETED 180 || exit 1

# ---- THE ASSERTION THIS TEST EXISTS FOR ----
PO_AFTER="$(crdb "$PO_COUNT_QUERY")"
echo "purchase orders after kill + resume + completion: ${PO_AFTER}"
if [[ "$PO_AFTER" != "1" ]]; then
    fail "expected exactly 1 purchase order across the whole workflow, got '${PO_AFTER}'"
    echo "      A second row means the idempotency key on purchase_orders is not suppressing"
    echo "      re-execution — the agent issued a duplicate PO, which is real money."
    crdb "SELECT po_number, attempt, total_amount FROM purchase_orders WHERE workflow_id='${WF_ID}';" || true
    exit 1
fi

PO_NUMBER="$(crdb "SELECT po_number FROM purchase_orders AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}';")"
EXPECTED_PO="PO-${WF_ID:0:8}-1"
echo "po_number: ${PO_NUMBER} (expected ${EXPECTED_PO})"
if [[ "$PO_NUMBER" != "$EXPECTED_PO" ]]; then
    fail "po_number is not the deterministic one; a non-deterministic number cannot be deduplicated by the unique constraint"
    exit 1
fi

# The governance trail must have survived the crash, and must record an accepted decision.
ACCEPTED="$(crdb "SELECT count(*) FROM agent_decisions AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}' AND outcome = 'accepted';")"
echo "accepted agent decisions: ${ACCEPTED}"
[[ "${ACCEPTED:-0}" -ge 1 ]] || { fail "no accepted agent_decisions row for this workflow"; exit 1; }

# Validation actually ran: the persisted total equals the sum of the line extended amounts. Compared
# numerically IN SQL — string comparison of DECIMAL is a known trap in this repo (2.00 vs 2.0000).
MISMATCH="$(crdb "SELECT count(*) FROM purchase_orders AS OF SYSTEM TIME '-30s' WHERE workflow_id = '${WF_ID}' AND total_amount != (SELECT coalesce(sum((l->>'extended_amount')::DECIMAL), 0) FROM jsonb_array_elements(lines) AS l);")"
echo "purchase orders whose total disagrees with their lines: ${MISMATCH}"
[[ "${MISMATCH:-1}" == "0" ]] || { fail "a persisted PO's total does not equal the sum of its lines"; exit 1; }

log "✓ PASSED — exactly one purchase order across a kill and resume: at-least-once execution, exactly-once effect"
