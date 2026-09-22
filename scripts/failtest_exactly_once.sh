#!/usr/bin/env bash
# scripts/failtest_exactly_once.sh — duplicate-approval suppression.
#
# Runs a workflow to COMPLETED, then replays the SAME HumanApprovalEvent and asserts the orchestrator
# no-ops it: the outbox row count does not move, and its absolute value is exactly what the DAG
# emits. Redelivery is normal in an at-least-once pipeline; a duplicate that re-ran the DAG would
# re-emit downstream events, which is the failure this exists to catch.
export COMPOSE_PROJECT_NAME="omniflow-failtest-exactly-once"
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

# One outbox row per PAYLOAD-BEARING checkpoint, so this tracks the DAG:
#   draft_po       -> PurchaseOrderDrafted (added when the drafting agent became a real node)
#   human_approval -> the approved transition
#   final_step     -> the completion transition
# It was 2 while draft_po was a no-op. Adding another executing node moves this number — intended,
# since a silent change in emitted events is exactly what this is here to catch.
EXPECTED_OUTBOX_ROWS="${EXPECTED_OUTBOX_ROWS:-3}"

boot_stack

log "Seeding workflow (full: seed, wait SUSPENDED, approve)"
SEED_OUT="$(SEED_ACTION=full go run ./tools/seed)"
echo "$SEED_OUT"
seed_keys "$SEED_OUT"

# Wait for the workflow to actually COMPLETE before duplicating the approval. A fixed sleep here was
# a guess, and the wrong one: the duplicate arrived mid-flight, the assertion read a half-finished
# workflow, and the test never exercised its own premise — a duplicate arriving AFTER completion.
log "Waiting for the workflow to reach COMPLETED"
wait_state "$SEED_EVENT_ID" COMPLETED 150 || exit 1

# orchestrator_outbox.aggregate_id is the workflow UUID (wf.ID), NOT the event_id — resolve it via
# the workflows row. The `::STRING` cast is required: aggregate_id is declared STRING (it doubles as
# the Kafka partition key) while workflows.id is UUID, and CockroachDB will not silently compare them.
OUTBOX_QUERY="SELECT count(*) FROM orchestrator_outbox AS OF SYSTEM TIME '-30s' WHERE aggregate_id = (SELECT id::STRING FROM workflows WHERE event_id='${SEED_EVENT_ID}');"

# Baseline against a settled workflow, so the duplicate is measured against completion.
OUTBOX_BEFORE="$(crdb "$OUTBOX_QUERY")"
echo "outbox rows before the duplicate approval: ${OUTBOX_BEFORE}"

log "Sending the duplicate approval"
SEED_ACTION=approve-only go run ./tools/seed

# This is a NEGATIVE assertion — "nothing happened" — so it needs a window in which the wrong
# behaviour would have shown up. If the orchestrator were going to re-run the DAG, extra rows would
# appear within this window; a poll cannot wait for an event that must not occur.
sleep 10

log "Checking orchestrator_outbox rows"
OUTBOX_COUNT="$(crdb "$OUTBOX_QUERY")"

# Two assertions, because they fail for different reasons and the distinction is diagnostic:
#   (a) the duplicate added nothing         -> suppression actually happened
#   (b) the absolute count is the expected  -> the DAG emitted the expected outbox rows
# Without (a) a passing (b) could hide a duplicate that replaced rather than added.
if [[ "$OUTBOX_COUNT" != "$OUTBOX_BEFORE" ]]; then
    fail "duplicate approval was NOT suppressed: outbox went from ${OUTBOX_BEFORE} to ${OUTBOX_COUNT} rows"
    exit 1
fi
if [[ "$OUTBOX_COUNT" != "$EXPECTED_OUTBOX_ROWS" ]]; then
    fail "expected exactly ${EXPECTED_OUTBOX_ROWS} outbox rows (draft_po + approved + completed), got '${OUTBOX_COUNT}'"
    exit 1
fi

LEDGER_COUNT="$(crdb "SELECT count(*) FROM node_execution_ledger AS OF SYSTEM TIME '-30s' WHERE node_id='final_step' AND workflow_id = (SELECT id FROM workflows WHERE event_id='${SEED_EVENT_ID}');")"
if [[ "$LEDGER_COUNT" != "1" ]]; then
    fail "expected exactly 1 completed final_step row in the ledger, got '${LEDGER_COUNT}'"
    exit 1
fi

WORKFLOW_COUNT="$(crdb "SELECT count(*) FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${SEED_EVENT_ID}';")"
if [[ "$WORKFLOW_COUNT" != "1" ]]; then
    fail "expected exactly 1 workflow row, got '${WORKFLOW_COUNT}'"
    exit 1
fi

log "✓ PASSED — the duplicate approval was suppressed; outbox stayed at ${OUTBOX_COUNT} rows"
