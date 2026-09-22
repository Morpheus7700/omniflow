#!/usr/bin/env bash
# scripts/failtest_killed_pod.sh — durable checkpoint resume.
#
# Seeds a workflow to its human-approval SUSPEND, kills the orchestrator, restarts it, approves, and
# asserts the resumed run executes `final_step` EXACTLY once and the workflow reaches COMPLETED.
# This is the proof for the durable checkpointer: a pod that dies mid-workflow resumes its exact
# state on restart rather than restarting from zero or losing the workflow.
export COMPOSE_PROJECT_NAME="omniflow-failtest-pod"
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

boot_stack

log "Seeding workflow (suspend-only)"
SEED_OUT="$(SEED_ACTION=suspend-only go run ./tools/seed)"
echo "$SEED_OUT"
seed_keys "$SEED_OUT"

log "Verifying the workflow suspended before the kill"
wait_state "$SEED_EVENT_ID" SUSPENDED 90 || exit 1

log "Killing the orchestrator"
docker compose kill p2p-orchestrator
log "Restarting the orchestrator"
docker compose up -d p2p-orchestrator
wait_healthy p2p-orchestrator

SSE_OUT="$(tmpfile)"
open_sse "$SSE_OUT"

log "Approving the workflow"
SEED_ACTION=approve-only go run ./tools/seed

# After approval the restarted orchestrator must re-acquire the lease and execute the remaining DAG
# nodes before the outbox row exists.
log "Waiting for the SSE event with sequence key ${SEED_SEQUENCE_ENGINE_KEY}"
wait_sse "$SSE_OUT" "$SEED_SEQUENCE_ENGINE_KEY" 40 || exit 1
echo "SSE event received"

# The SSE event proves the workflow RESUMED, not that it FINISHED — it is emitted by the first
# payload-bearing checkpoint while the DAG still has nodes to run. Wait for the terminal state.
log "Waiting for the workflow to reach COMPLETED"
wait_state "$SEED_EVENT_ID" COMPLETED 150 || exit 1

# NOW the exactly-once assertion is meaningful: the killed-and-restarted orchestrator resumed from its
# durable checkpoint and executed final_step EXACTLY once, not zero times and not twice.
log "Checking the ledger for exactly one final_step execution"
LEDGER_COUNT="$(crdb "SELECT count(*) FROM node_execution_ledger AS OF SYSTEM TIME '-30s' WHERE node_id='final_step' AND workflow_id = (SELECT id FROM workflows WHERE event_id='${SEED_EVENT_ID}');")"
if [[ "$LEDGER_COUNT" != "1" ]]; then
    fail "expected 1 completed final_step row in the ledger, got '${LEDGER_COUNT}'"
    exit 1
fi

log "✓ PASSED — the orchestrator resumed from its durable checkpoint and executed final_step exactly once"
