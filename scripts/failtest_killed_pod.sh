#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# CRDB_LICENSE is OPTIONAL (single-node v24.3+ runs changefeeds license-free). We do NOT skip when it
# is unset — the stack boots and any real licensing error surfaces as a hard failure, never a silent pass.

export COMPOSE_PROJECT_NAME="omniflow-failtest-pod"

# Every CockroachDB query goes through crdb(), and BOTH the per-call `timeout` and the pre-resolved
# container id matter. History, because this took three attempts:
#   1. `docker compose exec` blocked indefinitely — the job twice sat at its 25-minute cap (which
#      GitHub reports as "cancelled", NOT "failure") leaving docker/tail/tr as orphan processes.
#   2. A deadline loop did not rescue it: a deadline is only re-checked BETWEEN iterations, so one
#      stuck call pins the loop forever. Each call needs its own timeout.
#   3. With timeouts in place the hang became measurable — every compose-exec call burned its full
#      20s (poll window was exactly 5*(20+2)s), while cockroachdb stayed healthy and the orchestrator
#      logged nothing. So the fault is the compose exec path, not the database.
# Hence: resolve the container id once and use plain `docker exec`, which skips compose's project and
# state resolution entirely.
# Returns the last CSV cell, whitespace/CR stripped; empty string on timeout or error.
CRDB_CID=""   # assigned once the stack is up, after the crdb-init gate

# `tail -n +2` drops the CSV header FIRST. Without it a zero-row result returns the header text (e.g.
# the literal "state"), which reads as data and produced the misleading failure
# "expected SUSPENDED ... got 'state'". Header-first, then last row: no rows now yields "".
crdb() {
    [[ -n "$CRDB_CID" ]] || return 0
    timeout 20s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv -e "$1" 2>/dev/null \
      | tail -n +2 | tail -n 1 | tr -d '[:space:]' || true
}

# Called only on failure. crdb() swallows stderr to keep the poll quiet, which makes an erroring query
# indistinguishable from one returning no rows — both come back as "". These probes separate the
# possibilities: container state, then bare exec plumbing, then the query with stderr visible.
crdb_diag() {
    echo "---- docker compose ps ----"
    timeout 30s docker compose ps || echo "(compose ps failed/timed out)"
    echo "---- bare exec plumbing check ----"
    timeout 15s docker exec "$CRDB_CID" echo alive || echo "(docker exec failed/timed out: $?)"
    # The AOST form is what the polls actually run, so its stderr is the one that matters. An earlier
    # diag only showed the plain query, which merely re-confirmed the (expected) intent block while
    # leaving the polls' fast-empty results unexplained.
    echo "---- AOST query (the form the polls use) with stderr visible ----"
    timeout 20s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv \
        -e "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${SEED_EVENT_ID}';" || echo "(exit $?)"
    echo "---- plain query with stderr visible (expected to block on write intents) ----"
    timeout 20s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv -e "$1" || echo "(exit $?)"
}

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
# kafka-init creates every topic — including omniflow.p2p.approval.v1, which this test's
# approve-only seed produces to, and the .dlq sinks. franz-go never requests auto-topic-creation,
# so the broker's KAFKA_AUTO_CREATE_TOPICS_ENABLE does nothing for our Go clients. Check it first:
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

# Resolve the CockroachDB container id ONCE, now that the stack is up. Every later query uses plain
# `docker exec` against this id, keeping compose out of the polling hot path.
CRDB_CID="$(docker compose ps -q cockroachdb)"
if [[ -z "$CRDB_CID" ]]; then
    echo "FAIL: could not resolve the cockroachdb container id"
    docker compose ps || true
    exit 1
fi
echo "cockroachdb container: ${CRDB_CID:0:12}"
[[ "$INIT_CODE" == "0" ]] || { echo "crdb-init failed (exit $INIT_CODE)"; docker compose logs crdb-init || true; exit 1; }

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

# Poll, don't read once. The reads are historical (AS OF SYSTEM TIME '-30s') to avoid blocking on the
# orchestrator's write intents, which means a row created moments ago is not visible yet — the seeder
# reports SUSPENDED well before a -30s read can see it. Polling absorbs that lag; a one-shot
# read raced it and reported an empty result.
echo "Verifying workflow suspended..."
DEADLINE=$(( SECONDS + 90 ))
SUSPENDED_STATE=""
while (( SECONDS < DEADLINE )); do
    SUSPENDED_STATE=$(crdb "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id = '${SEED_EVENT_ID}';")
    [[ "$SUSPENDED_STATE" == "SUSPENDED" ]] && break
    sleep 2
done
if [[ "$SUSPENDED_STATE" != "SUSPENDED" ]]; then
    echo "FAIL: expected workflow state SUSPENDED before the kill, got '${SUSPENDED_STATE}'"
    crdb_diag "SELECT event_id, state FROM workflows WHERE event_id='${SEED_EVENT_ID}';"
    exit 1
fi

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
# 40s, not 20s: after approval the restarted orchestrator must re-acquire the lease and execute the
# remaining DAG nodes before the outbox row exists, and CI runners are slow. Also report WHY on
# failure — a bare `timeout` under `set -e` exits 124 with no message, which is indistinguishable
# from a crash and cost a debugging round earlier.
timeout 40s bash -c 'until grep -q "'"${SEED_SEQUENCE_ENGINE_KEY}"'" sse_out.log; do sleep 1; done' || {
    echo "FAIL: sequence key ${SEED_SEQUENCE_ENGINE_KEY} never reached the SSE stream after resume"
    echo "---- captured SSE output ----"; cat sse_out.log || true
    exit 1
}
echo "SSE event received!"

kill $SSE_PID || true

# The SSE event proves the workflow RESUMED, not that it FINISHED. It is emitted by the first
# payload-bearing checkpoint; the DAG still has nodes to execute before `final_step` lands. Asserting
# terminal state the instant that event arrives is a race — it read `final_step = 0` and failed while
# the orchestrator was mid-DAG. Wait for the terminal state explicitly.
# Plain deadline loop, deliberately NOT `timeout 90s bash -c '…'`.
# This query needs a quoted SQL literal (event_id='…'), and smuggling that through a single-quoted
# `bash -c` requires '"'"' escaping that silently misbehaved: the loop never expired and the job hung
# to its 25-minute cap (which GitHub reports as "cancelled", not "failure" — see 05-gotchas).
# Here the SQL's single quotes sit harmlessly inside double quotes, and $SEED_EVENT_ID expands
# normally. The FIFO test's `timeout bash -c` form is fine only because its query needs no quotes.
echo "Waiting for the workflow to reach COMPLETED..."
DEADLINE=$(( SECONDS + 150 ))
STATE=""
while (( SECONDS < DEADLINE )); do
    STATE=$(crdb "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${SEED_EVENT_ID}';")
    [[ "$STATE" == "COMPLETED" ]] && break
    sleep 2
done
if [[ "$STATE" != "COMPLETED" ]]; then
    echo "FAIL: workflow never reached COMPLETED after resume (last state: '${STATE}')"
    crdb_diag "SELECT event_id, state FROM workflows WHERE event_id='${SEED_EVENT_ID}';"
    exit 1
fi

# NOW the exactly-once assertion is meaningful: the killed-and-restarted orchestrator resumed from its
# durable checkpoint and executed final_step EXACTLY once, not zero times and not twice.
echo "Checking ledger exactly-once state..."
LEDGER_COUNT=$(crdb "SELECT count(*) FROM node_execution_ledger AS OF SYSTEM TIME '-30s' WHERE node_id='final_step' AND workflow_id = (SELECT id FROM workflows WHERE event_id='${SEED_EVENT_ID}');")

if [[ "$LEDGER_COUNT" != "1" ]]; then
    echo "Expected 1 completed final_step row in ledger, got ${LEDGER_COUNT}"
    exit 1
fi

echo "Success: Pod survival test passed."
