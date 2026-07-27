#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# CRDB_LICENSE is OPTIONAL (single-node v24.3+ runs changefeeds license-free). We do NOT skip when it
# is unset — the stack boots and any real licensing error surfaces as a hard failure, never a silent pass.

export COMPOSE_PROJECT_NAME="omniflow-failtest-exactly-once"

# Every CockroachDB query goes through this wrapper — see the identical note in failtest_killed_pod.sh.
# `docker compose exec` can block indefinitely (observed twice: the job pinned at its 25-minute cap
# with docker/tail/tr surviving as orphan processes), and a deadline loop cannot rescue a stuck call
# because it only re-checks the clock between iterations. Bounding each call is the fix.
# Returns the last CSV cell, whitespace/CR stripped; empty string on timeout or error.
# Resolved once, after the stack is up (see CRDB_CID assignment below).
CRDB_CID=""

# Uses plain `docker exec` on a pre-resolved container id rather than `docker compose exec`.
# Measured: every `docker compose exec` call here was hitting its 20s timeout — the poll window was
# exactly 5*(20s timeout + 2s sleep) = 110s against a 90s deadline, in two separate jobs. cockroachdb
# was healthy throughout and the orchestrator logged no errors, so the hang is in the compose exec
# path itself, not the database. `docker exec` skips compose's project/state resolution.
# `tail -n +2` drops the CSV header FIRST. Without it a zero-row result returns the header text (e.g.
# the literal "state"), which reads as data. Header-first, then last row: no rows now yields "".
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
    # Discriminator: is the SQL LAYER wedged, or only this row? `echo alive` above proves only that
    # the container runs. A trivial table-free query proves SQL itself still answers.
    echo "---- SELECT 1 (is the SQL layer alive at all?) ----"
    timeout 15s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv -e "SELECT 1;" || echo "(exit $?)"
    echo "---- open transactions (a long-lived one would block reads of its row) ----"
    timeout 15s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv \
        -e "SELECT id, application_name, start, num_stmts FROM [SHOW CLUSTER TRANSACTIONS];" || echo "(exit $?)"
    echo "---- a DIFFERENT table (is the block row-specific?) ----"
    timeout 15s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv \
        -e "SELECT count(*) FROM orchestrator_outbox AS OF SYSTEM TIME '-30s';" || echo "(exit $?)"
    echo "---- AOST query (the form the polls use) with stderr visible ----"
    timeout 20s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv \
        -e "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${SEED_EVENT_ID}';" || echo "(exit $?)"
    echo "---- plain query with stderr visible (expected to block on write intents) ----"
    timeout 20s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv -e "$1" || echo "(exit $?)"
}

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
# Deadline loop over the bounded crdb() helper. Two earlier attempts at this wait both hung the job to
# its 25-minute cap: first `timeout 90s bash -c 'until …'` (the '"'"' escaping needed to smuggle a
# quoted SQL literal through a single-quoted bash -c silently never expired), then a plain deadline
# loop around an UNBOUNDED docker exec (a deadline only re-checks between iterations, so one stuck call
# pins it forever). It takes both: a per-call timeout AND an overall deadline.
echo "Waiting for the workflow to reach COMPLETED..."
DEADLINE=$(( SECONDS + 150 ))
STATE=""
while (( SECONDS < DEADLINE )); do
    STATE=$(crdb "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${SEED_EVENT_ID}';")
    [[ "$STATE" == "COMPLETED" ]] && break
    sleep 2
done
if [[ "$STATE" != "COMPLETED" ]]; then
    echo "FAIL: workflow never reached COMPLETED before the duplicate approval (last state: '${STATE}')"
    crdb_diag "SELECT event_id, state FROM workflows WHERE event_id='${SEED_EVENT_ID}';"
    exit 1
fi

# Record the post-completion baseline, so the duplicate is measured against a settled workflow.
OUTBOX_BEFORE=$(crdb "SELECT count(*) FROM orchestrator_outbox AS OF SYSTEM TIME '-30s' WHERE aggregate_id = (SELECT id::STRING FROM workflows WHERE event_id='${SEED_EVENT_ID}');")
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
OUTBOX_COUNT=$(crdb "SELECT count(*) FROM orchestrator_outbox AS OF SYSTEM TIME '-30s' WHERE aggregate_id = (SELECT id::STRING FROM workflows WHERE event_id='${SEED_EVENT_ID}');")

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

LEDGER_COUNT=$(crdb "SELECT count(*) FROM node_execution_ledger AS OF SYSTEM TIME '-30s' WHERE node_id='final_step' AND workflow_id = (SELECT id FROM workflows WHERE event_id='${SEED_EVENT_ID}');")

if [[ "$LEDGER_COUNT" != "1" ]]; then
    echo "Expected exactly 1 completed final_step row in ledger, got ${LEDGER_COUNT}"
    exit 1
fi

WORKFLOW_COUNT=$(crdb "SELECT count(*) FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${SEED_EVENT_ID}';")

if [[ "$WORKFLOW_COUNT" != "1" ]]; then
    echo "Expected exactly 1 workflow row, got ${WORKFLOW_COUNT}"
    exit 1
fi

echo "Success: Exactly-once delivery semantics passed."
