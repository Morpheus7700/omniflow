#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Proves the dead-letter path actually works, which no other test covers.
#
# WHY THIS EXISTS: every consumer withholds its offset commit until DLQ delivery is CONFIRMED
# (see produceToDLQConfirmed — the commit is deliberately downstream of the produce). That ordering is
# load-bearing: it guarantees no message is dropped. But it also means a missing or unwritable .dlq
# topic wedges the partition FOREVER on the first poison message, and every happy-path test stays
# green while it happens. Topics were previously left to broker auto-create, which franz-go never
# requests — so the .dlq topics did not exist and this failure mode was live and invisible.
#
# TWO KINDS of poison are exercised, because they take different code paths:
#   1. WIRE poison    — bytes proto.Unmarshal rejects. Returns at the unmarshal guard.
#   2b. SEMANTIC poison — wire-valid protobuf that protovalidate rejects. This is the ONLY case that
#       reaches validator.Validate. Testing only (1) leaves the validator's terminal-error
#       classification completely unproven, which is how a dependency bump could wedge a partition
#       with every CI job still green.
#
# The decisive assertion is the last one: a VALID message processed AFTER the poison pills. That is
# what distinguishes "poison routed to DLQ and the stream moved on" from "partition wedged".
#
# CRDB_LICENSE is OPTIONAL (single-node v24.3+ runs changefeeds license-free).

export COMPOSE_PROJECT_NAME="omniflow-failtest-dlq"

TOPIC="omniflow.inventory.movement.v1"
DLQ_TOPIC="${TOPIC}.dlq"
GROUP="inventory-intelligence-v1"
KCAT="/opt/kafka/bin"

# Bounded query helper — `docker compose exec` can block indefinitely, and an unbounded call inside a
# poll loop pins the job to its 25-minute cap (which GitHub reports as "cancelled", not "failure").
# Returns the last CSV cell, whitespace/CR stripped; empty on timeout or error.
crdb() {
    timeout 20s docker compose exec -T cockroachdb \
        cockroach sql --insecure -d omniflow --format=csv -e "$1" 2>/dev/null \
      | tail -n 1 | tr -d '[:space:]' || true
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

# kafka-init must succeed first: this test is meaningless if the .dlq topic does not exist, and
# crdb-init gates on kafka-init, so a topic failure would otherwise look like a crdb-init stall.
docker compose wait kafka-init >/dev/null 2>&1 || true
KINIT_CID="$(docker compose ps -aq kafka-init)"
KINIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$KINIT_CID" 2>/dev/null || echo unknown)"
echo "kafka-init exit code: ${KINIT_CODE}"
[[ "$KINIT_CODE" == "0" ]] || { echo "kafka-init failed (exit $KINIT_CODE)"; docker compose logs kafka-init || true; exit 1; }

docker compose wait crdb-init >/dev/null 2>&1 || true
INIT_CID="$(docker compose ps -aq crdb-init)"
INIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$INIT_CID" 2>/dev/null || echo unknown)"
echo "crdb-init exit code: ${INIT_CODE}"
[[ "$INIT_CODE" == "0" ]] || { echo "crdb-init failed (exit $INIT_CODE)"; docker compose logs crdb-init || true; exit 1; }

# Explicitly confirm the DLQ topic exists before asserting anything about it, so a missing topic
# reports itself rather than surfacing as an empty-DLQ assertion failure later.
echo "Confirming ${DLQ_TOPIC} exists..."
timeout 60s docker compose exec -T kafka "${KCAT}/kafka-topics.sh" --bootstrap-server kafka:29092 --list \
  | tr -d '\r' | grep -qx "${DLQ_TOPIC}" \
  || { echo "MISSING TOPIC: ${DLQ_TOPIC}"; exit 1; }

sleep 5 # let the inventory consumer join the group before we hand it a poison pill

# 1. Poison pill. The leading byte 'n' (0x6E) decodes as field 13 / wire type 6 — 6 is not a valid
#    protobuf wire type, so proto.Unmarshal is guaranteed to fail and classify ErrTerminal. (Even if
#    it somehow parsed, protovalidate would reject it — also terminal, also DLQ.)
echo "Producing poison pill to ${TOPIC}..."
printf 'not-a-valid-protobuf\n' \
  | timeout 60s docker compose exec -T kafka "${KCAT}/kafka-console-producer.sh" \
      --bootstrap-server kafka:29092 --topic "${TOPIC}"

# 2. Assert the poison pill was routed to the DLQ.
echo "Waiting for the poison pill to appear on ${DLQ_TOPIC}..."
DLQ_MSG="$(docker compose exec -T kafka "${KCAT}/kafka-console-consumer.sh" \
    --bootstrap-server kafka:29092 --topic "${DLQ_TOPIC}" \
    --from-beginning --max-messages 1 --timeout-ms 45000 2>/dev/null | tr -d '\r' || true)"

if [[ -z "$DLQ_MSG" ]]; then
    echo "FAIL: nothing arrived on ${DLQ_TOPIC} — the poison pill was dropped or the consumer is stuck"
    exit 1
fi
echo "DLQ received: ${DLQ_MSG}"
if [[ "$DLQ_MSG" != *"not-a-valid-protobuf"* ]]; then
    echo "FAIL: DLQ payload is not the original message body (got: ${DLQ_MSG})"
    exit 1
fi

# 2b. THE SEMANTIC POISON PILL — wire-valid protobuf that protovalidate rejects.
#
#     Step 1's payload fails proto.Unmarshal and returns at the consumer's unmarshal guard, so it
#     NEVER reaches validator.Validate. Without this step, "a protovalidate failure is terminal and
#     routes to the DLQ" is an invariant that nothing in CI actually exercises — a dependency bump
#     could change validation-error classification (transient instead of terminal, wedging the
#     partition) and every job would stay green.
#
#     SEED_INV_INVALID sets event_id to a non-UUID. Every other field stays well-formed, so exactly
#     one rule fails — (buf.validate.field).string.uuid — and the failure can only come from the
#     validator, not from a malformed envelope.
echo "Producing SEMANTIC poison pill (valid protobuf, invalid per protovalidate)..."
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_INVALID=1 SEED_INV_SEQ=501 \
  SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY="9" SEED_INV_UNIT_COST="4.00" go run ./tools/seed

echo "Waiting for the semantic poison pill to reach ${DLQ_TOPIC}..."
DLQ_BOTH="$(docker compose exec -T kafka "${KCAT}/kafka-console-consumer.sh" \
    --bootstrap-server kafka:29092 --topic "${DLQ_TOPIC}" \
    --from-beginning --max-messages 2 --timeout-ms 60000 2>/dev/null | tr -d '\r' || true)"

if [[ "$DLQ_BOTH" != *"definitely-not-a-uuid"* ]]; then
    echo "FAIL: the protovalidate-rejected message never reached ${DLQ_TOPIC}."
    echo "      A validation failure is being classified as something other than terminal —"
    echo "      it is either being retried forever or silently dropped."
    echo "      DLQ contents were: ${DLQ_BOTH}"
    exit 1
fi
echo "Semantic poison pill correctly routed to the DLQ."

# It must ALSO have been rejected before persistence — a validation failure that still wrote a fact
# row would mean the validator is running downstream of the ledger, not in front of it.
INVALID_ROWS=$(crdb "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 501;")
if [[ "$INVALID_ROWS" != "0" ]]; then
    echo "FAIL: the protovalidate-rejected movement was persisted anyway (rows: '${INVALID_ROWS}')"
    exit 1
fi

# 3. Consumer-group state, DIAGNOSTIC ONLY — deliberately not an assertion.
#    An earlier version gated on parsing `kafka-consumer-groups --describe` by column ($2==topic,
#    $6==LAG) and failed with rows=0 even though the DLQ routing had demonstrably worked: that
#    human-readable table is not a stable machine interface (padding, "-" placeholders while
#    rebalancing, and no rows at all for a group between generations). Gating a correctness proof on
#    scraping it produces false failures.
#    It is also redundant. Step 4 below proves the same invariant in a format-independent way: if the
#    source offset had NOT been committed, the consumer would redeliver the poison pill forever and
#    never advance to the valid message. "The stream moved on" IS "the offset was committed".
echo "Consumer-group state for ${GROUP} (diagnostic):"
timeout 30s docker compose exec -T kafka "${KCAT}/kafka-consumer-groups.sh" \
  --bootstrap-server kafka:29092 --describe --group "${GROUP}" 2>/dev/null || true

# 4. THE DECISIVE ASSERTION: a valid message produced AFTER the poison pill must still be processed.
#    This is what proves the stream recovered rather than merely that a DLQ record was written.
echo "Seeding a VALID movement after the poison pill..."
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_SEQ=500 SEED_INV_MOVEMENT_TYPE=receipt \
  SEED_INV_QTY="7" SEED_INV_UNIT_COST="3.00" go run ./tools/seed

echo "Polling for the post-poison fact row..."
DEADLINE=$(( SECONDS + 60 ))
FOUND=""
while (( SECONDS < DEADLINE )); do
    FOUND=$(crdb "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 500;")
    [[ "$FOUND" == "1" ]] && break
    sleep 2
done
if [[ "$FOUND" != "1" ]]; then
    echo "FAIL: the valid message after the poison pill was never processed — partition is wedged"
    exit 1
fi

# Numeric DECIMAL comparison in SQL, never a CSV string compare (CRDB: 3.00 = 3.0000).
QTY_MATCH=$(crdb "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 500 AND qty_delta = 7;")
if [[ "$QTY_MATCH" != "1" ]]; then
    echo "FAIL: post-poison fact row has the wrong qty_delta (matched rows: '${QTY_MATCH}')"
    exit 1
fi

# "The stream kept flowing" is the offset-commit proof: a still-uncommitted poison pill would be
# redelivered indefinitely and this later message would never have been processed.
echo "Success: poison pill routed to the DLQ and the stream kept flowing (offset committed)."
