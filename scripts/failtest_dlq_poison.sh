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
# The decisive assertion is the last one: a VALID message processed AFTER the poison pill. That is
# what distinguishes "poison routed to DLQ and the stream moved on" from "partition wedged".
#
# CRDB_LICENSE is OPTIONAL (single-node v24.3+ runs changefeeds license-free).

export COMPOSE_PROJECT_NAME="omniflow-failtest-dlq"

TOPIC="omniflow.inventory.movement.v1"
DLQ_TOPIC="${TOPIC}.dlq"
GROUP="inventory-intelligence-v1"
KCAT="/opt/kafka/bin"

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
docker compose exec -T kafka "${KCAT}/kafka-topics.sh" --bootstrap-server kafka:29092 --list \
  | tr -d '\r' | grep -qx "${DLQ_TOPIC}" \
  || { echo "MISSING TOPIC: ${DLQ_TOPIC}"; exit 1; }

sleep 5 # let the inventory consumer join the group before we hand it a poison pill

# 1. Poison pill. The leading byte 'n' (0x6E) decodes as field 13 / wire type 6 — 6 is not a valid
#    protobuf wire type, so proto.Unmarshal is guaranteed to fail and classify ErrTerminal. (Even if
#    it somehow parsed, protovalidate would reject it — also terminal, also DLQ.)
echo "Producing poison pill to ${TOPIC}..."
printf 'not-a-valid-protobuf\n' \
  | docker compose exec -T kafka "${KCAT}/kafka-console-producer.sh" \
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

# 3. Assert the source offset was COMMITTED after DLQ delivery — i.e. the partition is not wedged.
#    LAG must drain to 0. A wedged partition sits at LAG>=1 forever.
#
#    The row count matters as much as the lag: `kafka-consumer-groups --describe` prints LAG as "-"
#    when a group has no committed offset yet, and prints nothing at all for an unknown group. Summing
#    only numeric LAG values would then yield 0 and PASS while proving nothing. So require at least
#    one numeric LAG row for this topic AND every one of them at 0.
#    Column order (Kafka 3.8): GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
echo "Asserting consumer-group lag drains to 0 for ${GROUP}..."
lag_state() {
    docker compose exec -T kafka "${KCAT}/kafka-consumer-groups.sh" \
        --bootstrap-server kafka:29092 --describe --group "${GROUP}" 2>/dev/null \
      | awk -v t="${TOPIC}" '$2==t && $6 ~ /^[0-9]+$/ { rows++; total+=$6 } END { print (rows+0) " " (total+0) }'
}

DEADLINE=$(( SECONDS + 60 ))
LAG_OK=0
while (( SECONDS < DEADLINE )); do
    read -r ROWS TOTAL_LAG <<< "$(lag_state)"
    if (( ROWS > 0 )) && (( TOTAL_LAG == 0 )); then LAG_OK=1; break; fi
    sleep 2
done

if (( LAG_OK != 1 )); then
    echo "FAIL: expected >=1 committed partition for ${TOPIC} with total lag 0; got rows=${ROWS:-0} lag=${TOTAL_LAG:-unknown}"
    echo "      (no committed offset means the poison pill was never acknowledged — partition wedged)"
    docker compose exec -T kafka "${KCAT}/kafka-consumer-groups.sh" \
      --bootstrap-server kafka:29092 --describe --group "${GROUP}" || true
    exit 1
fi
echo "Committed offsets present for ${TOPIC} and lag is 0."

# 4. THE DECISIVE ASSERTION: a valid message produced AFTER the poison pill must still be processed.
#    This is what proves the stream recovered rather than merely that a DLQ record was written.
echo "Seeding a VALID movement after the poison pill..."
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_SEQ=500 SEED_INV_MOVEMENT_TYPE=receipt \
  SEED_INV_QTY="7" SEED_INV_UNIT_COST="3.00" go run ./tools/seed

echo "Polling for the post-poison fact row..."
timeout 45s bash -c 'until [[ "$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 500;" | tail -n 1)" == "1" ]]; do sleep 2; done' || {
    echo "FAIL: the valid message after the poison pill was never processed — partition is wedged"
    exit 1
}

# Numeric DECIMAL comparison in SQL, never a CSV string compare (CRDB: 3.00 = 3.0000).
QTY_MATCH=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv \
  -e "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 500 AND qty_delta = 7;" | tail -n 1)
if [[ "$QTY_MATCH" != "1" ]]; then
    echo "FAIL: post-poison fact row has the wrong qty_delta (matched rows: ${QTY_MATCH})"
    exit 1
fi

echo "Success: poison pill routed to the DLQ, offset committed, and the stream kept flowing!"
