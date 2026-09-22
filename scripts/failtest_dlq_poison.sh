#!/usr/bin/env bash
# scripts/failtest_dlq_poison.sh — the dead-letter path, which no other proof covers.
#
# WHY THIS EXISTS: every consumer withholds its offset commit until DLQ delivery is CONFIRMED
# (see produceToDLQConfirmed — the commit is deliberately downstream of the produce). That ordering is
# load-bearing: it guarantees no message is dropped. But it also means a missing or unwritable .dlq
# topic wedges the partition FOREVER on the first poison message, and every happy-path proof stays
# green while it happens. Topics were once left to broker auto-create, which franz-go never requests —
# so the .dlq topics did not exist and this failure mode was live and invisible.
#
# TWO KINDS of poison are exercised, because they take different code paths:
#   1.  WIRE poison     — bytes proto.Unmarshal rejects. Returns at the unmarshal guard.
#   2b. SEMANTIC poison — wire-valid protobuf that protovalidate rejects. This is the ONLY case that
#       reaches validator.Validate. Testing only (1) leaves the validator's terminal-error
#       classification unproven, which is how a dependency bump could wedge a partition with every
#       CI job still green. A test that stops at an earlier guard proves nothing about the code
#       behind it.
#
# The decisive assertion is the last one: a VALID message processed AFTER the poison pills. That is
# what distinguishes "poison routed to DLQ and the stream moved on" from "partition wedged".
export COMPOSE_PROJECT_NAME="omniflow-failtest-dlq"
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

TOPIC="omniflow.inventory.movement.v1"
DLQ_TOPIC="${TOPIC}.dlq"
GROUP="inventory-intelligence-v1"
KBIN="/opt/kafka/bin"

boot_stack

# Confirm the DLQ topic exists before asserting anything about it, so a missing topic reports itself
# rather than surfacing as an empty-DLQ assertion failure later.
log "Confirming ${DLQ_TOPIC} exists"
timeout 60s docker compose exec -T kafka "${KBIN}/kafka-topics.sh" --bootstrap-server kafka:29092 --list \
  | tr -d '\r' | grep -qx "${DLQ_TOPIC}" \
  || { fail "MISSING TOPIC: ${DLQ_TOPIC}"; exit 1; }

# 1. Wire poison. The leading byte 'n' (0x6E) decodes as field 13 / wire type 6 — 6 is not a valid
#    protobuf wire type, so proto.Unmarshal is guaranteed to fail and classify ErrTerminal.
log "Producing WIRE poison pill to ${TOPIC}"
printf 'not-a-valid-protobuf\n' \
  | timeout 60s docker compose exec -T kafka "${KBIN}/kafka-console-producer.sh" \
      --bootstrap-server kafka:29092 --topic "${TOPIC}"

log "Waiting for the poison pill on ${DLQ_TOPIC}"
DLQ_MSG="$(timeout 60s docker compose exec -T kafka "${KBIN}/kafka-console-consumer.sh" \
    --bootstrap-server kafka:29092 --topic "${DLQ_TOPIC}" \
    --from-beginning --max-messages 1 --timeout-ms 45000 2>/dev/null | tr -d '\r' || true)"
if [[ -z "$DLQ_MSG" ]]; then
    fail "nothing arrived on ${DLQ_TOPIC} — the poison pill was dropped or the consumer is stuck"
    exit 1
fi
echo "DLQ received: ${DLQ_MSG}"
if [[ "$DLQ_MSG" != *"not-a-valid-protobuf"* ]]; then
    fail "DLQ payload is not the original message body (got: ${DLQ_MSG})"
    exit 1
fi

# 2b. Semantic poison. SEED_INV_INVALID sets event_id to a non-UUID; every other field stays
#     well-formed, so exactly one rule fails — (buf.validate.field).string.uuid — and the failure can
#     only come from the validator, not from a malformed envelope.
log "Producing SEMANTIC poison pill (valid protobuf, invalid per protovalidate)"
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_INVALID=1 SEED_INV_SEQ=501 \
  SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY="9" SEED_INV_UNIT_COST="4.00" go run ./tools/seed

log "Waiting for the semantic poison pill on ${DLQ_TOPIC}"
DLQ_BOTH="$(timeout 75s docker compose exec -T kafka "${KBIN}/kafka-console-consumer.sh" \
    --bootstrap-server kafka:29092 --topic "${DLQ_TOPIC}" \
    --from-beginning --max-messages 2 --timeout-ms 60000 2>/dev/null | tr -d '\r' || true)"
if [[ "$DLQ_BOTH" != *"definitely-not-a-uuid"* ]]; then
    fail "the protovalidate-rejected message never reached ${DLQ_TOPIC}"
    echo "      A validation failure is being classified as something other than terminal —"
    echo "      it is either being retried forever or silently dropped."
    echo "      DLQ contents were: ${DLQ_BOTH}"
    exit 1
fi
echo "semantic poison pill correctly routed to the DLQ"

# It must ALSO have been rejected before persistence — a validation failure that still wrote a fact
# row would mean the validator is running downstream of the ledger, not in front of it.
INVALID_ROWS="$(crdb "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 501;")"
if [[ "$INVALID_ROWS" != "0" ]]; then
    fail "the protovalidate-rejected movement was persisted anyway (rows: '${INVALID_ROWS}')"
    exit 1
fi

# 3. Consumer-group state, DIAGNOSTIC ONLY — deliberately not an assertion. That human-readable
#    table is not a stable machine interface (padding, "-" placeholders while rebalancing, no rows
#    between generations); gating on scraping it produced false failures. Step 4 proves the same
#    invariant format-independently: "the stream moved on" IS "the offset was committed".
echo "consumer-group state for ${GROUP} (diagnostic):"
timeout 30s docker compose exec -T kafka "${KBIN}/kafka-consumer-groups.sh" \
  --bootstrap-server kafka:29092 --describe --group "${GROUP}" 2>/dev/null || true

# 4. THE DECISIVE ASSERTION: a valid message produced AFTER the poison pill must still be processed.
log "Seeding a VALID movement after the poison pills"
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_SEQ=500 SEED_INV_MOVEMENT_TYPE=receipt \
  SEED_INV_QTY="7" SEED_INV_UNIT_COST="3.00" go run ./tools/seed

log "Polling for the post-poison fact row"
wait_sql "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 500;" 1 60 \
    || { fail "the valid message after the poison pill was never processed — partition is wedged"; exit 1; }

# Numeric DECIMAL comparison in SQL, never a CSV string compare (CRDB: 3.00 = 3.0000).
QTY_MATCH="$(crdb "SELECT count(*) FROM fact_inventory_movement WHERE sequence_engine_key = 500 AND qty_delta = 7;")"
if [[ "$QTY_MATCH" != "1" ]]; then
    fail "post-poison fact row has the wrong qty_delta (matched rows: '${QTY_MATCH}')"
    exit 1
fi

log "✓ PASSED — both poison pills routed to the DLQ and the stream kept flowing (offset committed)"
