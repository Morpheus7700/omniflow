#!/usr/bin/env bash
# scripts/failtest_fifo_restatement.sh — HLC late-arrival restatement of FIFO valuation.
#
# Scenario (one SKU):
#   Receipt B      seq=200  qty 10 @ 5.00
#   Consumption C  seq=300  qty 5            -> valued at 5.00 (only lot B exists)
#   Late Receipt A seq=100  qty 10 @ 2.00    -> arrives AFTER C but sorts BEFORE it
# The inventory engine must notice seq 100 < max seen (200), reset the SKU's lots, and replay in HLC
# order — so C is now consumed from lot A and its fifo_unit_cost flips 5.00 → 2.00, and the snapshot
# value becomes 60.00 (5 @ 2.00 remaining from A + 10 @ 5.00 from B).
#
# The late event has a LOW sequence key but a `now` occurred_at: sequence_engine_key drives FIFO
# order and triggers restatement; occurred_at drives the lateness horizon. Backdating occurred_at
# would silently test the adjustment branch instead. See docs/kb/05-gotchas.md.
export COMPOSE_PROJECT_NAME="omniflow-failtest-fifo"
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

boot_stack

# Extra NAME=VALUE pairs arrive as arguments, so they go through `env`: a bare "$@" before the
# command would make bash execute "SEED_INV_SEQ=200" as a program (exit 127).
seed_inv() {
    env SEED_ACTION=inventory SEED_MODE=inventory "$@" go run ./tools/seed
}

# DECIMALs are compared numerically IN SQL (CRDB: 5.00 = 5.0000), never by CSV string — shopspring/
# CRDB serialisation of DECIMAL scale is not guaranteed to be the literal "5.00".

log "Seeding Receipt B (seq 200, 10 @ 5.00)"
seed_inv SEED_INV_SEQ=200 SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY="10" SEED_INV_UNIT_COST="5.00"
wait_sql "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=1;" 1 30 \
    || { fail "Receipt B fact not found (last: '${SQL_LAST}')"; exit 1; }

log "Seeding Consumption C (seq 300, 5 units)"
seed_inv SEED_INV_SEQ=300 SEED_INV_MOVEMENT_TYPE=consumption SEED_INV_QTY="5"
wait_sql "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=2;" 1 30 \
    || { fail "Consumption C fact not found (last: '${SQL_LAST}')"; exit 1; }

C_MATCH="$(crdb "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=2 AND fifo_unit_cost = 5.00;")"
if [[ "$C_MATCH" != "1" ]]; then
    fail "expected Consumption C fifo_unit_cost=5.00 before restatement (matched rows: '${C_MATCH}')"
    exit 1
fi

log "Seeding LATE Receipt A (seq 100, 10 @ 2.00)"
seed_inv SEED_INV_SEQ=100 SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY="10" SEED_INV_UNIT_COST="2.00"

log "Waiting for the restatement to flip C's fifo_unit_cost to 2.00"
wait_sql "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=2 AND fifo_unit_cost = 2.00;" 1 40 \
    || { fail "restatement did not flip fifo_unit_cost to 2.00 (last: '${SQL_LAST}')"; exit 1; }

TOTAL_MATCH="$(crdb "SELECT count(*) FROM fact_inventory_snapshot WHERE sku = 'SKU-TEST-001' AND fifo_total_value = 60.00;")"
if [[ "$TOTAL_MATCH" != "1" ]]; then
    fail "expected snapshot fifo_total_value=60.00 (matched rows: '${TOTAL_MATCH}')"
    exit 1
fi

log "✓ PASSED — late receipt restated FIFO: C now 2.00, snapshot 60.00"
