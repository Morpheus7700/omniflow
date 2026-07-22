#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${CRDB_LICENSE:-}" ]]; then
    echo "CRDB_LICENSE is unset. Skipping failtest_fifo_restatement."
    exit 78 # EX_CONFIG
fi

export COMPOSE_PROJECT_NAME="omniflow-failtest-fifo"

cleanup() {
    echo "Tearing down..."
    docker compose logs > fifo-restatement-failtest.log || true
    docker compose down -v
}
trap cleanup EXIT

echo "Booting stack..."
docker compose up -d --build

echo "Waiting for CRDB initialization..."
timeout 60s bash -c 'until docker compose logs crdb-init | grep -q "crdb-init complete."; do sleep 1; done' || { echo "CRDB init failed"; exit 1; }
INIT_CODE="$(docker compose wait crdb-init)"
[[ "$INIT_CODE" == "0" ]] || { echo "crdb-init failed (exit $INIT_CODE)"; exit 1; }

sleep 5

# 2. Receipt B — SEED_INV_SEQ=200 type=receipt QTY=10 UNIT_COST=5.00
echo "Seeding Receipt B..."
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_SEQ=200 SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY="10" SEED_INV_UNIT_COST="5.00" go run ./tools/seed

echo "Polling for Receipt B fact..."
timeout 20s bash -c 'until [[ "$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=1;" | tail -n 1)" == "1" ]]; do sleep 1; done' || { echo "Receipt B fact not found"; exit 1; }

# 3. Consumption C — SEED_INV_SEQ=300 type=consumption QTY=5
echo "Seeding Consumption C..."
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_SEQ=300 SEED_INV_MOVEMENT_TYPE=consumption SEED_INV_QTY="5" go run ./tools/seed

echo "Polling for Consumption C fact..."
timeout 20s bash -c 'until [[ "$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=2;" | tail -n 1)" == "1" ]]; do sleep 1; done' || { echo "Consumption C fact not found"; exit 1; }

# Compare DECIMALs numerically IN SQL (CRDB: 5.00 = 5.0000), never by CSV string — shopspring/CRDB
# serialization of DECIMAL scale is not guaranteed to be the literal "5.00".
C_MATCH=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=2 AND fifo_unit_cost = 5.00;" | tail -n 1)
if [[ "$C_MATCH" != "1" ]]; then
    echo "Expected Consumption C fifo_unit_cost=5.00 before restatement (matched rows: ${C_MATCH})"
    exit 1
fi

# 4. Late Receipt A — SEED_INV_SEQ=100 type=receipt QTY=10 UNIT_COST=2.00
echo "Seeding Late Receipt A..."
SEED_ACTION=inventory SEED_MODE=inventory SEED_INV_SEQ=100 SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY="10" SEED_INV_UNIT_COST="2.00" go run ./tools/seed

# 5. Assert: flip to 2.00 for C, and total_value to 60.00
echo "Waiting for restatement flip..."
timeout 20s bash -c 'until [[ "$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM fact_inventory_movement WHERE movement_type=2 AND fifo_unit_cost = 2.00;" | tail -n 1)" == "1" ]]; do sleep 1; done' || { echo "Restatement did not flip fifo_unit_cost to 2.00"; exit 1; }

TOTAL_MATCH=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv -e "SELECT count(*) FROM fact_inventory_snapshot WHERE sku = 'SKU-TEST-001' AND fifo_total_value = 60.00;" | tail -n 1)
if [[ "$TOTAL_MATCH" != "1" ]]; then
    echo "Expected snapshot fifo_total_value=60.00 (matched rows: ${TOTAL_MATCH})"
    exit 1
fi

echo "Success: FIFO restatement proved!"
