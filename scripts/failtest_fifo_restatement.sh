#!/usr/bin/env bash
set -euo pipefail

# Git Bash / MSYS2 on Windows rewrites arguments that look like absolute POSIX paths into Windows
# paths before the process ever sees them, so `docker compose exec -T kafka /opt/kafka/bin/...`
# arrives inside the container as `C:/Program Files/Git/opt/kafka/bin/...` and exits 127. The path
# is meaningful in the CONTAINER, not on the host, so the rewrite is always wrong here. Exporting
# this disables it. On Linux and macOS the variable is simply ignored, so CI is unaffected — which
# is exactly why this never showed up until the stack was first booted on Windows.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

# CRDB_LICENSE is OPTIONAL (single-node v24.3+ runs changefeeds license-free). We do NOT skip when it
# is unset — the stack boots and any real licensing error surfaces as a hard failure, never a silent pass.

export COMPOSE_PROJECT_NAME="omniflow-failtest-fifo"

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

# kafka-init creates every topic — including omniflow.inventory.movement.v1, which this test's
# inventory seeds produce to, and the .dlq sinks. franz-go never requests auto-topic-creation, so
# the broker's KAFKA_AUTO_CREATE_TOPICS_ENABLE does nothing for our Go clients. Check it first:
# crdb-init gates on kafka-init, so a topic failure would otherwise surface as a confusing
# crdb-init stall rather than naming itself.
docker compose wait kafka-init >/dev/null 2>&1 || true
KINIT_CID="$(docker compose ps -aq kafka-init)"
KINIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$KINIT_CID" 2>/dev/null || echo unknown)"
echo "kafka-init exit code: ${KINIT_CODE}"
[[ "$KINIT_CODE" == "0" ]] || { echo "kafka-init failed (exit $KINIT_CODE)"; docker compose logs kafka-init || true; exit 1; }

echo "Waiting for CRDB initialization..."
timeout 60s bash -c 'until docker compose logs crdb-init | grep -q "crdb-init complete."; do sleep 1; done' || { echo "CRDB init failed"; exit 1; }
# docker compose wait adopts the container's exit code as its own → set -e would abort before we
# can report it. Block, then read the true code via docker inspect.
docker compose wait crdb-init >/dev/null 2>&1 || true
INIT_CID="$(docker compose ps -aq crdb-init)"
INIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$INIT_CID" 2>/dev/null || echo unknown)"
echo "crdb-init exit code: ${INIT_CODE}"
[[ "$INIT_CODE" == "0" ]] || { echo "crdb-init failed (exit $INIT_CODE)"; docker compose logs crdb-init || true; exit 1; }

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
