# Prompt 6 — Fix the inventory ingress + prove FIFO late-arrival restatement (paste into Antigravity)

> **Precondition:** `e2e` + `failtest-resume` + `failtest-exactly-once` are green in CI. This prompt
> adds a third failure proof on the same harness.

## BLOCK A — Grounding (Sentinel investigation, 2026-07-23)
`inventory-intelligence`'s ingress is **broken scaffold**, discovered during the Prompt 6 grounding pass:
- It subscribes to `omniflow.p2p.completed.v1`, whose changefeed payload is
  `{"status":"completed","node":"final_step"}` — **no SKU/quantity/unit_cost**. A completing PO does
  not carry line items. So no movement can ever be decoded from it.
- The consumer's envelope struct reads `payload.after` (a nested `{"payload":{"after":...}}`), but a
  CRDB changefeed emits `{"after":{...columns...}}` — the nesting is inverted; it can't decode a real
  changefeed regardless of topic.
- There is **no movement-input table or input changefeed** anywhere. The ingress was never wired.

**Design decision (do not re-litigate):** inventory movements are **external ingress** (warehouse
receipts/consumption/adjustments), the same class as vendor emails on `omniflow.communication.v1`.
Fix them as a **raw-protobuf ingress topic** `omniflow.inventory.movement.v1` — NOT an
outbox→changefeed. (Outbox→changefeed exists to avoid dual-write when a service writes DB state *and*
emits an event; here the movements ARE the ingress, no prior DB write, so that rationale doesn't
apply. Rejected alternative: a `movement_outbox` table + changefeed — more moving parts, no benefit.)

## LOCKED — the Phase-4 core stays byte-for-byte (regressions fail audit)
`services/inventory-intelligence/internal/core/domain/valuation.go` (FIFO, `restateSKUFrom`,
`depleteLots`, lateness horizon) and `.../adapters/outbound/crdb/repository.go` (single-tx
`ProcessInventoryTransaction`, ledger, `ON CONFLICT` fact upsert, movement log). This prompt only
repairs the **inbound adapter** (topic + decode) and adds a seeder mode + a test. Do NOT touch the
valuation or repository logic.

## Part 1 — Repair the ingress adapter (raw protobuf)
`services/inventory-intelligence/internal/adapters/inbound/kafka/consumer.go` + `cmd/main.go`:
1. `main.go`: change `kgo.ConsumeTopics("omniflow.p2p.completed.v1")` →
   `kgo.ConsumeTopics("omniflow.inventory.movement.v1")`.
2. `consumer.go`: `processRecord` must decode the record value as a **raw** `InventoryMovementReceived`
   protobuf — `proto.Unmarshal(record.Value, &pb)` — exactly like CommBot's inbound consumer decodes
   `communication.v1`. DELETE the changefeed-envelope struct, the `payload.after` extraction, and the
   hex/base64 branch; DELETE `isResolvedMessage` (raw ingress has no changefeed `resolved` messages).
   KEEP unchanged: `protovalidate.Validate`, `mapToDomain`, `service.Process`, the manual-commit +
   confirmed-DLQ + transient-no-commit contract, and the `ErrTransient`/`ErrTerminal` switch.
3. No `crdb-init.sh` change: `omniflow.inventory.movement.v1` is a plain Kafka topic
   (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` in compose creates it on first produce).

## Part 2 — Add an inventory seed mode to `tools/seed`
Add `SEED_MODE=inventory`: produce ONE `InventoryMovementReceived` (from `contracts/inventory/v1`) to
`omniflow.inventory.movement.v1` via the existing franz-go `produce(...)` helper, then exit 0. Fields
from env (all overridable; generate a fresh UUID `event_id` if unset, and print `SEED_EVENT_ID=`):
- `SEED_INV_SEQ` (uint64 → `sequence_engine_key`), `SEED_INV_MOVEMENT_TYPE` (`receipt|consumption|
  adjustment`), `SEED_INV_SKU`, `SEED_INV_QTY` (string, e.g. `"10"`), `SEED_INV_UNIT_COST` (string,
  e.g. `"5.00"`), `SEED_INV_LOCATION` (default `"LOC-1"`), `SEED_INV_VENDOR` (default `"VENDOR-ACME-001"`).
- **Set `occurred_at = published_at = now`** regardless of `SEED_INV_SEQ` — the HLC seq controls FIFO
  order + late-arrival detection, but `occurred_at` must stay within the 30-day lateness horizon so a
  low-seq arrival triggers RESTATEMENT (not the adjustment fallback). This distinction is the crux.
- Must pass the contract's protovalidate (event_id uuid, traceparent pattern, min-len sku/vendor/
  location, quantity `^-?[0-9]+(\.[0-9]+)?$`, unit_cost `^[0-9]+(\.[0-9]+)?$`). Verify enum/field
  names against `contracts/inventory/v1/inventory.pb.go`.

## Part 3 — `scripts/failtest_fifo_restatement.sh`
Follow the `scripts/e2e.sh` conventions (`set -euo pipefail`, license guard exit 78,
`docker compose wait crdb-init`, teardown trap, `docker compose exec -T cockroachdb`). Use a fixed
test SKU (stack is fresh per run). Drive movements IN ARRIVAL ORDER, waiting for each to land before
the next so ordering is deterministic:

1. Boot; wait `crdb-init`==0.
2. **Receipt B** — `SEED_INV_SEQ=200 SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY=10 SEED_INV_UNIT_COST=5.00`.
   Poll until a `fact_inventory_movement` row for the SKU with `movement_type=1` (receipt) exists.
3. **Consumption C** — `SEED_INV_SEQ=300 SEED_INV_MOVEMENT_TYPE=consumption SEED_INV_QTY=5`.
   Poll until the consumption fact (`movement_type=2`) exists. At this point assert its
   `fifo_unit_cost = 5.00` (it consumed the only lot, @5.00).
4. **Late Receipt A** — `SEED_INV_SEQ=100 SEED_INV_MOVEMENT_TYPE=receipt SEED_INV_QTY=10 SEED_INV_UNIT_COST=2.00`.
   seq 100 < max(200) ⇒ `restateSKUFrom` rewinds + replays in HLC order [A@100=10×2, B@200=10×5,
   C@300 consume 5]. The consumption now depletes the OLDEST lot (A@2.00).
5. **Assert the restatement** (poll ≤20s for the value to flip):
   - `SELECT fifo_unit_cost FROM fact_inventory_movement WHERE sku=<SKU> AND movement_type=2` **== 2.00**
     (was 5.00 before the late arrival — this is the whole proof).
   - Snapshot check: `SELECT fifo_total_value FROM fact_inventory_snapshot WHERE sku=<SKU>` **== 60.00**
     (surviving lots after replay: A 5×2 + B 10×5 = 60).
6. Dump logs on failure; always `docker compose down -v`.

> Rationale for the numbers: chronological FIFO consumes the cheap early lot first (COGS unit 2.00),
> vs. arrival-order which had already consumed the expensive lot (5.00). The restatement corrects a
> consumption that already committed against the wrong layer — that's the invariant under test.

## Part 4 — CI wiring (`.github/workflows/e2e.yml`)
Add a `failtest-restatement` job, `needs: [build]`, guarded exactly like the other failtest jobs
(skip cleanly when `CRDB_LICENSE` absent), running `bash scripts/failtest_fifo_restatement.sh`.

## Deliverables & verification
Output the `consumer.go` + `main.go` diffs, the `tools/seed` diff, the new script, the `e2e.yml` diff.
Run `CGO_ENABLED=0 go build ./...` (root) + `cd services/viz-gateway && go build ./...` + `go vet ./...`
and show exit 0. Confirm `valuation.go` and `repository.go` are unchanged (`git diff --stat` on those
paths must be empty). **Do NOT fabricate a boot log** — state what you verified vs. what only CI proves.
Hand back to the Sentinel for audit.
