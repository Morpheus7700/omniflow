# OmniFlow — Phase 4 (Inventory Intelligence + BI Star Schema) Audit Charter

The constraints the Sentinel will grade against. Scaffold to these so Phase 4 passes on first review.

## What the service is
`services/inventory-intelligence/` consumes the orchestrator's completed events (`omniflow.p2p.completed.v1`), maintains inventory state, computes valuations, and emits a **BI-ready dimensional (star) model** with FIFO and moving-average **pre-computed in the warehouse** — so Power BI DAX stays trivial. This resolves the contradiction from the very first project doc (the Gemini README said compute FIFO in DAX; the house standard demands it precomputed in the warehouse).

## Non-negotiable constraints (graded)
1. **Ingestion (reuse, don't reinvent):** consume `omniflow.p2p.completed.v1` with the SAME CRDB JSON changefeed decode the orchestrator uses — decode the envelope, **skip `resolved` messages**, `decodeChangefeedBytes` (`\x`-hex or base64) → domain event. Strict hexagonal Go; core imports no Kafka/CRDB/warehouse types.
2. **Valuation engine (correctness + DSA):** FIFO via ordered cost **lots** (a per-SKU queue consumed oldest-first) and **moving-average** cost, both **deterministic and idempotent** — replaying an event must not double-count. Guard with an idempotency ledger keyed on `event_id` (same pattern as the orchestrator). Order lots by **event-time / HLC `sequence_engine_key`**, never arrival order (late/out-of-order events must not corrupt FIFO).
3. **Money math:** cost/value columns are `DECIMAL/NUMERIC`, never float.
4. **Star schema (precompute, don't snowflake):**
   - Facts (separate, by grain): `fact_inventory_movement` (grain = one stock movement) and `fact_inventory_snapshot` (grain = SKU × day). Precomputed columns: `qty_delta`, `qty_on_hand`, `fifo_unit_cost`, `fifo_total_value`, `moving_avg_cost`.
   - Dimensions: `dim_product` (SKU/MDM), `dim_vendor` (`mdm_vendor_id`, SCD2), `dim_date` (full date spine — required for time intelligence), `dim_location`.
   - Surrogate keys + FKs; grain documented per table; no snowflaking that forces expensive DAX joins.
   - Carry both `occurred_at` (event-time) and `cdc_emit_ts` (processing-time) so BI latency is a single subtraction.
5. **Warehouse target:** BigQuery **or** Snowflake. Valuations materialized **incrementally** (dbt-style incremental model or scheduled `MERGE`), not recomputed full-scan. Operational inventory state (on-hand, open FIFO lots) may live in CRDB; the warehouse is the OLAP sink. No dual-write — if it writes CRDB + emits, use the outbox pattern.
6. **Prove the payoff:** ship `infrastructure/warehouse/dax_measures.md` with example **thin** DAX (e.g. `Total Inventory Value = SUM(fact_inventory_snapshot[fifo_total_value])`) demonstrating the precompute keeps DAX to SUM/simple aggregations.

## Expected layout
```
services/inventory-intelligence/
  cmd/main.go
  internal/core/domain/    dag? no — valuation.go (FIFO lots + moving avg), inventory.go (on-hand state machine)
  internal/core/ports/     EventConsumer, WarehouseWriter, InventoryStore, IdempotencyStore
  internal/adapters/inbound/kafka/consumer.go        # consumes p2p.completed.v1
  internal/adapters/outbound/warehouse/              # BigQuery/Snowflake loader (precomputed rows)
  internal/adapters/outbound/crdb/                   # on-hand + FIFO-lot ledger + idempotency
infrastructure/warehouse/
  star_schema.sql          # dims + facts DDL with precomputed valuation columns
  dax_measures.md          # thin DAX proving the precompute
contracts/proto/events/inventory/v1/inventory.proto  # InventoryMovement / InventorySnapshot
```

## Failure modes I will hunt (pre-warned)
- FIFO/moving-avg computed in DAX instead of the warehouse (violates the standard).
- Non-idempotent valuation → replayed event double-counts inventory.
- Ordering by arrival instead of event-time/HLC → corrupted FIFO layers.
- `float` money columns → rounding drift.
- Snowflaked dims / missing `dim_date` → expensive or broken DAX time intelligence.
- One fact table mixing movement-grain and snapshot-grain → grain confusion.
- Re-decoding the changefeed envelope differently from the orchestrator → drift; reuse the same decode.
