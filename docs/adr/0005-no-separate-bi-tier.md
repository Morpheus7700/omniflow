# ADR 0005 — One first-party dashboard, no separate BI tier

- **Status:** Accepted
- **Date:** 2026-07-27 (recorded here 2026-09-22)

## Context

The original blueprint specified a warehouse hop: sync the inventory facts to BigQuery or Snowflake,
model them dimensionally, and query them from Power BI. Some of it was built — a star schema, DAX
measures, a BigQuery-bound changefeed.

## Decision

Analytics is the **same Next.js dashboard** reading the **same SSE stream** as the operational view.
No warehouse, no BI tool, no second copy of the facts. The warehouse and DAX artefacts were deleted
rather than left as "legacy for reference".

## Consequences

- The numbers a buyer asks about (FIFO cost basis, moving average, snapshot value) are already
  computed in `fact_inventory_*` by the inventory service, in one transaction with the ledger. A
  warehouse would copy them, adding a staleness window and a second thing to reconcile — for data
  that is already aggregated and already arriving over the changefeed.
- At this size a BI tier is surface, not capability: another deployable that has never run, which is
  precisely the liability the "Make It Real" mandate exists to kill.
- **What this gives up:** ad-hoc SQL over history by non-engineers, and retention beyond what the
  operational database holds. If either becomes a requirement, the changefeed is already the right
  seam to hang a warehouse sink on — this decision costs little to reverse, which is part of why it
  was the right one to take first.
- Anything in `docs/audit/` describing Power BI, DAX or a BigQuery target predates this and is
  history, not architecture.
