# 03 · Locked Constraints

These are load-bearing. Modifying them is a regression that **fails audit**. If a task seems to
require touching one, stop and flag it — there is almost always a way around.

## Locked files / logic
- `services/p2p-orchestrator/internal/core/domain/dag.go` — the DAG engine.
- **CommBot core logic** — `service.go` (`ProcessVendorEmail`), the LLM gateway's SSRF/allowlist
  guard. See [[05-gotchas#CommBot cannot run in CI]].
- `services/p2p-orchestrator/.../crdb/state_store.go` — the **exactly-once CTE**: the `insert_ledger`
  CTE + `WHERE EXISTS (SELECT 1 FROM insert_ledger)` guard must stay byte-for-byte. (Adding a column
  to the outbox INSERT target list is the ONE authorized exception — see [[05-gotchas#Additive column exception]].)
- orchestrator `service.go` suspend/HITL logic.
- **Phase-4 valuation/ledger** — `inventory-intelligence/internal/core/domain/valuation.go` and
  `.../adapters/outbound/crdb/repository.go`. The FIFO/restatement/moving-avg core. Untouched through
  Prompt 6 (verified via `git diff --stat`).

## Load-bearing invariants (in every consumer)
- **Manual commit only** (`kgo.DisableAutoCommit()`); commit AFTER successful processing. Never switch
  to auto-commit.
- **Confirmed-DLQ ordering**: `ProduceSync` to the DLQ must succeed BEFORE committing the source
  offset. A failed produce + committed offset = permanent data loss.
- **Transient vs terminal** error taxonomy drives retry-vs-DLQ; preserve the `errors.Is` classification.
- **`sequence_engine_key` stays a STRING** past the DB boundary (viz reads it via `UseNumber`, never
  float64).

## Locked product decisions (see [[00-project-overview]])
CDC = CRDB changefeeds (no Debezium). Kafka = franz-go. BI = Next.js dashboard (no Power BI). Browser
transport = SSE. DB name = `omniflow` for every service.

Related: [[01-working-loop]] · [[05-gotchas]]
