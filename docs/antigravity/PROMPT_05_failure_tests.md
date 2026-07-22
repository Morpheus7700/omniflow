# Prompt 5 — Prove the core SURVIVES failure (paste into Antigravity)

> **Precondition (do not skip):** the base E2E (`scripts/e2e.sh`, CI job `e2e`) must have gone
> **green at least once** before starting this prompt. Make-It-Real forbids piling un-run code on
> un-run code. If `e2e` has never passed, STOP and get it green first (push repo + set
> `CRDB_LICENSE`/`CRDB_ORG` secrets).

## BLOCK A — Re-onboard (disk changed since your last turn)

Claude (the Principal Sentinel/Auditor) implemented **Prompt 4 directly on disk** — you did not build
it. Sync to reality before touching anything:

- **`tools/seed/main.go`** — franz-go + pgx seeder. `SEED_MODE=outbox` (default) inserts a classified
  `VendorEmailReceived` protobuf into the **`commbot_outbox` table**; the real CRDB→Kafka changefeed
  then drives `omniflow.orchestration.v1` → orchestrator → HITL suspend → approval → `final_step` →
  `orchestrator_outbox` → `omniflow.p2p.completed.v1` → viz SSE. It generates its own
  `event_id`/`traceparent`/`sequence_engine_key`, **polls `workflows` until `SUSPENDED`**, then
  produces a `HumanApprovalEvent` (same `event_id`) to `omniflow.p2p.approval.v1`. Prints
  `SEED_EVENT_ID=` and `SEED_SEQUENCE_ENGINE_KEY=`. `SEED_MODE=email` is a secondary path.
  - Reusable primitives already in the file: `seedOutbox(...)`, `waitForSuspended(...)`,
    `produceApproval(...)`, `buildVendorEmail(...)`, `uuidV4()`, `newTraceParent()`.
- **`tools/mock-llm/`** — deterministic `/chat/completions`, wired into compose; used only by `email`.
- **Part C landed:** `orchestrator_outbox` has an additive `sequence_engine_key INT8 NOT NULL` column;
  the exactly-once CTE in `state_store.go` populates it as `$10 = wf.SequenceEngineKey` — the
  `insert_ledger` guard `WHERE EXISTS (SELECT 1 FROM insert_ledger)` is **byte-for-byte unchanged**.
  `viz-gateway`'s `handleP2PCompleted` now unwraps the changefeed `after{}` envelope.
- **`scripts/e2e.sh`** — boots stack, waits `crdb-init`, asserts ≥3 running changefeeds, opens the SSE
  stream **before** seeding, seeds, asserts the seq-key arrives, always `down -v`.
- **Why the orchestrator, not CommBot, is the failure-test target:** CommBot's classify path is
  un-runnable in CI (its SSRF guard fetches subject/body over https from a public-IP-resolving
  allowlisted host before the LLM call). The exactly-once + durable-checkpoint machinery we want to
  prove lives in the **p2p-orchestrator**, and the seed harness already drives it.

Run `CGO_ENABLED=0 go build ./...` (root) + `cd services/viz-gateway && go build ./...` and confirm
exit 0 before editing, so you know you started from green.

## LOCKED — regressions fail audit (do not modify)
`dag.go`; CommBot core logic; the orchestrator exactly-once CTE in `state_store.go`
(`insert_ledger` + `WHERE EXISTS`); `service.go` suspend/HITL logic; Phase-4 valuation/ledger. The
manual-commit + confirmed-DLQ ordering in every consumer is load-bearing. **These tests must OBSERVE
the existing behavior, never weaken it to pass.**

## Scope of this prompt — TWO resilience proofs (FIFO restatement is Prompt 6)
FIFO late-arrival restatement is deferred: `inventory-intelligence` consumes `omniflow.p2p.completed.v1`
(not a dedicated movement topic) and the p2p payload carries no SKU/quantity — that input path needs
a grounding/repair pass first. Do NOT attempt it here.

## Part 1 — Make the seeder splittable (additive refactor of `tools/seed`)
Add an env `SEED_ACTION` (default `full` = today's behavior) with two new values. Refactor `run()` to
compose the EXISTING primitives — do not duplicate logic, do not change `full`:
- `SEED_ACTION=suspend-only`: seed the entry seam, `waitForSuspended`, print `SEED_EVENT_ID` +
  `SEED_SEQUENCE_ENGINE_KEY`, exit 0 **without approving**.
- `SEED_ACTION=approve-only`: read `SEED_EVENT_ID`, `SEED_TRACE_PARENT` (optional; generate if empty),
  `SEED_SEQUENCE_ENGINE_KEY` from env and call `produceApproval(...)` only. (For the resume test the
  traceparent value is immaterial to the assertion.)
Rebuild + vet. `full` mode output must be identical to before.

## Part 2 — `scripts/failtest_killed_pod.sh` (durable checkpoint resume)
Prove a workflow survives orchestrator pod death because state lives in CRDB, not memory.
1. `docker compose up -d --build`; wait `crdb-init`==0.
2. `SEED_ACTION=suspend-only go run ./tools/seed` → capture `SEED_EVENT_ID`, `SEED_SEQUENCE_ENGINE_KEY`.
   Assert `workflows.state='SUSPENDED'` for that `event_id` (via `docker compose exec cockroachdb`).
3. **Kill and restart** only the orchestrator: `docker compose kill p2p-orchestrator` then
   `docker compose up -d p2p-orchestrator`. (This drops all in-memory state.)
4. Open the SSE stream, then `SEED_ACTION=approve-only` with the captured env to release approval.
5. Assert within ~20s: the seq-key reaches the SSE stream **AND**
   `SELECT count(*) FROM node_execution_ledger WHERE node_id='final_step'` **== 1** (resumed exactly
   once, no double-execution) **AND** `workflows.state` is `COMPLETED`.
6. Dump logs on failure; always `docker compose down -v`.

## Part 3 — `scripts/failtest_exactly_once.sh` (duplicate-delivery suppression)
Prove a redelivered approval produces no duplicate effects.
1. Boot; wait `crdb-init`==0.
2. `SEED_ACTION=full go run ./tools/seed` and let it complete once (captures the key).
3. **Re-produce the SAME approval a second time** (`SEED_ACTION=approve-only` with the same
   `SEED_EVENT_ID`). The workflow is already `COMPLETED`, so `service.go`'s
   `if wf.State != domain.StateSuspended || nodeID != "human_approval"` guard must no-op it.
4. Assert the duplicate changed nothing: `node_execution_ledger` for that workflow has exactly the
   nodes it should (one `human_approval`, one `final_step`), and `orchestrator_outbox` for that
   `aggregate_id` has **exactly 2 rows** (approved + completed), not 4. No new workflow row.
5. Dump logs on failure; always `docker compose down -v`.

> Both scripts follow the `scripts/e2e.sh` conventions: `set -euo pipefail`, license guard (exit 78 if
> `CRDB_LICENSE` unset), SSE opened before the triggering action, teardown in a trap.

## Part 4 — CI wiring (`.github/workflows/e2e.yml`)
Add two jobs, `failtest-resume` and `failtest-exactly-once`, each `needs: [build]`, guarded exactly
like the `e2e` job (skip cleanly when `CRDB_LICENSE` is absent), each running its script. Keep them
separate jobs so a failure names the exact invariant that broke.

## Deliverables & verification
Output: the `tools/seed` diff, both new scripts, the `e2e.yml` diff. Run `go build ./...` +
`cd services/viz-gateway && go build ./...` and show exit 0. **Do NOT fabricate a boot log** — the real
runs happen in CI. State plainly what you verified (build/vet/syntax) vs. what only CI proves (the
kill/restart, the duplicate suppression). Then hand back to the Sentinel for audit.
