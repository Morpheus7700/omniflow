# OmniFlow — Phase 3 (P2P Orchestrator) Final Grade

**Auditor:** Principal Engineering Council (Sentinel loop)
**Scope:** `services/p2p-orchestrator/` + `contracts/.../orchestration.proto` + schema
**Date:** 2026-07-21 (re-grade after N2/N3/N4)
**Verdict:** ⚠️ **B+ — an A- engine that isn't plugged into the bus.**

---

## N2 / N3 / N4 — landed correctly (credit)
- **N2 continuation** ✅ `drainWorkflow` walks the sorted DAG to `StateCompleted`; a PO reaches `final_step`.
- **N2 resume** ✅ `handleApproval` loads by event_id, `AcquireLease` (NOWAIT), guards `state==SUSPENDED && node==human_approval`, idempotency-checks, releases the durable lease, then drains. The N1 suspend fix held.
- **N2 short critical section** ✅ releases `FOR UPDATE` (`tx.Rollback`) before the I/O boundary, re-acquires only to checkpoint.
- **N3 protobuf** ✅ `proto.Unmarshal` on the correct `omniflow/contracts/communication/v1` path; `orchestration.proto` in the shared tree.
- **N4 export/shutdown** ✅ `otlptracehttp.WithBatcher`, `sync.WaitGroup`, `producer.Flush`.

## Why it's held at B+, not A- — INTEGRATION IS BROKEN
- **I1 (HIGH) Wrong inbound topic → feedback loop.** CommBot publishes to `omniflow.orchestration.v1`. The orchestrator subscribes to `omniflow.p2p.completed.v1` — **its own output topic** — plus the approval topic. So it never receives CommBot's events and would consume the events it just emitted. Fix: subscribe to `omniflow.orchestration.v1` + `omniflow.p2p.approval.v1`.
- **I2 (HIGH) Contract mismatch.** It unmarshals a home-grown `OrchestratorEvent`, but CommBot emits `VendorEmailReceived` wrapped in the CRDB **changefeed row envelope** (the `payload` column). The orchestrator neither shares CommBot's message type nor decodes the changefeed envelope. Reconcile the on-the-wire contract once, repo-wide (define the changefeed envelope; all consumers decode it identically).

## Concurrency / correctness hardening (for true multi-pod)
- **H-A (HIGH) Re-acquire without re-validation.** In the auto-node path, after releasing the lock for I/O and re-acquiring, the code writes `wf.CurrentNodeIndex++` from **stale in-memory state** without re-reading the row. A pod that interleaved during the window can be **rewound**. Fix: on re-acquire, re-load the row under the lock and confirm it's still at the expected node before checkpointing.
- **H-B (MED) Outbox not exactly-once.** `SaveCheckpoint` always inserts the outbox row even when the ledger insert was a no-op (conflict) — so a retry/interleave emits the downstream event twice. Gate the outbox INSERT on the ledger row being newly inserted (CTE `WHERE EXISTS (RETURNING ...)`).
- **M** approval reads `wf.State` from a pre-lease read; **M** `owner_pod`/`lease_expires_at` are written but never read/enforced (no TTL-reclaim path); **L** completed-event payload is a JSON string, not protobuf.

## Build
`go.mod` still needs `otlptracehttp` + `google.golang.org/protobuf`; CGO/librdkafka for confluent-kafka-go; `.proto` stubs must be generated. Review-grade, not CI-green.

## Gate decision
**Hold at B+.** The internal engine is A- work, but end-to-end the service does not connect to CommBot (I1/I2). Fix the topic + contract, then the re-acquire re-validation (H-A) and exactly-once outbox (H-B), and this is a clean A-.
