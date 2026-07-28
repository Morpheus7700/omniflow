# Scope

This document draws the boundary of OmniFlow honestly: what it sets out to prove, what is deliberately
left out, and what is real versus deferred. The governing principle is **"Make It Real"** — prove the
existing core runs end to end and survives failure, rather than accumulate surface area that compiles
but never runs.

## Goal

Demonstrate Staff-level distributed-systems judgment on a realistic domain (autonomous procure-to-pay &
supply chain) by building an event-sourced core and **proving its correctness properties by booting the
real stack in CI** — not by asserting them in prose.

## In scope — and proven in CI

- **Transactional outbox + native CDC** as the only DB→bus bridge (no dual writes, no Debezium).
- **Durable, resumable DAG workflow** with human-in-the-loop suspend/resume.
- **Exactly-once effects** under duplicate delivery.
- **FIFO / moving-average inventory valuation** with **HLC-ordered late-arrival restatement**.
- **Precision-safe streaming projection** to a browser settlement ledger over SSE.
- **Failure-survival proofs** as first-class CI jobs (killed-pod resume, exactly-once suppression,
  FIFO restatement), each asserting its invariant on the real wire.

Each item above maps to a runnable proof — see the "Proving it's real" table in the [README](README.md).

## In scope — built, exercised indirectly

- **CommBot zero-trust ingress** (SSRF allowlist, quarantine boundary, DoW token bucket, LLM classify
  hop). The classification hop fetches quarantined content over public HTTPS *before* the LLM call,
  which is impossible inside CI's private network — so the E2E deliberately seeds at the
  `commbot_outbox` table (`SEED_MODE=outbox`) and lets the **real** changefeed drive everything
  downstream. CommBot's own logic is covered by its unit tests and by the `email` seed mode (which
  needs a reachable, allowlisted HTTPS source).

## Explicitly out of scope (deferred, by design)

These are omitted on purpose. Adding them would be "blueprint surface" that dilutes the goal, not
progress toward it.

- **Kubernetes / Istio / service mesh / mTLS.** The proof target is `docker compose` in CI.
- **Neo4j / Pinecone / vector or graph stores.** No use case in the proven core.
- **A separate BI tier or data warehouse.** Analytics surface through the first-party Next.js
  dashboard over the same stream the operational view uses. The facts a buyer asks about are already
  computed in `fact_inventory_movement` / `fact_inventory_snapshot` and travel on the changefeed, so a
  warehouse hop would duplicate state and introduce a staleness window for no gain at this size.
  Removed rather than parked: the previous `infrastructure/warehouse/*` sketches were never applied by
  `crdb-init` or referenced by any script, and unreachable files invite the reader to assume a system
  is larger than it is.
- **WebSocket transport.** SSE is the deliberate choice for a read-only broadcast stream; a WebSocket
  upgrade, if ever pursued, is its own scoped and audited change (`docs/adr/0001-sse-over-websocket.md`).
- **Multi-node / multi-region CockroachDB.** Single-node is sufficient to prove the CDC and ordering
  properties; horizontal scale is an operational concern, not a correctness one.
- **Production authn/z, tenancy, billing.** Not part of the systems-design thesis.

## Known constraints & honest caveats

- **No CockroachDB license required.** The stack runs a single-node cluster, which under CockroachDB
  v24.3+ licensing needs no key (changefeeds included). crdb-init, the proof scripts, and CI run
  license-free and fail loudly if a key ever turns out to be required — they never skip silently. A free
  Enterprise license (`CRDB_LICENSE` / `CRDB_ORG`) is only relevant for a multi-node deployment.
- **No local Docker daemon was used during development.** All boot/failure proofs are authored to run in
  GitHub Actions (or any Docker host); their first real execution is the CI run. This is intentional —
  it forces the proofs to be genuine rather than validated against a hand-held local environment.
- **Locked correctness core.** The DAG engine, CommBot core domain, the orchestrator schema and
  suspend/HITL logic, and the inventory valuation/ledger are treated as locked — changes to them are
  reviewed as regressions. The single sanctioned schema deviation is the additive
  `orchestrator_outbox.sequence_engine_key` column.

## Definition of done for the current milestone

The repo is public and the four CI boot proofs (`boot stack + seed E2E` and the three failure tests)
are green on `master`. Everything up to that gate is built and audited; the gate itself is the first
end-to-end execution of the real stack.
