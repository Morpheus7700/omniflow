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
  upgrade, if ever pursued, is its own scoped and audited change. The decision is ADR 0001; the
  deferred spec is `docs/specs/websocket-upgrade.md`.
- **Multi-node / multi-region CockroachDB.** Single-node is sufficient to prove the CDC and ordering
  properties; horizontal scale is an operational concern, not a correctness one.
- **Production authn/z, tenancy, billing.** Not part of the systems-design thesis. The read model is
  origin-allowlisted, rate-limited and capped, and the event spine can be pointed at a TLS+SASL
  broker — but nothing authenticates a *caller*, and SECURITY.md says so plainly rather than letting
  the controls imply otherwise.
- **A deployment pipeline.** No Terraform, no Helm, no Cloud Run config. Tagging `v*` publishes
  signed, attested images to GHCR; where they run is out of scope. A half-written deployment would be
  exactly the "compiles but never run" liability this project exists to kill.

## Known constraints & honest caveats

- **No CockroachDB license required.** The stack runs a single-node cluster, which under CockroachDB
  v24.3+ licensing needs no key (changefeeds included). crdb-init, the proof scripts, and CI run
  license-free and fail loudly if a key ever turns out to be required — they never skip silently. A free
  Enterprise license (`CRDB_LICENSE` / `CRDB_ORG`) is only relevant for a multi-node deployment.
- **The proofs are authored for CI first.** They run on any Docker host, including this machine now
  that Docker Desktop is installed — but CI is the authority, because only CI runs the full matrix on
  a clean machine. That ordering is deliberate: a proof validated only against a hand-held local
  environment is not a proof. (This bullet read "no local Docker daemon was used" for a month after
  that stopped being true, which is its own lesson about environment claims in documentation.)
- **Locked correctness core.** The DAG engine, CommBot core domain, the orchestrator schema and
  suspend/HITL logic, and the inventory valuation/ledger are treated as locked — changes to them are
  reviewed as regressions. The single sanctioned schema deviation is the additive
  `orchestrator_outbox.sequence_engine_key` column.

## Definition of done for the current milestone

**Met.** The repo is public, `master` is branch-protected, and the full suite — build/vet/`-race`
unit tests, lint, frontend (lint/typecheck/test/build), govulncheck as a gating CVE scan, the
testcontainers integration suite, six boot proofs and CodeQL — is green on every PR.

The next milestone is not more surface. It is the backlog in [`docs/kb/04-progress-ledger.md`]:
lease-TTL reclaim beyond the sweep, a DLQ re-drive tool, and a trace slice screenshot from the
observability profile. Each is small, each is provable, and none of them is a new service.
