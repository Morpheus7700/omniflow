# 00 · Project Overview

**OmniFlow** = an Autonomous Procure-to-Pay & Supply-Chain Orchestrator. It is a **portfolio piece**
built to demonstrate Staff/Principal-level distributed-systems judgment — explicitly NOT a CRUD app
and NOT an LLM wrapper. Event-driven Go microservices.

Location: `C:\Users\Aniket Roy\Desktop\Work\Omniflow`. Owner: Aniket (aniketroy2k@gmail.com).

## The current mandate — "Make It Real"
The five phases were built and audited but **never actually run**. The liability we killed is
"compiles but never run": the core now boots end-to-end and survives failure, proven by six boot
jobs on real infrastructure. CI is the authority; the stack also runs locally (Docker Desktop is
installed on this box — verify with `docker info` before relying on it, see [[06-build-and-test]]).

STOP adding blueprint surface (K8s/Istio/Neo4j/Pinecone/ADK/LiteLLM/a separate BI tier). Depth over
breadth. See [[04-progress-ledger]] for what's done.

## What exists (5 phases, all previously audited)
- **Phase 1** — infra/schema/proto baseline.
- **Phase 2 · CommBot** — classifies inbound vendor emails, zero-trust quarantine, SSRF allowlist,
  token bucket, transactional outbox. See [[05-gotchas#CommBot cannot run in CI]].
- **Phase 3 · P2P Orchestrator** — DAG workflow engine, Kahn topo-sort, durable checkpointer,
  human-in-the-loop (HITL) suspend/resume, exactly-once outbox CTE.
- **Phase 4 · Inventory Intelligence** — single-tx FIFO/moving-avg ledger, HLC late-arrival
  restatement, star-schema facts. See [[05-gotchas#HLC seq vs occurred_at]].
- **Phase 5 · Real-Time Viz** — SSE gateway + a Next.js **settlement-ledger dashboard**. (It was a
  React-Three-Fiber 3D flow-graph until 2026-07-27; the redesign replaced it with a document-of-record
  ledger whose signature element is the labelled settlement line at the changefeed watermark. There is
  no 3D, no R3F, and no HUD anywhere in `frontend/` — if a note says otherwise it predates that.)

## Key product decisions (settled — do not re-litigate)
- **Analytics = the same first-party Next.js dashboard** over the viz stream. No separate BI tier.
- **CDC = CockroachDB native JSON changefeeds.** NO Debezium, ever.
- **Kafka client = franz-go** (pure Go, no CGO) everywhere.
- **Browser transport = SSE** (deliberate ADR). A WebSocket upgrade is its own future audited prompt.

Related: [[02-architecture]] · [[03-locked-constraints]]
