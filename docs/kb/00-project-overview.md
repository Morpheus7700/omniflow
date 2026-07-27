# 00 · Project Overview

**OmniFlow** = an Autonomous Procure-to-Pay & Supply-Chain Orchestrator. It is a **portfolio piece**
built to demonstrate Staff/Principal-level distributed-systems judgment — explicitly NOT a CRUD app
and NOT an LLM wrapper. Event-driven Go microservices.

Location: `C:\Users\Aniket Roy\Desktop\Work\Omniflow`. Owner: Aniket (aniketroy2k@gmail.com).

## The current mandate — "Make It Real"
The five phases were built and audited but **never actually run**. The liability we are killing is
"compiles but never run." Every current effort proves the core **boots end-to-end** and **survives
failure**, in CI (there is **no local Docker daemon** on this machine — see [[06-build-and-test]]).

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
- **Phase 5 · Real-Time Viz** — SSE gateway + Next.js/R3F 3D flow-graph, HLC-watermark jitter control.

## Key product decisions (settled — do not re-litigate)
- **Analytics = the same first-party Next.js dashboard** over the viz stream. No separate BI tier.
- **CDC = CockroachDB native JSON changefeeds.** NO Debezium, ever.
- **Kafka client = franz-go** (pure Go, no CGO) everywhere.
- **Browser transport = SSE** (deliberate ADR). A WebSocket upgrade is its own future audited prompt.

Related: [[02-architecture]] · [[03-locked-constraints]]
