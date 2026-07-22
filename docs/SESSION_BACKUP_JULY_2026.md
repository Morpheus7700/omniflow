# OmniFlow Session Backup — July 2026

*This file acts as a hardcopy backup of the conversation state to ensure no context is lost if the chat interface history fails to load.*

## 1. Current Architectural State (Audited & Scaffolded)
The event-driven core is successfully built across 5 phases, with all LLM Council bars met:
*   **Phase 1 & 2 (Infrastructure & CommBot)**: Kafka bus, CockroachDB ACID + native changefeeds, Go strict-hexagonal service with zero-trust quarantine and SSRF allowlist.
*   **Phase 3 (P2P DAG Orchestrator)**: Kahn topo-sort DAG engine, durable checkpointer, HITL lease management.
*   **Phase 4 (Inventory Intelligence)**: CRDB single-tx ledger, exact FIFO/moving-avg lot drawdown, HLC true restatement (time-travel replay), and CRDB → BigQuery Star Schema changefeed.
*   **Phase 5 (Real-Time Visualization)**: SSE Gateway + Next.js/React-Three-Fiber frontend. Uses `sequence_engine_key` as String for precision, explicit watermarks for jitter-free rendering, subscribe-then-snapshot to prevent gaps, and REST-driven time-travel controls.

**Latest Fixes Applied (Phase 5 A- Grade):**
*   `replay.go` now correctly queries the `workflows` table for the real P2P orchestrator log and allows unbounded history (`to_seq=0`).
*   `NetworkGraph.tsx` implements stable `InstancedMesh` slot mapping via `aggregate_id` to prevent scrambling, uses `<Text>` stage labels, encodes success/failure via Box/Sphere geometries (colorblind safe), and uses `metalness=0.1`.
*   `<Canvas>` uses `frameloop="always"` for continuous `lerp`.
*   SSE gateway applies a 2-second timeout instead of silently dropping watermarks under backpressure.

## 2. The Council Verdict: "Make It Real"
Following the Phase 5 completion, an adversarial audit by the Principal Council determined that adding more "blueprint surface" (Kubernetes, Istio, Neo4j, Pinecone, ADK) is the wrong move. The true Staff-level liability is that the system "compiles but has never been run end-to-end."

**The mandate is to stop building new services and instead prove that the existing architecture survives execution and failure.**

## 3. The Roadmap for the Next Session
When you return, we will execute the **Make It Real** phase (The Chairman's Synthesis):

1.  **The E2E Crucible (`docker-compose.yml`)**: Stand up Kafka, CockroachDB, the Go services, and Next.js locally. Automate the CRDB schema and changefeed initialization. Push one synthetic vendor email completely through the pipeline and watch the 3D node move on the UI.
2.  **Failure Integration Tests**: Write 2-3 targeted tests proving the bold durability claims:
    *   The checkpointer survives a killed pod.
    *   Exactly-once semantics hold on redelivery.
    *   FIFO ledger restates correctly on a late-arriving event.
3.  **CI/CD**: Add GitHub Actions for `buf generate`, `go build`, `go vet`, and `go test`.
4.  **Documentation**: Write `README.md` (one-command run instruction) and `SCOPE.md` (explicitly documenting the conscious architectural choice to defer K8s/Istio/Pinecone in favor of core durability proofs).
5.  **Observability (Optional but High-Leverage)**: Wire a thin OTel collector → Grafana/Tempo slice to visually prove the distributed tracing already embedded in the Go code.

## 4. Required Setup for Next Session
*   **Skills to Load**: Ensure the **CockroachDB** and **Grafana/Observability** Gemini skills are pulled into the workspace so the agent has deep expertise for wiring the local single-node cluster, changefeeds, and OTel collector.
