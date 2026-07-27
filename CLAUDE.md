# OmniFlow — Claude Code project context

Autonomous Procure-to-Pay & Supply-Chain Orchestrator. Event-driven Go microservices, portfolio
piece demonstrating Staff-level distributed-systems judgment. NOT a CRUD app or LLM wrapper.

## The working loop
Antigravity (Gemini IDE) is the **builder** (edits files on disk). Claude is the **Principal
Sentinel/Auditor** — audits from disk each round, patches contained bugs, returns a verdict + a
redirection prompt the user pastes back to Antigravity. Canonical state: `docs/audit/STATE.md`,
audit trail: `docs/audit/COMPOSE_PLAN_AUDIT.md`, builder prompts: `docs/antigravity/`.
Full architecture map: `OMNIFLOW_CONTEXT_FOR_GEMINI.md`.

## Start here every session → the knowledge base
**`docs/kb/INDEX.md`** is an Obsidian-style vault for both Claude and Antigravity — read it first to
restore context cheaply instead of re-deriving from code. `docs/kb/05-gotchas.md` is the highest-value
note (the recurring bugs); read it before editing scripts, consumers, or schema.

## Current mandate — "Make It Real"
Prove the existing core RUNS end-to-end and SURVIVES failure. STOP adding blueprint surface
(K8s/Istio/Neo4j/Pinecone/ADK/LiteLLM/a separate BI tier). The liability we kill is "compiles but
never run" — **never accept simulated logs as verification.** The real E2E boot must execute in
GitHub Actions or Codespaces (no local Docker daemon available on this machine).

## Locked decisions (do not re-litigate)
- **CDC = CockroachDB native JSON changefeeds.** No Debezium, ever.
- **Kafka client = franz-go** (pure Go, no CGO) for all services.
- **Analytics = the same first-party Next.js dashboard** over the viz stream. No separate BI tier, no
  warehouse hop — the facts are already computed and travel on the changefeed.
- **Browser transport = SSE** (deliberate ADR). A WebSocket upgrade, if ever, is its own audited prompt.
- **DB name = `omniflow`** for every service. `sequence_engine_key` is carried as a STRING end-to-end
  (avoids JS float64 precision loss). Rendering gated by the changefeed `resolved` watermark.

## LOCKED files — do not modify (regressions fail audit)
`services/p2p-orchestrator/internal/core/domain/dag.go`; CommBot core domain logic;
`infrastructure/storage/orchestrator_schema.sql` structure; orchestrator `service.go` suspend/HITL
logic; Phase-4 valuation/ledger (`valuation.go`, inventory crdb repository). The confirmed-DLQ→commit
ordering and manual-commit contract in every consumer are load-bearing — never switch to auto-commit
or fire-and-forget produce.

## Build / verify commands
- Root module:  `CGO_ENABLED=0 go build ./...`  and  `go vet ./...`  (must be exit 0)
- viz-gateway (separate module, go 1.25):  `cd services/viz-gateway && CGO_ENABLED=0 go build ./...`
- Stack (CI only):  `docker compose up`  — runs license-free (single-node CRDB v24.3+ needs no key,
  changefeeds included). `CRDB_LICENSE`/`CRDB_ORG` are OPTIONAL env, only for a multi-node cluster;
  crdb-init + scripts + CI run without them and fail loudly if a key is ever actually required.
  Kafka image = JVM `apache/kafka:3.8.0` (NOT kafka-native — no JRE for the healthcheck).
- The Sentinel change-watcher (`scratchpad/omniflow-watch.sh`) polls the tree every 10s and only
  wakes Claude when files actually change — never poll from the model itself.

## Repo facts
Two Go modules: root `omniflow` (go 1.25) + `services/viz-gateway` (go 1.25). `git init`'d, baseline
commit `388cc35`; the audit loop reads Antigravity's edits via `git status`/`git diff` and never
commits (leaves diffs for the user to review).
