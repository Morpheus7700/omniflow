# OmniFlow — Claude Code project context

Autonomous Procure-to-Pay & Supply-Chain Orchestrator. Event-driven Go microservices, portfolio
piece demonstrating Staff-level distributed-systems judgment. NOT a CRUD app or LLM wrapper.

## The working loop
Claude is the lead engineer: it edits the code on disk, opens PRs, and audits its own work against
CI. (Earlier rounds used Antigravity as a separate builder with Claude auditing its output; that
loop is retired, along with its builder prompts.)

`master` is **branch-protected**: 9 required checks, `strict: true` (branches must be up to date
before merging), `enforce_admins: true`. There are no direct pushes to `master` — every change goes
through a PR, including trivial ones. Approvals are not required, so a solo author can self-merge
once the suite is green; requiring code-owner review would be self-blocking, since GitHub forbids
self-approval.

Because `strict: true` forces a re-run per merge, prefer batching related changes into one branch
over many single-file PRs.

## Start here every session → the knowledge base
**`docs/kb/INDEX.md`** is an Obsidian-style vault — read it first to restore context cheaply instead
of re-deriving from code. `docs/kb/05-gotchas.md` is the highest-value note (the recurring bugs);
read it before editing scripts, consumers, or schema. Architecture decisions live in `docs/adr/`.

## Current mandate — "Make It Real"
Prove the existing core RUNS end-to-end and SURVIVES failure. STOP adding blueprint surface
(K8s/Istio/Neo4j/Pinecone/ADK/LiteLLM/a separate BI tier). The liability we kill is "compiles but
never run" — **never accept simulated logs as verification.** The real E2E boot must execute in
GitHub Actions — and now also locally: Docker Desktop is installed on this machine and
`bash scripts/e2e.sh` passes here (seed → changefeed → DAG → HITL → SSE, verified
2026-08-18). On Windows the scripts export `MSYS_NO_PATHCONV=1` themselves; without it Git
Bash rewrites in-container paths and every `docker compose exec` exits 127. CI remains the
authority — a local pass is a fast pre-check, not a substitute for the required checks.

## Locked decisions (do not re-litigate)
- **CDC = CockroachDB native JSON changefeeds.** No Debezium, ever.
- **Kafka client = franz-go** (pure Go, no CGO) for all services.
- **Analytics = the same first-party Next.js dashboard** over the viz stream. No separate BI tier, no
  warehouse hop — the facts are already computed and travel on the changefeed.
- **Browser transport = SSE** (deliberate ADR). A WebSocket upgrade, if ever, is its own audited prompt.
- **DB name = `omniflow`** for every service. `sequence_engine_key` is carried as a STRING end-to-end
  (avoids JS float64 precision loss). Rendering gated by the changefeed `resolved` watermark.

## Load-bearing files — change only with the test that proves you didn't break them
`services/p2p-orchestrator/internal/core/domain/dag.go`; CommBot core domain logic;
`infrastructure/storage/orchestrator_schema.sql` structure; orchestrator `service.go` suspend/HITL
logic; Phase-4 valuation/ledger (`valuation.go`, inventory crdb repository). The confirmed-DLQ→commit
ordering and manual-commit contract in every consumer are load-bearing — never switch to auto-commit
or fire-and-forget produce.

These were previously described as LOCKED, enforced by this paragraph alone. That was a documented
promise, not a control: a paragraph cannot fail a build, and it froze the files against *any* edit
including provably safe ones — a repo-wide `gofmt` was blocked for months by a lock guarding
`valuation.go` against whitespace it could not semantically alter.

The enforcement is the test suite, and `.github/CODEOWNERS` names which test guards which path.
So the question is never "am I allowed to touch this file" but **"which test proves I did not break
it"** — and a change no test objects to is either safe or a gap in the suite. Chase the second case.
(CODEOWNERS is visibility only here: GitHub forbids self-approval, so requiring code-owner review on
a solo repo would make every PR unmergeable.)

## Build / verify commands
- Root module:  `CGO_ENABLED=0 go build ./...`  and  `go vet ./...`  (must be exit 0)
- viz-gateway (separate module, go 1.25):  `cd services/viz-gateway && CGO_ENABLED=0 go build ./...`
- Stack (CI only):  `docker compose up`  — runs license-free (single-node CRDB v24.3+ needs no key,
  changefeeds included). `CRDB_LICENSE`/`CRDB_ORG` are OPTIONAL env, only for a multi-node cluster;
  crdb-init + scripts + CI run without them and fail loudly if a key is ever actually required.
  Kafka image = the **JVM** `apache/kafka` image, never `kafka-native`: the GraalVM native image
  ships no JRE, so the `kafka-broker-api-versions.sh` healthcheck cannot run and the stack hangs
  waiting for a broker that never reports healthy. (Pinned version lives in `docker-compose.yml`;
  Dependabot bumps it.)

## Repo facts
Two Go modules: root `omniflow` (go 1.25) + `services/viz-gateway` (go 1.25). Shared, non-domain
helpers live under `internal/platform/` in the root module — `errclass` holds the one SQLSTATE
taxonomy all three root-module services classify against. viz-gateway is a separate module and
cannot import it.
