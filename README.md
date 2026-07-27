# OmniFlow

**An autonomous Procure-to-Pay & supply-chain orchestrator built on an event-sourced spine.**

OmniFlow ingests vendor communication, drives a procurement workflow through a durable DAG with a
human-in-the-loop approval gate, maintains a FIFO / moving-average inventory ledger that survives
out-of-order events, and streams the whole thing to a 3D timeline in the browser — all over a single
transactional-outbox → change-data-capture spine, with **no dual writes and no Debezium**.

It is a systems-design portfolio piece, not a CRUD app or an LLM wrapper. The emphasis is on the hard
parts of distributed systems: exactly-once effects, durable resume, ordering under late arrival, and
proving those properties actually hold by booting the real stack in CI.

---

## Why it's interesting

- **Transactional outbox + native CDC, no dual-write.** Every service writes its business row and its
  outbox row in one CockroachDB transaction. A **CockroachDB native JSON changefeed** turns those rows
  into Kafka events. There is no application code that writes to the DB *and* to Kafka — the changefeed
  is the only bridge, so a crash can never half-publish.
- **Durable, resumable workflow.** The P2P orchestrator builds a DAG, topo-sorts it (Kahn), and
  checkpoints node execution to CRDB. Kill the orchestrator mid-flight and restart it — it resumes from
  the last durable checkpoint and still completes exactly once.
- **Human-in-the-loop suspend/resume.** The DAG suspends at the `human_approval` node and writes nothing
  downstream until an approval event arrives; a duplicate approval is a no-op.
- **Ordering under late arrival.** Inventory valuation is keyed on a Hybrid Logical Clock
  (`sequence_engine_key`). A receipt that arrives *after* a later consumption triggers a **FIFO
  restatement**: the affected SKU is replayed in HLC order and the cost basis is corrected.
- **Precision-safe end to end.** `sequence_engine_key` is carried as a **STRING** across every hop to
  avoid JavaScript `float64` precision loss on 64-bit keys; the 3D client orders on the changefeed
  `resolved` watermark.

---

## Architecture

```
                    ┌──────────────┐
  vendor email ───► │   commbot    │──┐  (zero-trust quarantine, SSRF allowlist,
  (communication.v1)└──────────────┘  │   DoW token bucket, tx outbox)
                                       │ writes commbot_outbox
                                       ▼
                              CockroachDB ── native JSON changefeed ──► Kafka
                                       │                     (orchestration.v1)
                                       ▼
                    ┌──────────────────────────┐
   approval.v1 ───► │      p2p-orchestrator     │  Kahn topo-sort DAG · durable
                    │  (DAG · HITL suspend)     │  checkpointer · HITL lease
                    └──────────────────────────┘
                                       │ writes orchestrator_outbox (exactly-once CTE)
                                       ▼
                              CockroachDB ── changefeed ──► Kafka (p2p.completed.v1)
                                       │
  raw movement ──► ┌──────────────────────────┐            │
 (inventory.       │  inventory-intelligence   │            │
  movement.v1)     │  single-tx FIFO / moving  │            │
                   │  avg · HLC restatement    │            │
                   └──────────────────────────┘            │
                                       │ writes fact_inventory_* ─ changefeed ─┤
                                       ▼                                        ▼
                                                              ┌──────────────────────┐
                                                              │     viz-gateway      │
                                                              │  SSE /api/stream     │
                                                              └──────────────────────┘
                                                                         │
                                                              ┌──────────────────────┐
                                                              │  frontend (Next.js /  │
                                                              │  React-Three-Fiber)   │
                                                              └──────────────────────┘
```

### Services

| Service | Language | Consumes | Produces (via outbox → changefeed) | Owns in CRDB |
|---|---|---|---|---|
| **commbot** | Go | `omniflow.communication.v1` | `omniflow.orchestration.v1` | `commbot_outbox` |
| **p2p-orchestrator** | Go | `omniflow.orchestration.v1`, `omniflow.p2p.approval.v1` | `omniflow.p2p.completed.v1` | `workflows`, `node_execution_ledger`, `orchestrator_outbox` |
| **inventory-intelligence** | Go | `omniflow.inventory.movement.v1` | `omniflow.inventory.fact_inventory_movement`, `…_snapshot` | `fact_inventory_movement`, `fact_inventory_snapshot` |
| **viz-gateway** | Go (separate module) | `omniflow.p2p.completed.v1`, `omniflow.inventory.fact_inventory_*` | SSE `/api/stream` | — (read model) |
| **frontend** | Next.js / R3F | SSE `/api/stream` | 3D timeline | — |

### The event spine

| Topic | Origin | Consumer |
|---|---|---|
| `omniflow.communication.v1` | external vendor email (or seeder `email` mode) | commbot |
| `omniflow.orchestration.v1` | `commbot_outbox` changefeed | p2p-orchestrator |
| `omniflow.p2p.approval.v1` | HITL approval (seeder / UI) | p2p-orchestrator |
| `omniflow.p2p.completed.v1` | `orchestrator_outbox` changefeed | viz-gateway |
| `omniflow.inventory.movement.v1` | external movement ingress (raw protobuf) | inventory-intelligence |
| `omniflow.inventory.fact_inventory_movement` / `…_snapshot` | inventory fact changefeeds (`topic_prefix=omniflow.inventory.`) | viz-gateway |
| `…v1.dlq` (communication, orchestration, inventory.movement) | consumer dead-letter routing | operator / triage |

**Topics are created explicitly, never by broker auto-create.** `infrastructure/init/kafka-init.sh`
provisions all ten topics before any service starts, and `crdb-init` waits for it. This is not
belt-and-braces: **franz-go does not set `allow_auto_topic_creation` in its Metadata request** unless
`kgo.AllowAutoTopicCreation()` is passed, so `KAFKA_AUTO_CREATE_TOPICS_ENABLE` on the broker is inert
for every Go client here. The `.dlq` topics are the reason it matters most — each consumer withholds
its offset commit until dead-letter delivery is confirmed, so a missing `.dlq` topic would wedge that
partition permanently on the first poison message.

---

## Tech stack & decisions (ADRs in brief)

- **CockroachDB v24.3 native JSON changefeeds** for CDC. Not Debezium — the database is the CDC engine.
  No license key needed: a single-node v24.3+ cluster runs Kafka-sink changefeeds license-free.
- **Kafka (KRaft, single-node)** as the event bus, on the JVM image `apache/kafka:3.8.0` — deliberately
  *not* `apache/kafka-native`, whose GraalVM image ships no JRE and so cannot run the shell-script
  health probe, leaving every boot job hanging.
- **franz-go** as the Kafka client — pure Go, `CGO_ENABLED=0`, static binaries, no `librdkafka`.
- **Protobuf + `buf.validate`** for contracts; validation at the boundary.
- **OpenTelemetry** with W3C `traceparent` propagated end to end.
- **`sequence_engine_key` as STRING** across every hop (HLC key; avoids JS `float64` precision loss).
- **SSE** for the browser transport (deliberate ADR — a read-only broadcast stream needs nothing more;
  a WebSocket upgrade is scoped as separate future work).
- **First-party Next.js dashboard** for analytics, reading the same SSE stream the operational view
  uses. There is no separate BI tier and no warehouse hop: the facts a buyer asks about are already
  computed in `fact_inventory_*` and arrive over the changefeed, so a second analytics stack would
  duplicate state and add a staleness window for no gain at this size.

---

## Proving it's real

The mandate for this repo is **"Make It Real"** — not "compiles," but *runs end to end and survives
failure*, proven by booting the actual stack in CI. There are **no simulated logs.**

Verification is tiered, fastest first, so a regression is caught at the cheapest level that can see it.

**Tier 1 — unit (no Docker, sub-second, every push).** The domain is strict-hexagonal, so the
highest-risk logic is exercised through its ports with in-memory fakes:

| Package | What it pins |
|---|---|
| `inventory-intelligence/…/domain` | FIFO depletion across cost layers, value-weighted moving average, HLC late-arrival restatement, bounded-lateness horizon, over-consumption as terminal + rollback. Decimals compared numerically, never as strings. |
| `p2p-orchestrator/…/domain` | Kahn topo-sort determinism across repeated runs over freshly built maps (Go randomizes map iteration, so this is a real assertion), plus cycle detection joining `ErrTerminal` so a cyclic workflow goes straight to the DLQ instead of retrying forever. |
| `viz-gateway/internal/kafka` | Changefeed decode: the `after{}` envelope must be unwrapped and a top-level payload **dropped** rather than projected empty; `UseNumber` must preserve a 19-digit HLC key that a default JSON decode corrupts. Asserted through a real SSE broker, so the wire format is covered too. |
| `commbot/…/outbound/llm` | The zero-trust quarantine boundary: allowlist checked *before* DNS, suffix/prefix/userinfo confusion rejected, and an allowlisted host that resolves inward (cloud-metadata `169.254.169.254`, loopback, RFC1918) still refused. |

**Tier 2 — integration (`-tags=integration`, ephemeral real CockroachDB via testcontainers).** Covers
what a fake cannot honestly assert, because the guarantee *is* the SQL: single-transaction atomicity
(including that the idempotency marker rolls back — otherwise a failed event could never be retried),
the exactly-once guard under redelivery, lot ordering by `sequence_engine_key`, and DECIMAL/INT8
fidelity across the wire. It applies the shipped `infrastructure/crdb_schema.sql` rather than
duplicating DDL, so the test cannot drift from what deploys.

**Tier 3 — full-stack boot proofs.** Each stands up Kafka + CockroachDB + every service with
`docker compose` and asserts an invariant on the real wire:

| CI job | Script | Invariant proven |
|---|---|---|
| `boot stack + seed E2E` | `scripts/e2e.sh` | one seeded procurement event flows through the real changefeed spine + HITL suspend/resume and its `sequence_engine_key` reaches the SSE stream |
| `test durable checkpoint resume` | `scripts/failtest_killed_pod.sh` | orchestrator killed mid-workflow resumes from its durable checkpoint and completes exactly once |
| `test exactly-once delivery` | `scripts/failtest_exactly_once.sh` | a duplicate approval produces no extra outbox rows, ledger entries, or workflows |
| `test FIFO late-arrival restatement` | `scripts/failtest_fifo_restatement.sh` | a receipt arriving after a later consumption restates the SKU's FIFO cost basis in HLC order |
| `test DLQ poison-pill routing` | `scripts/failtest_dlq_poison.sh` | an undecodable message is routed to the `.dlq` topic, its source offset is committed, and a valid message behind it still processes — i.e. the partition is not wedged |

The tiers deliberately overlap on the FIFO restatement invariant — Tier 1 proves the arithmetic in
milliseconds, Tier 2 proves it survives a real DECIMAL/INT8 round trip, Tier 3 proves it survives the
whole changefeed spine. When it breaks, which tier goes red tells you where.

---

## Running it

### Build and test (no Docker needed)

Two Go modules, both build with the Kafka client compiled statically:

```bash
# root module (commbot, p2p-orchestrator, inventory-intelligence, tools)
CGO_ENABLED=0 go build ./... && go vet ./... && go test ./...

# viz-gateway is its own module
cd services/viz-gateway && CGO_ENABLED=0 go build ./... && go vet ./... && go test ./...
```

`go test ./...` runs Tier 1 only — no Docker, no services, sub-second. The integration suite is behind
a build tag so it cannot slow that path down or require a daemon to be present:

```bash
go test -tags=integration ./...   # Tier 2: pulls and boots a real CockroachDB per suite
```

### Boot the full stack (CI, or any Docker host)

No license key required. The stack runs a **single-node** CockroachDB, which under v24.3+ licensing
needs no key — changefeeds included. Just boot it:

```bash
bash scripts/e2e.sh    # boots the stack, seeds one event end to end, asserts, tears down
```

The same script runs unchanged in GitHub Actions. If you ever run against a multi-node cluster (which
*does* require a license), set `CRDB_LICENSE` / `CRDB_ORG` in the environment (or as repo secrets) and
the stack picks them up automatically; a free Enterprise license is available for individuals and
companies under $10M revenue at <https://www.cockroachlabs.com/get-cockroachdb/enterprise/>.

### Cloud Deployments (CockroachDB Serverless & Cloud Run)

To deploy to GCP Cloud Run and use CockroachDB Serverless (which offers a generous free tier of 5 GiB storage and 50M RUs/month), inject the connection string as `DATABASE_URL` (and `CRDB_DSN`) into your environment or CI pipeline:

```bash
export DATABASE_URL="postgresql://<user>:<password>@<your-serverless-host>:26257/omniflow"
export CRDB_DSN="${DATABASE_URL}"

# Only for a MULTI-node cluster; a single node needs neither of these:
# export CRDB_LICENSE="…"   CRDB_ORG="…"
```

Every service reads its DSN and broker list from the environment, so nothing assumes `localhost`.
Keep these in your platform's secret store — never in the repo (`.gitignore` covers `.env*`).

### The frontend is a frontend

`frontend/` is presentation-only, and that boundary is deliberate: no API routes, no server actions,
no Node/filesystem/database imports, no committed data files, and no credentials. Its **only** contact
with the system is the viz-gateway over SSE plus a replay `GET` — so a compromised browser bundle
exposes no more than the read-model already broadcasts.

The gateway origin comes from one build argument, `NEXT_PUBLIC_API_BASE` (see
`frontend/src/lib/config.ts`), defaulting to `http://localhost:8081`. Two things to know:

- **Port 8081, not 8080.** compose maps viz-gateway `8081:8080`; host `8080` is CockroachDB's admin UI.
- `NEXT_PUBLIC_*` is **inlined at build time and frozen into the image**, so pointing a deployment at a
  different gateway needs a rebuild, not a restart with new env. That is why it is an `ARG` on the
  builder stage rather than an `ENV` on the runner.


---

## Repository layout

```
contracts/                 protobuf contracts + generated Go
services/
  commbot/                 vendor comms ingest & classification
  p2p-orchestrator/        DAG workflow engine + HITL
  inventory-intelligence/  FIFO / moving-avg ledger
  viz-gateway/             SSE read model (separate Go module)
frontend/                  Next.js / React-Three-Fiber 3D timeline
infrastructure/            CRDB schema · crdb-init (changefeeds) · kafka-init (topics)
tools/                     seed (E2E harness) · mock-llm
scripts/                   e2e + three failure-survival proofs
.github/workflows/         CI: build/vet/unit · integration · four boot proofs · security scan
docs/                      knowledge base (docs/kb), audits, ADR trail
```

See [`SCOPE.md`](SCOPE.md) for what is and isn't in scope, and `docs/kb/INDEX.md` for the full
knowledge base.
