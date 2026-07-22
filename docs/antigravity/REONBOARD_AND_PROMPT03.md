# Antigravity Re-Onboarding + Next Prompt (paste into Antigravity)

> Antigravity's chat history was lost in a laptop crash. Paste **BLOCK A** first to restore its
> context, wait for it to acknowledge, then paste **BLOCK B** (the actual task). One prompt at a
> time. Copy Antigravity's reply back to Claude (Sentinel) for audit before executing on disk.

---

## BLOCK A — Context restore (paste first)

You are the **builder** on OmniFlow, an Autonomous Procure-to-Pay & Supply-Chain Orchestrator
(event-driven Go microservices + Kafka KRaft + CockroachDB native JSON changefeeds + Next.js/R3F
3D viz). A separate Principal Auditor reviews everything you produce from disk before it's kept.

Read `OMNIFLOW_CONTEXT_FOR_GEMINI.md` at the repo root — it is the current, accurate knowledge map
(architecture, golden event flow, service-by-service file map, schemas, locked constraints). Treat
it as ground truth.

**Non-negotiable constraints for this session:**
- Current mandate = **"Make It Real."** STOP adding new services or blueprint surface
  (K8s/Istio/Neo4j/Pinecone/ADK/LiteLLM/BigQuery/Power BI). Prove the existing core RUNS end-to-end
  and SURVIVES failure. The liability we are killing is "compiles but never run."
- **No Power BI, no Debezium.** BI/analytics is a first-party Next.js web dashboard fed by the
  viz-gateway stream. CDC is CockroachDB's **native** changefeed — never introduce Debezium.
- **Transport stays SSE** for the server→client event stream this round. Do NOT switch to WebSocket.
- **Do not modify LOCKED code:** `dag.go`; CommBot core domain; `orchestrator_schema.sql` structure;
  orchestrator `service.go` suspend/HITL logic; Phase-4 valuation/ledger (`valuation.go`, inventory
  crdb repository). The confirmed-DLQ→commit ordering and manual-commit contract in every consumer
  are load-bearing — never switch to auto-commit or fire-and-forget produce.
- Repo builds clean today: `CGO_ENABLED=0 go build ./...` (root + services/viz-gateway) = exit 0,
  `go vet` clean, all services on franz-go. Dockerfiles exist for all 5 services. Do not regress this.

Acknowledge you've read the context map and these constraints. Then wait for the task.

---

## BLOCK B — TASK: make the stack actually stand up and flow (Prompt 3, corrected)

Goal: one command brings up a CorrectlyWired, E2E-capable stack. The current draft compose plan has
bugs that would boot containers connected to nothing. Do all three parts. Docker will NOT be run
locally — after implementing, output the final `docker-compose.yml`, `crdb-init.sh`, and the exact
diffs for the code fixes, plus a short "how a successful boot + changefeed verification looks."

### Part A — Fix 3 wiring bugs (code), or the stack boots into a void
1. `services/inventory-intelligence/cmd/main.go` is a scaffold: it hardcodes
   `postgres://user:pass@localhost:26257/omniflow` and `SeedBrokers("localhost:9092")`. Rewrite the
   composition root to read `CRDB_DSN` and `KAFKA_BROKERS` from env (same pattern as
   p2p-orchestrator), with sensible localhost defaults. No business-logic changes — only wiring.
2. `services/viz-gateway/cmd/main.go:48` subscribes to bare `fact_inventory_movement`, but the
   inventory changefeed publishes with `topic_prefix='omniflow.inventory.'`. Change the subscription
   to `omniflow.inventory.fact_inventory_movement` (and add `omniflow.inventory.fact_inventory_snapshot`
   if the gateway is meant to surface valuation snapshots). Update the topic-routing switch to match.
3. Standardize the DSN env name OR set both: commbot + orchestrator use `CRDB_DSN`; viz-gateway uses
   `DATABASE_URL`. In compose, set `DATABASE_URL` for viz-gateway explicitly so it does NOT fall back
   to its `defaultdb` default. Every service must point at DB **omniflow**.

### Part B — `infrastructure/init/crdb-init.sh` (idempotent, license-aware)
- `restart: "no"`; runs after CRDB healthy AND Kafka healthy.
- FIRST, set cluster settings from env (once): `kv.rangefeed.enabled=true`,
  `cluster.organization='${CRDB_ORG}'`, `enterprise.license='${CRDB_LICENSE}'`.
  **Kafka-sink changefeeds are a licensed enterprise feature in CRDB v24.3+ — without this the
  changefeeds fail to create.** Fail loudly with a clear message if `CRDB_LICENSE` is empty.
- `CREATE DATABASE IF NOT EXISTS omniflow`.
- **Split idempotent DDL from changefeed creation.** Apply all `CREATE TABLE IF NOT EXISTS` DDL
  unconditionally (safe to re-run). Then create EACH changefeed only if a feed for that target/topic
  is not already present in `[SHOW CHANGEFEED JOBS]` — guard each of the 4 topics independently, not
  "any feed exists → skip all". (`CREATE CHANGEFEED` is not idempotent; duplicates must be avoided.)
- Also strip the inline `SET CLUSTER SETTING` lines from `infrastructure/crdb_schema.sql:4-6` so the
  org/license are owned in exactly one place (this script) — a mismatched org string breaks license
  validation.
- Update `resolved` + `min_checkpoint_frequency` from `10s`→`1s` in all three schema files.

### Part C — `infrastructure/docker-compose.yml`
- **CockroachDB** `cockroachdb/cockroach:v24.3.3`, `--insecure`, single node. Healthcheck:
  `cockroach sql --insecure -e 'SELECT 1'`.
- **Kafka (KRaft)**: single broker, `kafka-data` volume, no `version:` key. Advertised **internal**
  listener MUST be `kafka:29092` (matches changefeed sink URIs and every service's `KAFKA_BROKERS`);
  add a controller listener + fixed `CLUSTER_ID`; set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.
  Define a real healthcheck: `kafka-broker-api-versions --bootstrap-server localhost:9092`.
- Remove schema-registry and redis. No warehouse/BI service.
- `crdb-init` `depends_on`: roach `service_healthy`, kafka `service_healthy`.
- The 5 app services `depends_on: crdb-init: condition: service_completed_successfully`.
- Env: every Go service gets `CRDB_DSN=postgres://root@roach:26257/omniflow?sslmode=disable` (and
  viz-gateway ALSO `DATABASE_URL=` the same); all get `KAFKA_BROKERS=kafka:29092` (commbot uses
  `KAFKA_BOOTSTRAP` — set that name for commbot). Frontend gets `NEXT_PUBLIC_SSE_URL` as build arg.
- Verify each Dockerfile's Go base image tag actually exists (viz-gateway go.mod is go 1.26.5 — if
  `golang:1.26` is unpublished, pin to the latest real tag and set `go.mod` accordingly).
- Publish ports: CRDB 26257/8080, Kafka 9092, viz-gateway SSE, frontend 3000.

### Deliverables
Output final `docker-compose.yml`, `crdb-init.sh`, the three Part-A code diffs, the schema edits,
and a short simulated successful-boot + `SHOW CHANGEFEED JOBS` verification. Do not claim it ran.

---

## BLOCK C — Prompt 3 ADDENDUM (round 2 corrections; paste after Antigravity's revised plan)

Your revised plan is approved to build with 4 required corrections. Apply these exactly:

1. **Move changefeeds out of the schema files into `crdb-init.sh`.** The three `.sql` files currently
   co-locate `CREATE TABLE` and `CREATE CHANGEFEED` (`crdb_schema.sql:71`, `orchestrator_schema.sql:43`,
   `commbot_outbox_schema.sql:29`). You cannot "apply tables unconditionally" from a file that also
   contains a changefeed. So: DELETE the `CREATE CHANGEFEED` blocks from all three `.sql` files
   (leave pure idempotent DDL). Recreate all four changefeeds (commbot_outbox, orchestrator_outbox,
   fact_inventory_movement, fact_inventory_snapshot) as individually-guarded heredoc blocks inside
   `crdb-init.sh`. Preserve every WITH option exactly (format=json, key_column/topic_name or
   topic_prefix, unordered, updated, mvcc_timestamp, resolved='1s', min_checkpoint_frequency='1s',
   protect_data_from_gc_on_pause).

2. **Package the init container correctly.** The `crdb-init` service must run FROM
   `cockroachdb/cockroach:v24.3.3` (so the `cockroach` binary exists) with `../infrastructure`
   bind-mounted read-only into the container so the script can read the `.sql` files.

3. **Guard on job STATE, and fail loud on missing license.** In `crdb-init.sh`: first assert
   `CRDB_LICENSE` is non-empty and exit 1 with a clear message if not. When guarding each changefeed,
   treat only `running`/`pending` jobs for that topic as "already exists"; if the matching job is
   `failed`/`canceled`, recreate it.

4. **Use Go 1.25, not 1.24.** Set viz-gateway `go.mod` to `go 1.25` and its Dockerfile base to
   `golang:1.25` (matches the root module, which is a toolchain we know compiles; 1.24 is a larger,
   unverifiable downgrade since Docker can't be built locally).

Also confirm (no change if already true): frontend still receives `NEXT_PUBLIC_SSE_URL` as a build
arg; the inventory `main.go` rewrite keeps `DisableAutoCommit()` + `ConsumerGroup("inventory-intelligence-v1")`
(env wiring only, no consumer-semantics change).

Do NOT fix the `handleP2PCompleted` envelope-unwrap bug in this round — it's queued for the E2E prompt.
