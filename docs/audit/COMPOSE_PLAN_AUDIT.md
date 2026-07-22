# Sentinel Audit — Round: docker-compose + crdb-init plan (2026-07-23)

Auditor: Claude (Principal Sentinel). Builder: Antigravity (Gemini).
Subject: Antigravity's "docker-compose.yml and crdb-init Execution Plan".
Verdict: **REJECT as written.** Directionally correct, but will not stand up a *flowing* system. 5 hard stoppers + 3 code-wiring bugs the plan doesn't see.

## Baseline correction (the plan/persona assumed a stale stack)
- Debezium → **native CRDB changefeeds** (no Debezium in repo).
- Power BI/DAX → **Next.js web dashboard** over the live stream; `infrastructure/warehouse/*` is legacy reference.
- Pinecone/Neo4j/BigQuery/Snowflake/ADK/LiteLLM/K8s/Istio → deferred under **Make It Real**.
- Transport is **SSE** today (ADR, graded A-). A WebSocket upgrade is a *separate* future prompt, not part of this infra round.

## Findings (ranked)
- **F1 🔴 Enterprise license absent → changefeeds fail.** CRDB v24.3+ gates Kafka-sink changefeeds behind a license. `crdb_schema.sql:6` leaves it commented. `crdb-init` must `SET CLUSTER SETTING cluster.organization` + `enterprise.license` from env BEFORE any `CREATE CHANGEFEED`. User must obtain a free CockroachDB Enterprise Free/Trial key.
- **F2 🔴 inventory-intelligence ignores env.** `services/inventory-intelligence/cmd/main.go:24` hardcodes `postgres://user:pass@localhost:26257/omniflow`; `:38` hardcodes `SeedBrokers("localhost:9092")`. No env reads. Compose wiring is a no-op. REQUIRES code change.
- **F3 🔴 viz-gateway topic mismatch.** Inventory changefeed uses `topic_prefix='omniflow.inventory.'` → topic `omniflow.inventory.fact_inventory_movement`. viz-gateway (`cmd/main.go:48`) consumes bare `fact_inventory_movement`. Feed is dead. Snapshot topic not subscribed at all.
- **F4 🔴 Env-name schism.** commbot + orchestrator read `CRDB_DSN`; viz-gateway reads `DATABASE_URL` (`cmd/main.go:24`). "Unified CRDB_DSN" leaves viz-gateway on its defaultdb default. Compose must set `DATABASE_URL` for viz-gateway (or standardize the code on one name).
- **F5 🔴 Kafka healthcheck undefined.** Services gate on `kafka: service_healthy` but no healthcheck is defined. Add `kafka-broker-api-versions --bootstrap-server localhost:9092`.
- **F6 🟠 crdb-init idempotency too coarse.** `CREATE CHANGEFEED` is not idempotent; "any feed exists → skip all" leaves feeds permanently missing after a partial failure. Split idempotent DDL from changefeed creation; guard each feed independently via `[SHOW CHANGEFEED JOBS]` topic match.
- **F7 🟠 Kafka topic auto-create not guaranteed.** Set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` or pre-create the 4 topics in init.
- **F8 🟠 Duplicated/conflicting cluster settings.** `crdb_schema.sql:4-6` sets org='OmniFlow' inline; if it != the license org, F1 re-triggers. Centralize cluster settings in crdb-init; strip from schema files.
- **F9 🟡 KRaft listeners under-specified.** Advertised internal listener must be `kafka:29092` (matches changefeed sink + every `KAFKA_BROKERS`); needs controller listener + fixed CLUSTER_ID.
- **F10 🟡 golang:1.26 base image may not exist.** viz-gateway go.mod is go 1.26.5; verify/pin the Dockerfile base to a published tag.
- **F11 🟢 resolved=1s/min_checkpoint_frequency=1s** acceptable on single node for demo.

## BI/Viz
- Fact schema is dashboard-ready (pre-aggregated FIFO/moving-avg, seqkey STRING, cdc_emit_ts, HLC watermark). Inert until F3/F4 fixed.
- SSE stays for server→client. WebSocket only if client→server interactivity is wanted → separate audited prompt.

## Round 2 (2026-07-23) — Antigravity revised plan: CONDITIONAL APPROVE
All F1–F11 addressed. 4 new corrections required before build (addendum = BLOCK C in `docs/antigravity/REONBOARD_AND_PROMPT03.md`):
- **B1 🔴** Schema files co-locate CREATE TABLE + CREATE CHANGEFEED → "apply tables unconditionally + guard feeds" is impossible. Move all 4 CREATE CHANGEFEED out of the .sql files into crdb-init.sh as guarded heredocs; .sql become pure DDL.
- **B2 🔴** init container has no cockroach binary / no schema-file access. Run crdb-init FROM `cockroachdb/cockroach:v24.3.3` with `infrastructure/` bind-mounted read-only.
- **B3 🟠** Guard must check job STATE (running/pending only; recreate on failed/canceled). Fail loud if CRDB_LICENSE empty.
- **B4 🟠** Use `go 1.25` + `golang:1.25` for viz-gateway (not 1.24) — matches root, verifiable toolchain; Docker build can't be checked locally.
- **B5 🟡** Confirm frontend NEXT_PUBLIC_SSE_URL build arg; inventory rewrite preserves DisableAutoCommit + ConsumerGroup.

## QUEUED for Prompt 4 (E2E) — do not fix in compose round
- `viz-gateway/internal/kafka/consumer.go:112 handleP2PCompleted` reads top-level `po_id`/`status`/`sequence_engine_key`, but the orchestrator changefeed wraps them in an `after{}` envelope with protobuf body as `\x`-hex `payload` (handleInventoryMovement unwraps `after` correctly; this one doesn't). P2P nodes won't render until fixed.
- viz-gateway commit semantics: MarkCommitRecords with DisableAutoCommit — verify marks actually commit (or accept at-least-once re-read, fine for idempotent projection).

## Round 3 (2026-07-23) — Antigravity executed B1–B4; VERIFIED against disk
Go-side + compose: ALL CORRECT.
- B1 verified: no CREATE CHANGEFEED / SET CLUSTER SETTING left in .sql; changefeeds moved to crdb-init.sh heredocs, WITH-options preserved (resolved=1s).
- B4 verified: viz-gateway go.mod `go 1.25.0`, Dockerfile `golang:1.25`. Both modules `CGO_ENABLED=0 go build` exit 0.
- Wiring verified: viz topics fixed in main.go:48 + consumer.go:40/42; inventory main env-wired (CRDB_DSN/KAFKA_BROKERS with localhost defaults, ConsumerGroup + DisableAutoCommit preserved).
- compose at REPO ROOT with `context: .` — correct (Go Dockerfiles need root build context); `./infrastructure:/infrastructure:ro` mount + init from cockroachdb image = correct.

**crdb-init.sh had 3 hard blockers (script had never run — no local Docker — so undetected; Antigravity's "simulated boot" was fiction). PATCHED by Sentinel this round:**
- **C1 🔴** every `cockroach sql` lacked `--host=cockroachdb:26257` → crdb-init runs in a separate container; bare CLI hit localhost:26257 (empty) and the wait loop hung forever. Fixed: `CRDB="cockroach sql --insecure --host=cockroachdb:26257"` used everywhere.
- **C2 🔴** in-script Kafka wait shelled `/opt/kafka/bin/kafka-broker-api-versions.sh` inside the cockroach image (no Kafka tooling) → infinite loop. Fixed: removed both in-script waits; compose `depends_on: service_healthy` already gates readiness.
- **C3 🟠** guard query `WHERE statement LIKE` referenced a non-existent column. Fixed to `WHERE description LIKE`.

**Residual must-verify (cannot test w/o Docker):**
- Kafka healthcheck path `/opt/kafka/bin/kafka-broker-api-versions.sh` inside `apache/kafka-native:3.8.0` — confirm the native image ships the shell scripts there; if not, kafka never reports healthy and the whole stack blocks.
- `--accept-sql-without-tls` alongside `--insecure` on `start-single-node` — redundant; confirm cockroach doesn't reject it.
- These + the real E2E boot must run in GitHub Actions/Codespaces (Make-It-Real: the crucible must execute somewhere real).

## Required external action (user)
Obtain a free CockroachDB Enterprise license key (Enterprise Free tier) and provide `CRDB_LICENSE` + `CRDB_ORG` as env for crdb-init. https://www.cockroachlabs.com/get-cockroachdb/enterprise/
