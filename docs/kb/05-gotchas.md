# 05 · Gotchas & Hard-Won Findings

**The highest-value note.** Every one of these cost a debugging round. Read before editing scripts,
consumers, or schema. Most are things [[01-working-loop|Antigravity]] gets wrong because it can't run
the stack.

## Infra / scripting (recurring in every failtest)
- **DB compose service name is `cockroachdb`, NOT `crdb`.** `docker compose exec -T cockroachdb …`.
- **Ports:** CRDB admin UI = `8080`; **viz SSE = `8081`**. Curling `:8080/api/stream` hits the CRDB UI
  and silently never matches. The SSE stream must be curled at `localhost:8081/api/stream`.
- **crdb-init readiness:** it prints the literal line `crdb-init complete.` (not "Init complete").
  Prefer `docker compose wait crdb-init` + exit-code check over log-grepping.
- **SSE has NO backlog** — the gateway broadcasts live to connected clients only. In any test you must
  open the `curl -N` stream **before** triggering the event, or you miss it.
- **Guard `set -u` traps**: init `SSE_PID=""` and guard `kill`, or an early failure aborts cleanup and
  leaks the stack (skips `down -v`).

## Data-model traps
- **`orchestrator_outbox.aggregate_id` = the workflow UUID (`wf.ID`), NOT the event_id.** To find a
  workflow's outbox rows: `WHERE aggregate_id = (SELECT id FROM workflows WHERE event_id=…)`. The
  event_id (from the VendorEmailReceived) is a *different* UUID.
- **Changefeed envelope shape** = `{"after":{…columns…}}` (top-level `after`, columns inside). NOT
  `{"payload":{"after":…}}`. A BYTES column renders as a `\x`-hex string. See [[02-architecture]].
- **DECIMAL never string-compares reliably.** shopspring/CRDB may serialize `25.00/5` as `5`,
  `5.0000000000000000`, etc. Assert numerically **inside SQL**: `WHERE fifo_unit_cost = 5.00` and
  count rows — never `[[ "$x" == "5.00" ]]` on CSV output.
- **SQL string literals in a double-quoted `-e "…"`**: use plain single quotes `sku='SKU-TEST-001'`.
  The shell-escape `'\''` is wrong there and breaks the SQL.

## Domain logic
### CommBot cannot run in CI
`ProcessVendorEmail` → `ClassifyIntent` fetches subject/body over **https from an allowlisted,
public-IP-resolving host** (the SSRF guard) BEFORE calling the LLM. Impossible inside Docker's private
network. So the E2E seeds at the **`commbot_outbox` table** (`SEED_MODE=outbox`, the default), letting
the real changefeed drive everything downstream. CommBot's classify hop is covered by its own tests.
(`email` mode exists but needs a real public https quarantine URL.)

### HITL suspend needs an approval
The orchestrator DAG (`human_approval` → `final_step`) **suspends** at `human_approval` and writes no
outbox row. It only emits `orchestrator_outbox` rows (approved + completed = 2 rows) after an approval
event on `omniflow.p2p.approval.v1`. The seeder polls `workflows.state='SUSPENDED'` before approving
(deterministic; avoids racing the changefeed).

### HLC seq vs occurred_at (the FIFO restatement crux)
Inventory late-arrival: **`sequence_engine_key` (HLC)** controls FIFO order + triggers restatement
(`e.SequenceEngineKey < maxSeq(lots)` → `restateSKUFrom` = ResetLots + full HLC-ordered replay).
**`occurred_at`** controls the 30-day lateness horizon; beyond it, a late event becomes an
`adjustment` instead of a restatement. So a restatement test must give the late event a **low seq but
a `now` timestamp**. Backdating `occurred_at` to match the low seq would silently test the wrong branch.

### Additive column exception
`orchestrator_outbox.sequence_engine_key INT8` was added (Prompt 4) as a Sentinel-authorized exception
to the schema lock, populated from the workflow in the exactly-once CTE. The `insert_ledger` guard
stayed byte-for-byte. This is the ONLY sanctioned deviation from [[03-locked-constraints]].

## Docker build contexts (monorepo, two Go modules)
- **viz-gateway is its OWN module → compose build `context: services/viz-gateway`, `dockerfile: Dockerfile`.**
  Its Dockerfile does `COPY go.mod go.sum` + `go build ./cmd` relative to the module. Giving it the
  repo-root `context: .` (as the root-module services correctly use) makes the build fail with
  `stat /app/cmd: directory not found` and copies the wrong module's go.mod. The root-module services
  (commbot, inventory-intelligence, p2p-orchestrator, tools/mock-llm) DO use `context: .` with full
  `./services/X/cmd` paths — that's the *other* case; don't unify them. `frontend` likewise gets
  `context: ./frontend`. (Only a real CI `docker compose build` reveals this — local `go build ./...`
  passes because it respects module boundaries; Docker's `COPY . .` does not.) Fixed 2026-07-24.

## Kafka KRaft config (crash-on-boot, exit 1 in ~1.5s)
- **Multiple broker-facing listeners with the same protocol REQUIRE `KAFKA_INTER_BROKER_LISTENER_NAME`.**
  Our compose has `PLAINTEXT` (host, `localhost:9092`) + `INTERNAL` (in-network, `kafka:29092`), both
  PLAINTEXT-protocol. Without naming the inter-broker listener Kafka can't disambiguate and exits(1)
  before the healthcheck ever runs — so the failure looks like `dependency failed to start: kafka
  exited (1)`, NOT a hang. Set `KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL` (every service + changefeed
  sink connects to `kafka:29092`). Fixed 2026-07-24, first real CI boot.
- **Failtest scripts must dump `docker compose logs` to STDOUT on failure, not to a file.** The 3
  failtests wrote `docker compose logs > X.log` (invisible in CI) → a boot crash was completely opaque.
  Now they mirror `e2e.sh`: `local code=$?` in the trap, and on non-zero dump `docker compose logs
  --no-color --tail=200` to stdout. Without this you're blind to any container that dies during boot.

## Kafka image
- **Use the JVM image `apache/kafka:3.8.0`, NOT `apache/kafka-native`.** The native (GraalVM) image
  ships no JRE, so the `kafka-broker-api-versions.sh` healthcheck (a Java-launching shell script) can't
  run → kafka never reports healthy → `crdb-init` (gated on `kafka: service_healthy`) never starts →
  every boot job hangs to the CI cap. The JVM image ships the same CLI scripts *with* a JRE. (Fixed
  2026-07-24; see `docs/audit/GROUNDING_AUDIT_2026-07-24.md` finding #1.)

## CockroachDB specifics
- **Changefeeds run license-free on a single-node cluster** (`start-single-node`, what compose runs) under
  v24.3+ licensing. `CRDB_LICENSE`/`CRDB_ORG` are OPTIONAL env, needed only for a *multi-node* cluster.
  crdb-init + scripts + CI attempt changefeed creation unconditionally and **fail loudly** if a key is
  ever actually required — they never silently skip. See [[07-the-gate]].
- crdb-init runs in a *separate* cockroach container (CLI only) → every `cockroach sql` needs
  `--host=cockroachdb:26257`, else it hangs on localhost.
- `SHOW CHANGEFEED JOBS` has a `description` column, no `statement` column.

Related: [[03-locked-constraints]] · [[06-build-and-test]]
