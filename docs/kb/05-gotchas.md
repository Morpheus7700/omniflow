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

- **NO named volume on Kafka's log dir (`/tmp/kafka-logs`).** A fresh named volume mounts empty and
  root-owned; Kafka runs non-root and fails to format storage → `Error while writing meta.properties
  file /tmp/kafka-logs`, container exits 1 *after* config validation (so it's distinct from the
  inter-broker crash above). The image's own log dir is already correctly owned — don't clobber it.
  This is an ephemeral CI stack (`down -v` each run; the killed-pod test restarts the orchestrator, not
  Kafka) so persistence isn't needed. Removed the `kafka-data` volume entirely. Fixed 2026-07-24.

- **Single-broker cluster needs RF=1 on internal topics.** `__consumer_offsets` (and the
  transaction-state log) default to replication factor 3 and can't be created on one broker, so
  consumer groups get `COORDINATOR_NOT_AVAILABLE` and NO service ever consumes — the seed then times
  out with "workflow never suspended" even though the changefeed produced fine. Set
  `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`,
  `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1` (+ `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` to skip the 3s
  rebalance wait). Symptom is in the *consumer* log, not Kafka's. Fixed 2026-07-24.

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
- **`topic_prefix` and `topic_name` are sink-URI query params, NOT `WITH` options.** Putting
  `topic_prefix='…'` in the `WITH (…)` clause fails with `ERROR: invalid option "topic_prefix"` and
  crdb-init exits 1 (→ every boot job dies at "crdb-init didn't complete successfully"). Correct form:
  `INTO 'kafka://kafka:29092?topic_prefix=omniflow.inventory.&tls_enabled=false' WITH updated, …`.
  Valid `WITH` options here: format, updated, resolved, mvcc_timestamp, key_column, unordered,
  min_checkpoint_frequency, protect_data_from_gc_on_pause. Fixed 2026-07-24.
- **`unordered` and `resolved` are mutually exclusive**: `ERROR: unordered is not usable with resolved
  because resolved timestamps cannot be guaranteed to be correct in unordered mode`. `key_column`
  pulls in `unordered` (custom keying breaks ordering), so `key_column` + `resolved` can't coexist.
  The viz rendering gate needs the resolved watermark → resolved wins; dropped `key_column`+`unordered`
  from both outbox changefeeds. Safe because (a) viz reads `aggregate_id` from the value `after{}`, not
  the Kafka key, and (b) the only `.Key` uses are DLQ pass-through (`routeToDLQ`,
  `produceToDLQConfirmed`) — no logic keys off it. Single-partition dev Kafka preserves order anyway.
- **License-free single-node changefeeds CONFIRMED working in CI** — crdb-init logged "No CRDB_LICENSE
  — proceeding license-free" and created the DB + schema with no license error. The pivot holds; the
  only changefeed failure was the `topic_prefix` placement above, not licensing.

### CockroachDB SQL dialect ≠ PostgreSQL (two bugs, one statement, 2026-07-27)
Both were in `p2p-orchestrator/.../crdb/state_store.go` `SaveCheckpoint`, both **pre-existing and
invisible** until the topic fix let the DAG resume far enough to execute that branch. Expect this
pattern: fixing infra unmasks the next latent layer.

- **A data-modifying CTE MUST return columns.** `WITH update_wf AS (UPDATE … WHERE id = $5)` — legal
  in PostgreSQL, rejected by CRDB:
  `ERROR: WITH clause "update_wf" does not return any columns (SQLSTATE 0A000)`.
  Add `RETURNING 1`. It does **not** need to be consumed: per CRDB's documented subquery semantics a
  data-modifying statement in a CTE "is always executed to completion, even if the surrounding query
  only uses a subset of the results", so the UPDATE still applies unreferenced.
- **A placeholder gets exactly ONE inferred type.** `$5` (= `wf.ID`, a Go `string`) was used against
  `workflows.id` UUID, `node_execution_ledger.workflow_id` UUID, *and*
  `orchestrator_outbox.aggregate_id` **STRING** (intentionally — it doubles as the Kafka partition
  key). Inference latched onto UUID from the first two, then:
  `ERROR: placeholder $5 already has type uuid, cannot assign string (SQLSTATE 42804)`.
  Fix = pin the placeholder to the Go type and cast at the divergent sites: `$5::UUID` where UUID is
  wanted, bare `$5` for the STRING column. Do not "fix" the schema — it is LOCKED.
- **Both surfaced as `WARN Unknown error, assuming transient for safety` × 5 → DLQ.** `0A000` and
  `42804` are deterministic (feature-not-supported / type error); retrying cannot help. The classifier
  defaults unknown errors to transient, so a deterministic SQL bug costs five retries and then looks
  like a DLQ routing problem. When you see five identical WARNs then "Exhausted retries", read the
  SQLSTATE — the bug is upstream of the retry logic.

## Kafka topic creation — franz-go does NOT auto-create
- **`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` is INERT for every Go service here.** The broker only
  creates a topic when the *client* asks in its Metadata request, and franz-go omits that unless you
  pass `kgo.AllowAutoTopicCreation()`. Nothing in this repo passes it.
- The tell that isolates it: `omniflow.orchestration.v1` existed while `omniflow.p2p.approval.v1` and
  `omniflow.inventory.movement.v1` did not — because the first is created by CockroachDB's *Java*
  changefeed sink, which does request creation, and the others are touched only by franz-go.
- **The dangerous half is the `.dlq` topics.** Every consumer commits its offset ONLY after DLQ
  delivery is confirmed (`produceToDLQConfirmed`, `routeToDLQ`), so a missing `.dlq` topic wedges that
  partition permanently on the first poison message — and every happy-path job stays green while it
  does. Covered now by `scripts/failtest_dlq_poison.sh`.
- Fix = `infrastructure/init/kafka-init.sh` creates all 10 topics explicitly (RF=1 single broker;
  partitions=1 keeps per-key ordering total for the HLC assertions); `crdb-init` gates on it.

## Dev-machine traps (not code bugs — they waste hours)
- **`jq` is NOT installed in the Git-Bash environment.** Any script or Monitor piping through `jq`
  silently produces nothing every iteration and looks like "no events yet". Use `gh … --jq` (gh ships
  jq internally) or `--template`.
- **Quick Heal real-time scanning throttles the Go build cache into uselessness.** After the
  testcontainers dependency tree landed (docker/moby/containerd/grpc), a *trivial* single-package
  `go build` exceeded 120s where it previously took seconds — the AV re-scans thousands of unseen
  files on every compile. Exclude `%LOCALAPPDATA%\go-build` and `%USERPROFILE%\go\pkg\mod`. Until then
  verify locally with `gofmt -e -l` (parse-only) and let CI compile — it does both modules in ~17s.
- Killing a background `go build` leaves zombie `go.exe` entries that `taskkill` reports as
  "no running instance". They are harmless and are NOT holding cache locks — don't chase them.

Related: [[03-locked-constraints]] · [[06-build-and-test]]
