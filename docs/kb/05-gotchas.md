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
- **Use the JVM image `apache/kafka`, NOT `apache/kafka-native`.** The native (GraalVM) image
  ships no JRE, so the `kafka-broker-api-versions.sh` healthcheck (a Java-launching shell script) can't
  run → kafka never reports healthy → `crdb-init` (gated on `kafka: service_healthy`) never starts →
  every boot job hangs to the CI cap. The JVM image ships the same CLI scripts *with* a JRE. (Fixed
  2026-07-24; see `docs/audit/GROUNDING_AUDIT_2026-07-24.md` finding #1.)
- **The pinned tag is `apache/kafka:4.3.1`** (`docker-compose.yml`), not the 3.8.0 this note claimed
  until 2026-08-18. The JVM-vs-native rule above is what matters and is unchanged; only the version
  had drifted. Dependabot's `docker-compose` ecosystem owns this pin — don't hardcode a version in
  prose here again, it will rot.

## CockroachDB specifics
- **Changefeeds run license-free on a single-node cluster** (`start-single-node`, what compose runs) under
  v24.3+ licensing. `CRDB_LICENSE`/`CRDB_ORG` are OPTIONAL env, needed only for a *multi-node* cluster.
  crdb-init + scripts + CI attempt changefeed creation unconditionally and **fail loudly** if a key is
  ever actually required — they never silently skip.
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

## Boot-proof scripts: assert the terminal state, never a proxy for it
Once E2E and FIFO went green (2026-07-27), the two still-failing jobs turned out to be bugs in the
**tests**, not the system — both used a stand-in for "the workflow finished":

- `failtest_killed_pod.sh` treated **the arrival of an SSE event** as completion. That event is emitted
  by the FIRST payload-bearing checkpoint (`service.go:100`); the DAG still has nodes to run before
  `final_step` lands (`service.go:195`). It asserted terminal state mid-DAG → "Expected 1 final_step
  row, got 0".
- `failtest_exactly_once.sh` used **`sleep 3`** as "wait for completion". Wrong on a CI runner, so the
  duplicate approval landed mid-flight and the assertion read a half-built workflow (1 outbox row, not
  2). It also meant the test never exercised its own premise: a duplicate arriving *after* completion.

Rule: poll for the real terminal condition (`SELECT state … == 'COMPLETED'`, or the row you expect)
with a generous timeout, and print the last observed state when the timeout fires. A bare `timeout`
under `set -e` exits 124 with no message and is indistinguishable from a crash.

Two payload-bearing `SaveCheckpoint` calls exist, so a completed workflow has **exactly 2**
`orchestrator_outbox` rows. When asserting suppression, check BOTH that the count equals the
pre-duplicate baseline (suppression happened) AND its absolute value (the DAG emitted what it should)
— either alone can pass for the wrong reason.

Calibration from a real run: after approval the SSE event took **17s** to arrive. Windows of 20s are
one second from being permanent flakes; use 40s+, and remember these scripts also run on cold-pull
runners where boot alone is ~4min.

### A `timeout-minutes` expiry reports as CANCELLED, not FAILURE
This is actively misleading and cost a wrong diagnosis (assumed a manual cancel or an Actions quota).
The tell is arithmetic: both jobs ran **exactly 25m14s** against `timeout-minutes: 25`. If a job's
duration equals its `timeout-minutes`, it hung — look for the last line it printed, not for an
external cause. `gh api …/jobs --jq '… started_at, completed_at, steps'` gives the timing and shows
which step was cut off.

### Don't smuggle a quoted SQL literal into `timeout bash -c '…'`
That hang was self-inflicted: `timeout 90s bash -c 'until [[ "$(… -e "SELECT state … event_id='…';")" ]]'`
needs `'"'"'` escaping to get the SQL quotes through a single-quoted `bash -c`, and it silently never
expired — the job ran to its 25-minute cap instead of failing at 90s. Use a plain deadline loop in the
main shell, where the SQL's single quotes sit harmlessly inside double quotes:

```bash
DEADLINE=$(( SECONDS + 90 )); STATE=""
while (( SECONDS < DEADLINE )); do
    STATE=$(docker compose exec -T cockroachdb cockroach sql --insecure -d omniflow --format=csv \
        -e "SELECT state FROM workflows WHERE event_id='${EVENT_ID}';" | tail -n 1 | tr -d '[:space:]')
    [[ "$STATE" == "COMPLETED" ]] && break
    sleep 2
done
[[ "$STATE" == "COMPLETED" ]] || { echo "FAIL: last state '${STATE}'"; exit 1; }
```

Always `tr -d '[:space:]'` the CSV cell — docker/cockroach output carries stray whitespace and CR.
The `timeout bash -c` form remains fine where the query needs **no** quotes (as in the FIFO test).

## A test that stops at an earlier guard proves nothing about the code behind it

`failtest_dlq_poison.sh` was cited for months as the proof that "a protovalidate failure is terminal
and routes to the DLQ". **It never tested that.** Its payload `not-a-valid-protobuf` has a leading
byte decoding to an illegal protobuf wire type, so `proto.Unmarshal` rejects it and the consumer
returns at the *unmarshal* guard — `validator.Validate` is never called. The script's own comment
conceded it ("even if it somehow parsed, protovalidate would reject it").

So the repo's most load-bearing consumer invariant was verified by nothing, and a dependency bump
that reclassified validation errors as transient would have wedged the partition with all jobs green.

The fix is the general lesson: **to test a code path, your input must survive every guard in front of
it.** `SEED_INV_INVALID=1` emits *wire-valid* protobuf with a non-UUID `event_id`, so it parses
cleanly and fails exactly one rule (`string.uuid`). Assert both that it reaches the DLQ **and** that
no fact row was written — the second half proves the validator sits in front of the ledger.

## Dependabot: ecosystems are narrower than they sound, and failures are invisible

- **`docker` and `docker-compose` are SEPARATE ecosystems.** The `docker` ecosystem reads Dockerfiles
  and Kubernetes YAML only — **not** `docker-compose.yml`. `cockroachdb/cockroach` and `apache/kafka`
  live only in compose, so the database and the broker had *zero* surveillance while the config
  comment claimed to cover them.
- **`directory: /` requires a Dockerfile at the root.** There isn't one here (all 7 are one or two
  levels down), so the ecosystem failed outright with `No Dockerfiles nor Kubernetes YAML found in /`.
  Use `directories:` (plural) with an explicit list.
- **`gcr.io/distroless/static:nonroot` cannot be version-bumped** — it is a floating, non-semver tag.
  Tracking it needs digest pinning. Don't claim coverage you don't have.
- ⚠️ **Dependabot config errors surface at `/network/updates`, NEVER as a CI check.** That is why a
  broken ecosystem can sit unnoticed indefinitely. Check that page after editing `dependabot.yml`.
- A module that changes its import path (`protovalidate-go` → `buf.build/go/protovalidate`) makes the
  manifest **unresolvable**, which blocks Dependabot from updating *anything* in that module —
  a single stale dep silently freezes the whole security pipeline.
- ⚠️ **`open-pull-requests-limit` does not queue — it ERRORS, and the error is invisible.** With the
  root-Go limit of 5 reached (#38, #42, #43, #44, #46 all open), the 2026-08-18 job on `go.mod`
  ended `Errored with the message "Dependabot cannot open any more pull requests"` and produced
  nothing. Updates that were due — possibly security-relevant ones — were never raised and there is
  no way to see what they were. A full queue is therefore not a backlog, it is a **blackout**:
  leaving stale PRs open actively suppresses new ones. Check the ⚠️ icon next to a manifest at
  `/network/updates`; nothing about this reaches a CI check or an email.
- **Dependabot *alerts* are a separate switch from Dependabot *version updates*.** This repo has
  version updates running weekly while `/security/dependabot` reads "Dependabot alerts are
  disabled" — so routine bumps arrive, but nothing is raised *because* an advisory was published.
  That is the opposite of the priority you want, and it is why `govulncheck` in CI has been doing
  the entire job of catching reachable CVEs. Enabling alerts is a repo Settings change.

## Report-only scanners become unread scanners

`gosec -no-fail` and `trivy --exit-code 0` are both deliberate (too noisy to gate on), but the result
was that nobody read them. A `govulncheck` run found **5 CVEs the code genuinely reaches**, sitting in
the Trivy SARIF the whole time — including two in *indirect* deps (`grpc`, `x/text`) that no
Dependabot PR would ever have raised.

`govulncheck` is call-graph aware — it reports a vulnerability only if the affected symbol is actually
reachable — so its output is small and true enough to **gate** on. It now does. Keep Trivy for breadth,
report-only; keep govulncheck as the gate. **Scan BOTH modules**: `services/viz-gateway` has its own
`go.mod` and is invisible to a root-module scan (it was carrying a vulnerable `x/text` while root
reported clean). Pin the scanner to a tag, not `@latest`, or a scanner release can redden the build
with no code change.

## A floating `go-version` defeats the govulncheck gate — pin the exact patch

The gate above was pinned at the scanner (`govulncheck@v1.6.0`) but left floating at the *toolchain*
(`go-version: '1.25'`), and that hole silently disarmed it for days.

**`setup-go` resolves a floating spec against the runner image's tool cache first.** Any cached
1.25.x satisfies `'1.25'`, so it keeps returning that patch and never fetches the newer one. The
evidence, from PR #49 (a PR that touched only `frontend/package-lock.json`):

- `actions/go-versions` published 1.25.13 at **2026-08-14T03:55Z**.
- The run at **2026-08-14T12:35Z** — 8½ hours later — still logged `go version go1.25.12`.
- govulncheck exited 3 with **seven reachable stdlib CVEs**, every one `Fixed in: go1.25.13`.

So **re-running does not fix this**, and that is the trap: the failure looks transient and unrelated
to the diff, which invites exactly the wrong response (re-run it, or blame the PR). Only an explicit
version changes what gets installed.

Corollaries, all of them earned:

- **A green check is only as fresh as its run.** Nine other PRs showed green from 2026-08-07 —
  before these CVEs were published. Stale green is not clearance. After any security-baseline change,
  re-run every open PR; never merge on a check older than the baseline.
- **CI must build the toolchain you ship.** CI sat on the 1.25 line while all six Go builder stages
  use `FROM golang:1.26` — so the gate was scanning a stdlib no artifact ever contained. The pin is
  now `1.26.6` in all nine `e2e.yml` jobs *and* the one in `codeql.yml` (ten sites; it is easy to
  miss codeql.yml, which has its own `setup-go`).
- **`go.mod` does not pin the toolchain.** `go 1.25.0` is a minimum-language declaration; the
  installed toolchain is whatever `setup-go` put on PATH.
- **Nothing bumps this for you.** Dependabot's `github-actions` ecosystem updates action refs only —
  it will never touch a `go-version:` value. An exact pin is a standing manual obligation, and that
  is the correct trade: a security gate that silently re-floats is not a gate.

## Frontend build failures take down the ENTIRE E2E matrix

All four compose boot jobs build the frontend image, so a `next build` failure kills jobs that have
nothing to do with the frontend (FIFO included), in ~1m45s. TypeScript **7** does this under Next
16.2.x (`The "id" argument must be of type string. Received undefined`); TypeScript **6** is fine —
so the ignore rule is `versions: [">=7"]`, not `>=6`. Corollary: the boot jobs going green **is** the
frontend build proof, which matters because `npm run build` is unusable locally under the AV.

## Dev-machine traps (not code bugs — they waste hours)
- **The stack DOES run locally now** (Docker Desktop, since 2026-08-18) — `bash scripts/e2e.sh`
  passes on this box. Three things had to be true first, and two of them cost a run each:
- ⚠️ **Git Bash rewrites in-container absolute paths — `export MSYS_NO_PATHCONV=1`.** MSYS2 converts
  any argument that looks like a POSIX absolute path into a Windows path *before the process sees
  it*, so `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh` arrives inside the container
  as `C:/Program Files/Git/opt/kafka/bin/kafka-topics.sh` and the step exits **127**. Proof:
  `docker run --rm alpine echo /opt/x` prints `C:/Program Files/Git/opt/x`; with the variable set it
  prints `/opt/x`. All five `scripts/*.sh` now export it themselves; the variable is ignored on
  Linux/macOS, which is why CI never saw this. A 127 from a boot script on Windows is this, until
  proven otherwise.
- **Nothing else may hold port 3000** (or 8081, 9092, 26257). A `next dev` left running takes 3000,
  and compose then fails the frontend container with `ports are not available: ... bind: Only one
  usage of each socket address`. The stack boots *fine* right up to that container, so the failure
  reads as a frontend bug rather than a port clash. Kill the dev server before booting the stack.
- ⚠️ **`bash script.sh | tail` DESTROYS the exit code.** A pipeline's status is its LAST command, so
  `tail` (always 0) reports success for a script that failed — the same trap as the `git push | tail`
  note below, and it produced a false "E2E PASSED" here. Redirect instead and capture the status:
  `bash scripts/e2e.sh > run.log 2>&1; RC=$?`. Piping also truncates away the assertion lines you
  need. The tell that a run really failed: `e2e.sh` dumps `docker compose logs` ONLY on non-zero, so
  a wall of container logs means failure no matter what the exit code appears to say.
- **`jq` is NOT installed in the Git-Bash environment.** Any script or Monitor piping through `jq`
  silently produces nothing every iteration and looks like "no events yet". Use `gh … --jq` (gh ships
  jq internally) or `--template`.
- **`if git push …| tail -2; then break; fi` NEVER retries.** A pipeline's exit status is its *last*
  command, so `tail` (always 0) masks the push failure and the loop breaks on the first attempt.
  Capture separately: `OUT=$(git push 2>&1); RC=$?`. This matters here because the ISP drops
  connections constantly — a retry loop that doesn't retry is worse than none.
- **`next dev` is NOT throttled the way `next build` is.** Turbopack is ready in ~1s even though
  `npm run build` gets killed by the AV at 10 min — so local visual/a11y verification of the frontend
  IS available; only the production build must go to CI.
- **`agent-browser`'s daemon cannot bind a socket on this box** (`os error 10013`, Windows reserved
  port range / AV). Install and a single screenshot work, then it dies; retrying is pointless. Use
  claude-in-chrome instead — `javascript_tool` + `read_page` cover screenshots, the accessibility
  tree, computed label checks and `performance.getEntriesByType('resource')`.
- **Read the accessibility tree sceptically.** Its dump showed one form input with a label and its
  identical sibling without — the real DOM had both (`input.labels` proved it). Verify a suspected
  a11y bug against the DOM before "fixing" it.
- **Quick Heal real-time scanning throttles the Go build cache into uselessness.** After the
  testcontainers dependency tree landed (docker/moby/containerd/grpc), a *trivial* single-package
  `go build` exceeded 120s where it previously took seconds — the AV re-scans thousands of unseen
  files on every compile. Exclude `%LOCALAPPDATA%\go-build` and `%USERPROFILE%\go\pkg\mod`. Until then
  verify locally with `gofmt -e -l` (parse-only) and let CI compile — it does both modules in ~17s.
- Killing a background `go build` leaves zombie `go.exe` entries that `taskkill` reports as
  "no running instance". They are harmless and are NOT holding cache locks — don't chase them.

Related: [[03-locked-constraints]] · [[06-build-and-test]]
