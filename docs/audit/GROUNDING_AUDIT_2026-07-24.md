# Grounding + Independent Audit — 2026-07-24 (Sentinel, from disk)

> Done by Claude directly against disk (no Antigravity round). This is the pre-push gate review the
> `docs/antigravity/GROUNDING_AND_INDEPENDENT_AUDIT.md` prompt was going to outsource — superseded by
> this. HEAD at audit time: `8b6b7ff` on `master`, working tree otherwise clean.

## Verdict
**Safe to push to CI now.** The golden path is wired end-to-end with no wiring gaps, vacuous asserts,
or env/topic drift. One first-run unknown remains (Kafka native-image healthcheck) — bounded by a new
job `timeout-minutes` so it fails fast+clean instead of hanging to the 6h cap.

## Build result
- root module `CGO_ENABLED=0 go build ./...` → **exit 0**; `go vet ./...` → **exit 0**
- `services/viz-gateway` build → **exit 0**; vet → **exit 0**
- Local Go = 1.26.5. Both `go.mod` declare `go 1.25.0`, no `toolchain` directive → CI `setup-go 1.25`
  satisfies it and 1.26.5 builds it. **Not a blocker.**

## Findings (most-severe first)
| # | file:loc | category | what | severity | fix | conf |
|---|---|---|---|---|---|---|
| 1 | docker-compose.yml:15 | infra/CI-hang | **RESOLVED (2026-07-24).** Kafka healthcheck shells `kafka-broker-api-versions.sh` (a Java-launching script); `apache/kafka-native` is a GraalVM binary with NO JRE, so it would fail → kafka never healthy → crdb-init never runs → boot jobs hang. **Fix applied:** switched image to JVM `apache/kafka:3.8.0` (ships the same CLI scripts *with* a JRE — the canonically documented healthcheck combo). | was major | done — image swapped | high |
| 2 | .github/workflows/e2e.yml (4 boot jobs) | CI-hygiene | no per-job timeout → a stall burns to the 360-min default | minor | **FIXED this pass**: `timeout-minutes: 25` on e2e + 3 failtests | high |
| — | scripts/failtest_fifo_restatement.sh:24 | redundancy | belt-and-suspenders `grep "crdb-init complete."` loop *and* `docker compose wait crdb-init` | trivial | leave; harmless | high |

No other discrepancies. Everything else below was checked and confirmed correct.

## What was checked and PASSED (with the specific evidence)
- **Topic spine, byte-for-byte.** changefeed sinks (crdb-init.sh) ↔ `kgo.ConsumeTopics` in every
  service ↔ seeder producers: `omniflow.orchestration.v1` (commbot_outbox→orchestrator),
  `omniflow.p2p.completed.v1` (orchestrator_outbox→viz), `omniflow.p2p.approval.v1` (seed→orchestrator),
  `omniflow.inventory.fact_inventory_movement`/`_snapshot` (topic_prefix `omniflow.inventory.`→viz),
  `omniflow.inventory.movement.v1` (seed→inventory raw ingress), `omniflow.communication.v1`
  (seed email-mode→commbot). All match.
- **Env contract.** commbot reads `KAFKA_BOOTSTRAP` (compose sets it); inventory/orchestrator read
  `KAFKA_BROKERS`+`CRDB_DSN`; viz reads `DATABASE_URL`+`KAFKA_BROKERS`. DB name = `omniflow` in every
  compose env string and in every host-side seed/e2e command. No `defaultdb` reached at runtime (the
  code defaults are overridden by compose).
- **viz envelope unwrap** (viz-gateway/internal/kafka/consumer.go): both handlers unwrap top-level
  `after{}`, read `sequence_engine_key` via `extractString` over a `json.Number` (UseNumber → no
  float64 loss), and short-circuit on the `resolved` watermark. Correct.
- **4 proof scripts assert real invariants:** e2e opens SSE at `:8081` *before* seeding; killed_pod
  proves durable resume (kill→up→approve→ledger final_step count=1, state COMPLETED); exactly_once
  proves dup-approval no-op (outbox=2, ledger=1, workflows=1, `aggregate_id` resolved via
  `SELECT id FROM workflows WHERE event_id=…`); fifo compares DECIMAL numerically **in SQL**
  (`fifo_unit_cost = 2.00`) and asserts snapshot `fifo_total_value = 60.00`. `SSE_PID=""` init + guarded
  kill in every EXIT trap. Service name `cockroachdb`, port `8081`, `docker compose wait crdb-init`.
- **Compose/schema completeness:** all 6 Dockerfiles compose references exist; all 3 schema files
  crdb-init applies exist; every table the scripts query (workflows, node_execution_ledger,
  orchestrator_outbox, commbot_outbox, fact_inventory_movement, fact_inventory_snapshot) is defined in
  an *applied* file, and the exact columns read exist (movement_type INT8, fifo_unit_cost/fifo_total_value
  DECIMAL, sequence_engine_key INT8 incl. the additive orchestrator_outbox column).

## Perspective (my independent read)
1. **Single most-likely CI failure if pushed now:** finding #1 (Kafka native-image healthcheck). Not a
   logic bug — an image-internals unknown I can't settle without a Docker daemon. Now bounded to a
   ~25-min clean red by #2 instead of a silent 6h hang.
2. **Vacuous/assert-nothing proofs:** none found. Each of the four asserts a distinct invariant that can
   only pass if the real spine ran.
3. **"Make It Real" holds:** no "compiles but never runs" surface reachable by the E2E — the default
   seed path (`SEED_MODE=outbox`) deliberately routes around CommBot's un-runnable-in-CI SSRF/LLM hop,
   and everything downstream is exercised by real Kafka messages + real changefeeds.
4. **Dissent on locked decisions:** none material. The additive `orchestrator_outbox.sequence_engine_key`
   column remains the only sanctioned schema-lock deviation and is populated + read consistently.
5. **KB vs disk drift:** none. `05-gotchas.md` matches the code (service name, ports, envelope shape,
   DECIMAL-in-SQL, aggregate_id semantics all as the code actually is).

## Open questions for the human
- The Kafka healthcheck (#1) can only be settled by the first CI boot (or a Docker host). Push and watch
  the `boot stack + seed E2E` job: if it sits at "Waiting for crdb-init" with no crdb-init logs, that's
  #1 — apply the TCP-probe fallback and re-run.
