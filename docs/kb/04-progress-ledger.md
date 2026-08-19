# 04 · Progress Ledger

Condensed status. Full detail in `docs/audit/STATE.md`.

**Updated 2026-08-19.** The line this note carried for months — *"Nothing is pushed; no CI has
run"* — was false long before it was corrected. Everything below is on a public, branch-protected
`master` where every one of the 9 required checks runs on every PR.

## Make-It-Real prompts
| Prompt | What | Status |
|---|---|---|
| 1 | franz-go migration (kill CGO/confluent) | ✅ done + build-verified |
| 2 | Dockerfiles (all services + tools) | ✅ done |
| 3 | docker-compose + `crdb-init.sh` | ✅ built, audited, Sentinel-patched (3 blockers) |
| 4 | Seed E2E: `tools/seed`, `tools/mock-llm`, projection fix, `scripts/e2e.sh` | ✅ **Claude built it directly** |
| 5 | Failure tests: killed-pod resume + exactly-once | ✅ Antigravity built, Sentinel patched 4 bugs |
| 6 | Inventory ingress repair + FIFO restatement test | ✅ Antigravity built, Sentinel patched 2 bugs |

## What each proof does (all CI-only — no local Docker)
- `scripts/e2e.sh` → the golden path ([[02-architecture]]) reaches the SSE stream.
- `scripts/failtest_killed_pod.sh` → durable checkpoint resume: kill+restart orchestrator mid-suspend,
  approve, assert `final_step` ledger count == 1 and workflow COMPLETED.
- `scripts/failtest_exactly_once.sh` → duplicate approval no-ops (outbox stays exactly 2 rows).
- `scripts/failtest_fifo_restatement.sh` → HLC late-arrival flips a consumption's `fifo_unit_cost`
  5.00→2.00 and snapshot value to 60.00. See [[05-gotchas#HLC seq vs occurred_at]].

CI jobs in `.github/workflows/e2e.yml`: `build`, `e2e`, `failtest-resume`, `failtest-exactly-once`,
`failtest-restatement`, `security` (gosec+trivy). Plus `codeql.yml`. E2E jobs skip cleanly without
`CRDB_LICENSE`.

## Critical path — CLEARED 2026-08-19

All three items this section listed are done. Kept with their outcomes rather than deleted, because
what each one turned out to be is more useful than the fact it is finished.

1. ~~**Test `SaveCheckpoint`.**~~ **Done** — eight integration tests in
   `state_store_integration_test.go` drive the exactly-once CTE directly against real CockroachDB.
   They cover redelivery suppression and the fact that a *genuine retry still emits* (the pair only
   means something together — either alone is satisfied by a trivially wrong implementation), the
   `$5`/`$11` UUID-vs-STRING placeholder split, 19-digit HLC fidelity, and rollback releasing the
   idempotency marker. That last one is the case the shell proof structurally could not catch: a
   failed checkpoint leaving its ledger row behind means the redelivery hits `ON CONFLICT DO
   NOTHING`, the outbox row is never written *by anyone*, and the event is lost permanently while
   every table looks consistent.
2. ~~**Service lifecycle.**~~ **Partly done.** All four services now expose real `/healthz` and
   `/readyz` (`internal/platform/health`, duplicated into viz-gateway because it is a separate
   module). Liveness performs no I/O — deliberately: an orchestrator *kills* a container whose
   liveness probe fails, so a liveness probe that pinged CockroachDB would turn a 30-second database
   blip into a fleet-wide restart. Readiness checks dependencies with a per-check 2s timeout and
   gates on startup completion.
   **Still open:** `context.WithTimeout` on DB and Kafka calls generally (only a handful exist), and
   compose healthchecks for the app services — the Go images are `distroless/static:nonroot` and
   ship no shell, so `CMD-SHELL` cannot run and `depends_on: service_healthy` stays decorative until
   the binaries gain a self-probe mode.
3. ~~**Node execution is a stub.**~~ **Done** — the `draft_po` node calls a real model through the
   drafting agent, outside the workflow transaction (a multi-second LLM call cannot hold a row
   lock). That makes node execution at-least-once by construction, so the guarantee proven is
   exactly-once *effect*, enforced by a deterministic idempotency key and asserted by
   `scripts/failtest_agent_exactly_once.sh` across a mid-workflow kill.
4. Still deferred: the **WebSocket upgrade**, `docs/adr/0001-sse-over-websocket.md`.

## What is actually next

Code scanning is at **zero open alerts** and gosec reports 0 issues in both modules.

- **One repo setting only a human can change.** `test agent exactly-once effect` passes on every PR
  but is not among the 9 required contexts, so it cannot block a merge. Dependabot alerts are now
  **enabled** (it has already opened and landed its first bump), which closes the other half of what
  this bullet used to say.
- ~~**Bound the remaining DB/Kafka calls**, and give the binaries a self-probe mode~~ **Done, with
  one part deliberately left.**
  - Compose healthchecks are real: the images are distroless, so `CMD-SHELL` cannot run and the app
    services had no healthcheck at all — every `depends_on: service_healthy` silently meant
    `service_started`. Each binary now answers `-probe` against its own `/readyz`, and
    `scripts/e2e.sh` blocks on all four being healthy instead of leaning on the seeder's 60s poll to
    absorb the startup race.
  - Every DB statement is bounded by a session `statement_timeout` (`internal/platform/crdbpool`),
    not by wrapping 28 call sites. The call-site approach touches the checkpoint and DLQ paths, and
    the obvious version of it is wrong: the DLQ produce and the offset commit run *because*
    processing failed, often because a deadline expired, so sharing that expired context with them
    would break the confirmed-DLQ-before-commit ordering the delivery guarantee rests on. A session
    timeout bounds statements added later too, and cannot make that mistake. It is safe to enable
    because 57014 (`query_canceled`) already classifies Transient via errclass's class-57 prefix, so
    a timed-out statement re-enters the retry ladder rather than dead-lettering valid work.
  - **Still open:** the Kafka half. `PollFetches` is bounded by the consumer's own context, but the
    produce calls on the DLQ path are not individually bounded. That one wants its own change,
    because the DLQ produce is the single call in this system that must not fail quietly.
- ~~**10 gosec G104 warnings**~~ **Done** — seven were `tx.Rollback(ctx)` on orchestrator error
  paths, fixed by applying the `if rbErr := ...` convention the same file already used twice rather
  than writing `_ =`, which would have satisfied the scanner without answering it. A failed rollback
  means the transaction still holds its `FOR UPDATE` lock.
- ~~**4 gosec G706 warnings**~~ **Done, and they were not noise.** `scripts/e2e.sh` recovers the key
  the whole proof asserts on by grepping the seeder's stdout for `^SEED_SEQUENCE_ENGINE_KEY=`, and
  the seeder printed `$SEED_EVENT_ID` one line above it. A newline in that variable forged the
  marker, and the E2E would assert on a key nothing produced — a green run proving nothing. Fixed at
  the boundary: `SEED_EVENT_ID` must now be a UUID.

## Backlog (not blocking)
Lease-TTL reclaim enforcement (`owner_pod`/`lease_expires_at` written but not reaped); human-approval
admin producer; OTLP→Grafana/Tempo trace slice; Phase-4 restatement replay-from-checkpoint (currently
replays from 0).

Related: [[01-working-loop]] · [[06-build-and-test]]
