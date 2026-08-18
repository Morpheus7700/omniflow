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

Nothing is blocking. In rough order of value:

- **Two repo settings only a human can change.** Dependabot *alerts* are disabled repo-wide, so
  nothing is raised *because* an advisory was published — `govulncheck` has been carrying that
  alone. And `test agent exactly-once effect` passes but is not among the 9 required contexts, so
  it is advisory.
- **Bound the remaining DB/Kafka calls** with `context.WithTimeout`, and give the service binaries a
  self-probe mode so compose healthchecks become real.
- **10 gosec G104 warnings** (unhandled errors) in code we own — the only code-scanning findings
  left after generated files were excluded. Low severity, but they are now the whole list.

## Backlog (not blocking)
Lease-TTL reclaim enforcement (`owner_pod`/`lease_expires_at` written but not reaped); human-approval
admin producer; OTLP→Grafana/Tempo trace slice; Phase-4 restatement replay-from-checkpoint (currently
replays from 0).

Related: [[01-working-loop]] · [[06-build-and-test]]
