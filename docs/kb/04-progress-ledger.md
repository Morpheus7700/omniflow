# 04 · Progress Ledger

Condensed status. Full detail in `docs/audit/STATE.md`. Everything below is **committed** in
`d351b96` on `master`, on top of baseline `388cc35`. **Nothing is pushed; no CI has run.**

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

## Critical path (NEXT)
The gate this section used to describe — push the repo public, watch the first CI run — is long
since passed. All 9 required checks are green on a branch-protected `master`, and the stack boots
on real infrastructure every run.

1. **Test `SaveCheckpoint`.** `services/p2p-orchestrator/internal/adapters/outbound/crdb/state_store.go`
   has three divergent SQL branches and no Go test of any kind. Every exactly-once claim in the
   README rests on it, and it is currently proven only indirectly by a shell script counting rows
   in a booted stack — a proof that does not run on fork PRs. The pattern to copy is one directory
   over, in inventory-intelligence's `repository_integration_test.go` (testcontainers + real CRDB).
2. **Service lifecycle.** No `/healthz` or `/readyz` anywhere: all three root-module services
   register a single `/` handler returning 200 before the DB pool has been touched, so
   `depends_on: service_healthy` is meaningless for every app service. No `context.WithTimeout` on
   any DB or Kafka call either.
3. **Node execution is a stub.** `service.go` has `[EXTERNAL I/O EXECUTION HAPPENS HERE]` as a bare
   comment — every DAG node is a no-op checkpoint. Either implement one real node action or say so
   plainly in the README.
4. Then: the deferred **WebSocket upgrade**, `docs/adr/0001-sse-over-websocket.md`.

## Backlog (not blocking)
Lease-TTL reclaim enforcement (`owner_pod`/`lease_expires_at` written but not reaped); human-approval
admin producer; OTLP→Grafana/Tempo trace slice; Phase-4 restatement replay-from-checkpoint (currently
replays from 0).

Related: [[01-working-loop]] · [[06-build-and-test]]
