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
1. **[[07-the-gate]]** — human pushes repo + sets `CRDB_LICENSE`/`CRDB_ORG`. Then watch the 4 E2E
   jobs go green. This is the real milestone — everything above is unproven until then.
2. After green: README + SCOPE.md (portfolio framing).
3. Then: the deferred **WebSocket upgrade** (`docs/antigravity/WEBSOCKET_UPGRADE_SPEC.md`), its own
   audited prompt — do NOT fold into anything else.

## Backlog (not blocking)
Lease-TTL reclaim enforcement (`owner_pod`/`lease_expires_at` written but not reaped); human-approval
admin producer; OTLP→Grafana/Tempo trace slice; Phase-4 restatement replay-from-checkpoint (currently
replays from 0).

Related: [[01-working-loop]] · [[06-build-and-test]]
