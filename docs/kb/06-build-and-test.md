# 06 · Build & Test

The stack, changefeeds, and all failure proofs run in **GitHub Actions**. If the machine you are on
has no Docker daemon, local verification is build + vet + `go test ./...` + `bash -n`, and CI is the
first place the compose stack actually boots — so never report a boot result you did not observe.

## Build / vet (must exit 0)
```bash
# Root module (omniflow):
CGO_ENABLED=0 go build ./...
go vet ./...
# viz-gateway (separate module, go 1.25):
cd services/viz-gateway && CGO_ENABLED=0 go build ./... && go vet ./...
# Scripts:
bash -n scripts/*.sh
# Confirm a LOCKED file is untouched:
git diff --stat services/inventory-intelligence/internal/core/domain/valuation.go   # must be empty
```

## The seeder (`tools/seed`) — the harness Swiss-army knife
Env-driven. `SEED_MODE` picks the entry seam, `SEED_ACTION` picks the lifecycle.
- `SEED_MODE=outbox` (default) — insert a classified event into `commbot_outbox`; the real changefeed
  drives the golden path. `SEED_MODE=email` — produce raw to `communication.v1` (needs public https
  quarantine URLs + mock-llm). `SEED_MODE=inventory` — produce a raw `InventoryMovementReceived` to
  `omniflow.inventory.movement.v1`.
- `SEED_ACTION=full` (default) — seed, wait SUSPENDED, approve. `suspend-only` — seed + wait, no
  approve. `approve-only` — produce approval for `SEED_EVENT_ID`/`SEED_SEQUENCE_ENGINE_KEY`.
- Inventory params: `SEED_INV_SEQ`, `SEED_INV_MOVEMENT_TYPE` (receipt|consumption|adjustment),
  `SEED_INV_SKU`, `SEED_INV_QTY`, `SEED_INV_UNIT_COST`, `SEED_INV_LOCATION`, `SEED_INV_VENDOR`.
- Prints `SEED_EVENT_ID=` + `SEED_SEQUENCE_ENGINE_KEY=` for CI assertions.
- Connects to host-published ports: Kafka `localhost:9092`, CRDB `localhost:26257`.

## The test scripts (CI only; run license-free — no `CRDB_LICENSE` skip as of 2026-07-24)
- `scripts/e2e.sh` — golden path → SSE.
- `scripts/failtest_killed_pod.sh` — durable resume.
- `scripts/failtest_exactly_once.sh` — duplicate suppression.
- `scripts/failtest_fifo_restatement.sh` — HLC restatement.
- `scripts/failtest_dlq_poison.sh` — poison-pill routing, both wire-level (illegal protobuf) and
  semantic (wire-valid, fails `buf.validate`). Those take different code paths: a wire-level pill
  fails at `proto.Unmarshal` and never reaches the validator, so it proves nothing about
  validation-failure handling. A test that only exercises an earlier guard clause proves nothing
  about the code behind it.

`CRDB_LICENSE` is now OPTIONAL: single-node CRDB v24.3+ runs changefeeds license-free, so the scripts
no longer `exit 78` when it's absent — they boot and fail LOUDLY if a key is genuinely needed (crdb-init
non-zero). A key, if set, still flows through. All follow the same shape:
`set -euo pipefail` · `docker compose up -d --build` ·
`docker compose wait crdb-init` · assert · **open SSE before triggering** (where relevant) · numeric
SQL assertions via `docker compose exec -T cockroachdb` · teardown trap `docker compose down -v`.
See [[05-gotchas]] for the traps these encode.

## `mock-llm`
`tools/mock-llm` returns `{"choices":[{"message":{"content":"1"}}]}` on `POST /chat/completions` —
deterministic classifier for `email` mode only. Wired into compose as `mock-llm`.

Related: [[04-progress-ledger]] · [[05-gotchas]]
