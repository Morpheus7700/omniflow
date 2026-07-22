# 02 · Architecture — the Event Spine

Immutable baseline: **Kafka (KRaft) ← CockroachDB v24.3 native JSON changefeed → services**.
Transactional outbox + changefeed (no dual-write). Protobuf contracts + `buf.validate`. Go strict
hexagonal. OTel W3C traceparent. `sequence_engine_key` is an **HLC key carried as a STRING
end-to-end** (avoid JS float64 precision loss); rendering is gated by the changefeed `resolved`
watermark.

## Services (two Go modules)
Root module `omniflow` (go 1.25): `commbot`, `p2p-orchestrator`, `inventory-intelligence`, plus
`tools/seed` + `tools/mock-llm`. Separate module `services/viz-gateway` (go 1.25). Also `frontend/`
(Next.js).

## Topics & who reads/writes them
| Topic | Producer | Consumer | Wire format |
|---|---|---|---|
| `omniflow.communication.v1` | external ingress / `tools/seed` (email mode) | CommBot | **raw** protobuf `VendorEmailReceived` |
| `omniflow.orchestration.v1` | **changefeed** on `commbot_outbox` | p2p-orchestrator | changefeed envelope `{"after":{"payload":"\\x…"}}` |
| `omniflow.p2p.approval.v1` | `tools/seed` (HITL approval) | p2p-orchestrator | **raw** protobuf `HumanApprovalEvent` |
| `omniflow.p2p.completed.v1` | **changefeed** on `orchestrator_outbox` | viz-gateway | changefeed envelope `{"after":{…columns…}}` |
| `omniflow.inventory.movement.v1` | external ingress / `tools/seed` (inventory mode) | inventory-intelligence | **raw** protobuf `InventoryMovementReceived` |
| `omniflow.inventory.fact_inventory_movement` / `…_snapshot` | **changefeed** on fact tables | viz-gateway | changefeed envelope |

**Rule of thumb:** raw-protobuf topics = *external ingress* (decode `record.Value` directly, like
CommBot). Changefeed topics = *internal handoffs* (decode the `after` envelope). Getting these
crossed is the #1 recurring bug — see [[05-gotchas]].

## The golden E2E path (what `scripts/e2e.sh` proves)
`seed → commbot_outbox → changefeed → orchestration.v1 → orchestrator DAG → SUSPEND at human_approval
→ seed approval → orchestration resumes → final_step → orchestrator_outbox (exactly-once CTE) →
changefeed → p2p.completed.v1 → viz-gateway → SSE /api/stream (carries sequence_engine_key).`

## Changefeeds (created by `infrastructure/init/crdb-init.sh`, need Enterprise license)
3 jobs: `commbot_outbox`→orchestration.v1, `orchestrator_outbox`→p2p.completed.v1,
`fact_inventory_movement,fact_inventory_snapshot`→inventory.* . All `format=json, resolved='1s'`.

## Key host ports (docker-compose)
CockroachDB SQL `26257`, **CRDB admin UI `8080`**, Kafka `9092` (advertised `localhost:9092` to host,
`kafka:29092` internal), **viz-gateway SSE `8081`** (`8081:8080`), frontend `3000`. Compose service
name for the DB is **`cockroachdb`** (NOT `crdb`). See [[05-gotchas]].

Related: [[03-locked-constraints]] · [[06-build-and-test]]
