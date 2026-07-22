# OmniFlow — Sentinel State Ledger (2026-07-21)

Running audit state so the loop survives context resets. Antigravity scaffolds; Claude (Sentinel) audits from disk each round, patches contained bugs, returns a verdict + redirection prompt; the user pilots Antigravity manually.

## Architecture baseline (immutable)
Kafka (KRaft) ← CockroachDB v24.3+ **JSON** changefeed → orchestrator. Transactional outbox + changefeed (no dual-write). Protobuf contracts + `buf.validate`. Go strict hexagonal. OTel W3C traceparent. WebGL 3D timeline on HLC `sequence_engine_key`. Power BI on CDC emit-time. Pure-Go **franz-go** is the standard Kafka client for new services (no CGO).

## Phase status
- **Phase 1** (infra/schema/proto): DONE, hardened.
- **Phase 2 CommBot: A-.** Hexagonal; consumer (confirmed-DLQ, retry, traceparent, protovalidate); LLM gateway (SSRF allowlist, x/time/rate token bucket, real parse); CRDB tx outbox + idempotency; `enable.auto.commit=false`. `ports` merged into `domain` (import-cycle fix).
- **Phase 3 Orchestrator: A-.** Kahn topo-sort + cycle→ErrTerminal; 1-tx CTE checkpoint (exactly-once outbox); `FOR UPDATE NOWAIT` lease (55P03→transient); re-fetch-under-lock quiet abort; HITL suspend (no ledger write) + resume via approval topic; decodes CRDB JSON changefeed envelope, skips `resolved`. Consumes `omniflow.orchestration.v1` + `omniflow.p2p.approval.v1`; emits `omniflow.p2p.completed.v1`.
- **Phase 4 Inventory Intelligence: A (COMPLETE).** Single-CRDB-tx ledger (real pgx.Tx: idempotency + lots + facts atomic); true FIFO drawdown + moving-avg; HLC late-arrival restatement = ResetLots + full-log replay via `applyMovement` (no recursion), facts `ON CONFLICT (event_id) DO UPDATE`; signed adjustments (depleteLots on negative); franz-go consumer (envelope decode, `errors.Is` transient via pgconn 40001/08*, confirmed-DLQ `ProduceSync`); SCD2 star schema + HLC-guarded MERGE; hardened CRDB→BQ changefeed (29092, topic_prefix, mvcc_timestamp, gc-protect). Files: `services/inventory-intelligence/**`, `infrastructure/{crdb_schema.sql,warehouse/{star_schema.sql,dax_measures.md}}`, `contracts/.../inventory/v1`.
- **Phase 5 Real-Time Viz: KICKED OFF (charter written, research+council done).** SSE gateway (franz-go) → Next.js/R3F flow-graph. Strategy: readable-first, 3D only for topology, numbers on flat HUD, time-box ~1wk, record early on seeded data, SSE-over-WS (documented), jitter-free via HLC watermark + client buffer + LERP/SLERP, InstancedMesh + refs-not-state. Leverage: scrub-back replay over the event log + business-semantic overlays. See `PHASE5_CHARTER.md`.

## LOCKED — do not touch
`services/commbot/**` logic, `dag.go`, `orchestrator_schema.sql`, `service.go` suspend logic, Phase-4 valuation/ledger.

## OPEN / residuals
1. **franz-go migration incomplete:** CommBot + orchestrator still import CGO `confluent-kafka-go` → editor red + need librdkafka/CGO to build. Migrate both to franz-go for a build-anywhere repo (or accept CGO in CI).
2. Build never CGO-verified in the agent env; CI must build with `CGO_ENABLED=1` OR after the franz-go migration, `CGO_ENABLED=0` should pass.
3. protovalidate: fixed (vendored tree removed; `third_party/proto` include; BSR gen module in go.mod). buf.yaml modules = contracts/proto + third_party/proto.

## Cross-cutting backlog
human-approval event producer (admin → approval topic); lease-TTL reclaim (`owner_pod`/`lease_expires_at` written, never enforced); OTLP collector + Grafana/Tempo; K8s/Terraform/ArgoCD/Istio; `go test`/CI; Phase-4 restatement perf (replay-from-checkpoint not from 0). Recurring hygiene: delete Antigravity's `Architecture base N/` staging folders.

## Grade sheets / charters
`docs/audit/PHASE2_GRADE.md`, `PHASE3_GRADE.md`, `PHASE3_CHARTER.md`, `PHASE4_CHARTER.md`, `PHASE5_CHARTER.md`.
