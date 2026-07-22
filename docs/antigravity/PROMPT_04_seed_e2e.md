# Prompt 4 — Seed one vendor email E2E → a moving 3D node (paste into Antigravity)

Prereqs restored via BLOCK A (`REONBOARD_AND_PROMPT03.md`). The stack now stands up
(`docker-compose.yml` + `infrastructure/init/crdb-init.sh`, Sentinel-patched). This prompt proves the
core runs **end to end for real** — no simulated logs. It executes in GitHub Actions (see
`.github/workflows/e2e.yml`), not locally.

Golden path to prove:
`seed raw vendor email → omniflow.communication.v1 → CommBot (classify via mock-llm) → commbot_outbox
→ changefeed → omniflow.orchestration.v1 → P2P orchestrator (DAG) → orchestrator_outbox → changefeed
→ omniflow.p2p.completed.v1 → viz-gateway SSE (/api/stream) → one ProjectionEvent for the seeded
aggregate, carrying a real sequence_engine_key.`

## Part A — Deterministic LLM (do NOT touch LOCKED CommBot code)
CommBot's core domain logic is LOCKED. Do not change `service.go`/`ProcessVendorEmail`. Instead make
its **LLM dependency** deterministic for E2E:
1. Add a tiny `mock-llm` service to `docker-compose.yml` — a minimal HTTP server that returns a
   canned response in the exact shape CommBot's LLM gateway parses. READ
   `services/commbot/internal/adapters/outbound/llm/gateway.go` first and match its expected response
   contract precisely (endpoint path, JSON body). It must return a single deterministic intent
   classification + extracted JSON for the seeded email.
2. Point CommBot at it: set `LITELLM_URL=http://mock-llm:<port>` in the commbot service env. Keep the
   SSRF allowlist happy (add the mock host to `OBJECT_STORE_HOSTS` only if the gateway requires it).
3. `mock-llm` can be a ~30-line Go program (`tools/mock-llm/main.go`, root module) with its own
   Dockerfile, or a static-response container. Prefer the Go program for one-language consistency.

## Part B — The seeder (`tools/seed/main.go`, root module)
A small franz-go producer that publishes EXACTLY ONE synthetic vendor email and exits 0.
- Read CommBot's inbound decode (`services/commbot/internal/adapters/inbound/kafka/consumer.go` +
  the `communication/v1` proto) and produce a message that PASSES CommBot's protovalidate on
  `omniflow.communication.v1` (default `COMMBOT_INPUT_TOPIC`).
- Populate: fresh `event_id` (UUID), a valid W3C `traceparent`, a monotonic `sequence_engine_key`
  (e.g. current unix-nanos), a realistic vendor email body, `mdm_vendor_id`.
- Env: `KAFKA_BROKERS` (default `localhost:9092` so it runs from the CI host against the published
  Kafka port), `COMMBOT_INPUT_TOPIC` (default `omniflow.communication.v1`).
- Print the produced `event_id` + `sequence_engine_key` to stdout so CI can assert on them.

## Part C — Make the P2P projection real (additive, AUDITED exception to the schema lock)
The viz P2P handler is currently a placeholder: `orchestrator_outbox` has no `sequence_engine_key`
column (it lives inside the protobuf `payload`), and `handleP2PCompleted` reads a non-existent
`po_id`. Fix it the JSON-native way (mirror the inventory path; do NOT couple viz-gateway to protobuf
decoding):
1. **Additive column** on `orchestrator_outbox`: add `sequence_engine_key INT8 NOT NULL` (schema is
   normally LOCKED — this additive projection column is a Sentinel-authorized exception; do NOT alter
   the exactly-once CTE semantics or any other column). Populate it in the `state_store.go` outbox
   `INSERT ... SELECT` from the workflow's `sequence_engine_key` (already on the `workflows` row).
   Keep the CTE's exactly-once/`insert_ledger` guard byte-for-byte otherwise.
2. Update the orchestrator changefeed heredoc in `crdb-init.sh` only if needed (no WITH-option change;
   the new column flows automatically in the JSON `after`).
3. Rewrite `viz-gateway/internal/kafka/consumer.go handleP2PCompleted` to: unwrap the CRDB `after{}`
   envelope (mirror `handleInventoryMovement`), skip `resolved`/tombstone, read
   `after.aggregate_id`, `after.event_type` (→ Stage/Status), and `after.sequence_engine_key`
   (as STRING via the existing `extractString`/`UseNumber` path — never float64). No `po_id`.

## Part D — One-command E2E runner (`Makefile` or `scripts/e2e.sh`)
A script CI and humans can run: `docker compose up -d --build` → `docker compose wait crdb-init`
(must exit 0) → assert 3 changefeeds `running` → run the seeder → `curl -N --max-time 20
http://localhost:8081/api/stream` and assert at least one `ProjectionEvent` line contains the seeded
`sequence_engine_key`. On any failure, dump `docker compose logs`. Always `docker compose down -v`.

## Deliverables & verification
Output: the `mock-llm` + `seeder` code and Dockerfiles, the `orchestrator_outbox` + `state_store.go`
+ `consumer.go` diffs, the E2E runner, and any compose additions. Run `go build ./...` (root) +
`cd services/viz-gateway && go build ./...` and show exit 0. **Do NOT fabricate a boot log** — the
real run happens in CI. State clearly what you could and could not verify locally.
