-- infrastructure/storage/orchestrator_schema.sql
-- Applied ONCE by the crdb-init lifecycle step (which sets the enterprise license,
-- cluster.organization, and kv.rangefeed.enabled) behind a SHOW CHANGEFEED JOBS guard.
-- Cluster settings + license intentionally do NOT live in this schema file.

CREATE TABLE IF NOT EXISTS workflows (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id            STRING      NOT NULL,
    trace_parent        STRING      NOT NULL,
    sequence_engine_key INT8        NOT NULL,
    state               STRING      NOT NULL,
    current_node_index  INT4        NOT NULL DEFAULT 0,
    sorted_nodes        STRING[]    NOT NULL,
    -- Two-tier ownership: durable lease for Human-In-The-Loop cross-wait resume.
    owner_pod           STRING,
    lease_expires_at    TIMESTAMPTZ,
    -- Dedup at creation: a replayed inbound event must not spawn a second workflow.
    CONSTRAINT uq_workflows_event_id UNIQUE (event_id)
);

CREATE TABLE IF NOT EXISTS node_execution_ledger (
    workflow_id UUID REFERENCES workflows(id) ON DELETE CASCADE,
    node_id     STRING      NOT NULL,
    attempt     INT4        NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, node_id, attempt)
);

CREATE TABLE IF NOT EXISTS orchestrator_outbox (
    aggregate_id STRING      NOT NULL,                          -- workflow.id; Kafka partition key
    event_id     UUID        NOT NULL DEFAULT gen_random_uuid(),
    event_type   STRING      NOT NULL,
    trace_parent STRING      NOT NULL,
    payload      BYTES       NOT NULL,
    -- Projection column (additive, Sentinel-authorized exception to the schema lock): the viz
    -- gateway consumes this changefeed as JSON and must not decode the protobuf `payload`. The
    -- HLC sequence_engine_key is copied from the owning workflows row so the 3D timeline can order
    -- and place the node. It carries no exactly-once semantics — the CTE guard is unchanged.
    sequence_engine_key INT8  NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_orchestrator_outbox PRIMARY KEY (aggregate_id, event_id)
);

