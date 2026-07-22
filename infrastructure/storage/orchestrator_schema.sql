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
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_orchestrator_outbox PRIMARY KEY (aggregate_id, event_id)
);

-- JSON changefeed, hardened Phase-1 topology: CRDB has no 'protobuf' changefeed format, and the
-- viz-gateway consumer decodes p2p.completed.v1 as JSON (json.NewDecoder + UseNumber). BYTES payload
-- is emitted as a \x-hex string, which the consumer already handles. Explicit topic, per-workflow
-- partition affinity, HLC emit-time for the WebGL timeline + Power BI latency, GC protection on pause.
CREATE CHANGEFEED FOR TABLE orchestrator_outbox
  INTO 'kafka://kafka:29092?topic_name=omniflow.p2p.completed.v1&tls_enabled=false'
  WITH
    format                   = 'json',
    key_column               = 'aggregate_id',
    unordered,
    updated,
    mvcc_timestamp,
    resolved                 = '10s',
    min_checkpoint_frequency = '10s',
    protect_data_from_gc_on_pause;
