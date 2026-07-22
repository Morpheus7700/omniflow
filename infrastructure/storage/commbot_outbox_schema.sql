-- infrastructure/storage/commbot_outbox_schema.sql
-- Consumer-side (CommBot) idempotency + transactional outbox.
-- Applied once by the CRDB init lifecycle step (after the enterprise license + rangefeed settings),
-- behind the same SHOW CHANGEFEED JOBS guard used for the Phase 1 outbox.
--
-- WHY: CommBot consumes an event, classifies it, then must emit a *classified* event onward.
-- Producing to Kafka directly after a DB write would reintroduce the dual-write problem. Instead
-- the Publisher writes the classified event to this outbox in the SAME transaction that claims the
-- idempotency key; a changefeed streams it to the orchestrator. No dual-write, exactly-once effect.

CREATE TABLE IF NOT EXISTS commbot_processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS commbot_outbox (
    aggregate_id STRING      NOT NULL,                       -- == mdm_vendor_id; Kafka partition key
    event_id     UUID        NOT NULL DEFAULT gen_random_uuid(),
    event_type   STRING      NOT NULL,                       -- 'VendorEmailClassified'
    trace_parent STRING      NOT NULL,                       -- W3C traceparent, propagated onward
    payload      BYTES       NOT NULL,                       -- serialized protobuf classified event
    occurred_at  TIMESTAMPTZ NOT NULL,                       -- business event time (carried through)
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_commbot_outbox PRIMARY KEY (aggregate_id, event_id)
);

