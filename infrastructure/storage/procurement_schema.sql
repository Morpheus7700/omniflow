-- infrastructure/storage/procurement_schema.sql
-- Applied by crdb-init AFTER orchestrator_schema.sql — purchase_orders references workflows(id).
--
-- This file carries the state produced by autonomous agents, and its two constraints are the whole
-- reason an agent can be trusted to spend money here.

-- A durable workflow must carry its own input.
--
-- Node execution happens OUTSIDE the database transaction (correctly — an LLM call must not hold a
-- row lock). So when a pod dies mid-node and another picks the workflow up, the survivor has to
-- reconstruct what the node was working from. Without the trigger payload on the row it cannot: the
-- Kafka record that started the workflow belongs to a partition the survivor may not own. Storing
-- the payload is what makes resume possible at all.
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS trigger_payload BYTES;

CREATE TABLE IF NOT EXISTS purchase_orders (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id    UUID        NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    node_id        STRING      NOT NULL,
    attempt        INT4        NOT NULL,

    po_number      STRING      NOT NULL,
    mdm_vendor_id  STRING      NOT NULL,
    currency       STRING      NOT NULL,
    total_amount   DECIMAL(19,4) NOT NULL,
    lines          JSONB       NOT NULL,

    -- Provenance. Every PO points at the agent decision that produced it; a PO with no traceable
    -- decision is exactly the artifact a procurement auditor will refuse to accept.
    decision_id    UUID        NOT NULL,
    human_approved BOOL        NOT NULL DEFAULT false,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- THE exactly-once control for the agent's side effect.
    --
    -- Node execution is at-least-once by construction: the LLM call sits outside the transaction, so
    -- a crash between the call and the checkpoint re-runs the node. We do not try to prevent the
    -- duplicate CALL — that would need distributed consensus around a third party we do not control.
    -- We make the EFFECT idempotent instead. The key is deterministic in (workflow, node, attempt)
    -- and the insert is ON CONFLICT DO NOTHING, so N executions of a node yield exactly one PO.
    --
    -- This is the honest version of "exactly-once": at-least-once delivery, at-least-once execution,
    -- exactly-once effect.
    CONSTRAINT uq_po_per_node UNIQUE (workflow_id, node_id, attempt),

    -- po_number is derived from the same triple, so it is stable across retries rather than being a
    -- second source of truth that could drift from the constraint above.
    CONSTRAINT uq_po_number UNIQUE (po_number)
);

-- The governance trail. One row per agent call — accepted, refused on policy, or failed.
--
-- Written on EVERY outcome, deliberately. A trail that records only successful calls cannot answer
-- "did the agent try to do something we stopped it from doing", which is the question that actually
-- matters in a review.
CREATE TABLE IF NOT EXISTS agent_decisions (
    id                UUID PRIMARY KEY,
    workflow_id       UUID        NOT NULL,
    node_id           STRING      NOT NULL,
    attempt           INT4        NOT NULL,

    agent_id          STRING      NOT NULL,
    model             STRING      NOT NULL,

    -- The hash travels on the event bus; the body stays here. A prompt built from vendor
    -- correspondence is not something to broadcast to every consumer of the topic.
    prompt_sha256     STRING      NOT NULL,
    prompt            STRING      NOT NULL,
    raw_response      STRING      NOT NULL,

    prompt_tokens     INT4        NOT NULL DEFAULT 0,
    completion_tokens INT4        NOT NULL DEFAULT 0,
    -- Micro-USD (1e-6). Integral: money as a float is a defect waiting for a rounding complaint.
    cost_micro_usd    INT8        NOT NULL DEFAULT 0,
    latency_ms        INT8        NOT NULL DEFAULT 0,

    outcome           STRING      NOT NULL,
    reject_reason     STRING      NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    INDEX idx_agent_decisions_workflow (workflow_id)
);

-- Budget enforcement reads through this index: sum(cost_micro_usd) for a workflow is evaluated
-- before every agent call, so the cap holds across pods and across retries rather than per-process.
