-- CockroachDB Operational Schema for Inventory Intelligence

-- Enable Enterprise Features for Changefeeds
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING cluster.organization = 'OmniFlow';
-- SET CLUSTER SETTING enterprise.license = '...'; -- Placeholder for actual license if needed

CREATE TABLE IF NOT EXISTS processed_events (
    event_id STRING PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT current_timestamp()
);

CREATE TABLE IF NOT EXISTS inventory_lots (
    lot_id STRING PRIMARY KEY,
    sku STRING NOT NULL,
    location_id STRING NOT NULL,
    sequence_engine_key INT8 NOT NULL,
    remaining_qty DECIMAL NOT NULL,
    unit_cost DECIMAL NOT NULL,
    INDEX idx_sku_loc_seq (sku, location_id, sequence_engine_key)
);

CREATE TABLE IF NOT EXISTS inventory_movements_log (
    event_id STRING PRIMARY KEY,
    trace_parent STRING,
    aggregate_id STRING,
    occurred_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP NOT NULL,
    cdc_emit_ts TIMESTAMP NOT NULL,
    sequence_engine_key INT8 NOT NULL,
    movement_type INT8 NOT NULL,
    sku STRING NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    location_id STRING NOT NULL,
    quantity DECIMAL NOT NULL,
    unit_cost DECIMAL NOT NULL,
    INDEX idx_log_sku_loc_seq (sku, location_id, sequence_engine_key ASC)
);

CREATE TABLE IF NOT EXISTS fact_inventory_movement (
    event_id STRING PRIMARY KEY,
    trace_parent STRING,
    occurred_at TIMESTAMP NOT NULL,
    cdc_emit_ts TIMESTAMP NOT NULL,
    sequence_engine_key INT8 NOT NULL,
    sku STRING NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    location_id STRING NOT NULL,
    movement_type INT8 NOT NULL,
    qty_delta DECIMAL NOT NULL,
    fifo_unit_cost DECIMAL NOT NULL,
    fifo_total_value DECIMAL NOT NULL,
    moving_avg_cost DECIMAL NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    snapshot_id STRING PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    sku STRING NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    location_id STRING NOT NULL,
    qty_on_hand DECIMAL NOT NULL,
    fifo_total_value DECIMAL NOT NULL,
    moving_avg_cost DECIMAL NOT NULL,
    last_updated_at TIMESTAMP NOT NULL,
    sequence_engine_key INT8 NOT NULL
);

-- CREATE CHANGEFEED to stream fact outbox tables into Kafka
-- These topics represent the staging ingestion points for BigQuery Storage Write API
CREATE CHANGEFEED FOR TABLE fact_inventory_movement, fact_inventory_snapshot
  INTO 'kafka://kafka:29092?tls_enabled=false'
  WITH updated, resolved='10s', mvcc_timestamp, format='json',
       topic_prefix='omniflow.inventory.', min_checkpoint_frequency='10s',
       protect_data_from_gc_on_pause;

