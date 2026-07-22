-- BigQuery DDL for Inventory Intelligence Star Schema

-- 0. DIMENSIONS
CREATE TABLE dim_date (
    date_id INT64 NOT NULL,
    full_date DATE NOT NULL,
    year INT64 NOT NULL,
    quarter INT64 NOT NULL,
    month INT64 NOT NULL,
    day_of_month INT64 NOT NULL,
    day_of_week INT64 NOT NULL,
    is_weekend BOOLEAN NOT NULL
)
CLUSTER BY date_id;

CREATE TABLE dim_vendor (
    vendor_sk INT64 NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    vendor_name STRING NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
)
CLUSTER BY mdm_vendor_id;

CREATE TABLE dim_product (
    product_sk INT64 NOT NULL,
    sku STRING NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    brand STRING NOT NULL
)
CLUSTER BY sku;

CREATE TABLE dim_location (
    location_sk INT64 NOT NULL,
    location_id STRING NOT NULL,
    location_name STRING NOT NULL,
    region STRING NOT NULL,
    facility_type STRING NOT NULL
)
CLUSTER BY location_id;

-- 1. STAGING TABLE (For BQ Storage Write API Delta Load)
CREATE TABLE staging_inventory_movement (
    event_id STRING NOT NULL,
    trace_parent STRING,
    occurred_at TIMESTAMP NOT NULL,
    cdc_emit_ts TIMESTAMP NOT NULL,
    sequence_engine_key INT64 NOT NULL,
    sku STRING NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    location_id STRING NOT NULL,
    movement_type INT64 NOT NULL,
    qty_delta NUMERIC NOT NULL,
    fifo_unit_cost NUMERIC NOT NULL,
    fifo_total_value NUMERIC NOT NULL,
    moving_avg_cost NUMERIC NOT NULL
)
PARTITION BY DATE(occurred_at)
CLUSTER BY sku;

CREATE TABLE staging_inventory_snapshot (
    snapshot_id STRING NOT NULL,
    snapshot_date DATE NOT NULL,
    sku STRING NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    location_id STRING NOT NULL,
    qty_on_hand NUMERIC NOT NULL,
    fifo_total_value NUMERIC NOT NULL,
    moving_avg_cost NUMERIC NOT NULL,
    last_updated_at TIMESTAMP NOT NULL,
    sequence_engine_key INT64 NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY sku;

-- 2. FACT: Inventory Movement (Grain: Single Transaction)
CREATE TABLE fact_inventory_movement (
    event_id STRING NOT NULL,
    trace_parent STRING,
    occurred_at TIMESTAMP NOT NULL,
    cdc_emit_ts TIMESTAMP NOT NULL,
    sequence_engine_key INT64 NOT NULL,
    sku STRING NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    location_id STRING NOT NULL,
    movement_type INT64 NOT NULL,
    qty_delta NUMERIC NOT NULL,
    fifo_unit_cost NUMERIC NOT NULL,
    fifo_total_value NUMERIC NOT NULL,
    moving_avg_cost NUMERIC NOT NULL
)
PARTITION BY DATE(occurred_at)
CLUSTER BY sku;

-- 3. FACT: Inventory Snapshot (Grain: SKU x Day)
CREATE TABLE fact_inventory_snapshot (
    snapshot_id STRING NOT NULL,
    snapshot_date DATE NOT NULL,
    sku STRING NOT NULL,
    mdm_vendor_id STRING NOT NULL,
    location_id STRING NOT NULL,
    qty_on_hand NUMERIC NOT NULL,
    fifo_total_value NUMERIC NOT NULL,
    moving_avg_cost NUMERIC NOT NULL,
    last_updated_at TIMESTAMP NOT NULL,
    sequence_engine_key INT64 NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY sku;

-- 4. MERGE UPSERT TEMPLATES (For staging delta loading)

MERGE fact_inventory_movement F
USING staging_inventory_movement S
ON F.event_id = S.event_id
WHEN MATCHED AND S.sequence_engine_key > F.sequence_engine_key THEN
  UPDATE SET 
    trace_parent = S.trace_parent,
    occurred_at = S.occurred_at,
    cdc_emit_ts = S.cdc_emit_ts,
    sequence_engine_key = S.sequence_engine_key,
    sku = S.sku,
    mdm_vendor_id = S.mdm_vendor_id,
    location_id = S.location_id,
    movement_type = S.movement_type,
    qty_delta = S.qty_delta,
    fifo_unit_cost = S.fifo_unit_cost,
    fifo_total_value = S.fifo_total_value,
    moving_avg_cost = S.moving_avg_cost
WHEN NOT MATCHED THEN
  INSERT (
    event_id, trace_parent, occurred_at, cdc_emit_ts, sequence_engine_key, sku, 
    mdm_vendor_id, location_id, movement_type, qty_delta, fifo_unit_cost, 
    fifo_total_value, moving_avg_cost
  ) VALUES (
    S.event_id, S.trace_parent, S.occurred_at, S.cdc_emit_ts, S.sequence_engine_key, S.sku, 
    S.mdm_vendor_id, S.location_id, S.movement_type, S.qty_delta, S.fifo_unit_cost, 
    S.fifo_total_value, S.moving_avg_cost
  );

MERGE fact_inventory_snapshot F
USING staging_inventory_snapshot S
ON F.snapshot_id = S.snapshot_id
WHEN MATCHED AND S.sequence_engine_key > F.sequence_engine_key THEN
  UPDATE SET 
    snapshot_date = S.snapshot_date,
    sku = S.sku,
    mdm_vendor_id = S.mdm_vendor_id,
    location_id = S.location_id,
    qty_on_hand = S.qty_on_hand,
    fifo_total_value = S.fifo_total_value,
    moving_avg_cost = S.moving_avg_cost,
    last_updated_at = S.last_updated_at,
    sequence_engine_key = S.sequence_engine_key
WHEN NOT MATCHED THEN
  INSERT (
    snapshot_id, snapshot_date, sku, mdm_vendor_id, location_id, qty_on_hand, 
    fifo_total_value, moving_avg_cost, last_updated_at, sequence_engine_key
  ) VALUES (
    S.snapshot_id, S.snapshot_date, S.sku, S.mdm_vendor_id, S.location_id, S.qty_on_hand, 
    S.fifo_total_value, S.moving_avg_cost, S.last_updated_at, S.sequence_engine_key
  );
