# Power BI DAX Measures Proof

Because the `services/inventory-intelligence` service calculates the computationally expensive FIFO depletion and moving average metrics and writes them physically to BigQuery, the DAX required in the Power BI dashboard is completely trivial. We only rely on aggregations, eliminating the complexity and heavy processing associated with recursive or iterative row-by-row FIFO evaluation in DAX.

## Measures

### 1. Total Current Inventory Value (FIFO)
```dax
Total Inventory Value (FIFO) = 
SUM(fact_inventory_snapshot[fifo_total_value])
```

### 2. Total Current Quantity On Hand
```dax
Total Qty On Hand = 
SUM(fact_inventory_snapshot[qty_on_hand])
```

### 3. Inventory Value by Vendor (Dimensional Slice)
*Handled natively by the cross-filter context.*
```dax
// No special measure required. 
// Just drop 'Total Inventory Value (FIFO)' into a visual sliced by 'dim_vendor'[vendor_name].
```

### 4. Moving Average Cost
Because the moving average cost doesn't logically aggregate via `SUM` across multiple SKUs, we average the pre-computed snapshot row.
```dax
Avg Moving Cost = 
AVERAGE(fact_inventory_snapshot[moving_avg_cost])
```

## Why this is highly optimal:
If FIFO were calculated in DAX, it would require `EARLIER()` functions or nested iterative `SUMX` inside `FILTER` contexts to figure out which lots were consumed by which sales over time. Here, the `fact_inventory_snapshot` table has `fifo_total_value` precalculated for every single SKU on every single day natively from the backend's HLC-ordered event stream, making the DAX engine execute mere microseconds of math.
