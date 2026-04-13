-- ============================================================
-- KPI SQL Query — Auto-generated from ARIS KPI Definitions
-- Source: C:\Users\Abcom\J&J\j&j\data\kpi_definitions 1.csv
-- Graph:  C:\Users\Abcom\J&J\j&j\data\kpi_dependency_graph.json
-- Target: Supabase PostgreSQL
-- Generated: 2026-04-13
-- FIX APPLIED: Added In-Full Probability to Layer 2
--              Fixed Predictive OTIF % to use on_time_probability * in_full_probability
--              (was incorrectly: on_time_probability * on_time_probability)
-- ============================================================

-- Dependency Order (topological sort):
-- Layer 0 (raw inputs): Actual Delivery Date, Requested Delivery Date, Delivery Number, City, InFull, Shipment Affected, Stock Shortage in Warehouse, Transport Delay, Warehouse Issue
-- Layer 1: On Time, Avg Delay Duration, Delay Risk, Open Orders, Orders at Risk, OTIF
-- Layer 2: Avg Days Delay Duration, On Time Probability, In-Full Probability [NEW], Risk Ratio
-- Layer 3: Days Range Delay, Check Predictive OTIF, Predictive OTIF %

-- ============================================================
-- BASE CTE: Join all source tables
-- ============================================================
WITH base AS (
  SELECT
    -- Primary keys
    ac.case_id,
    so.sales_order_number,
    dd.delivery_number,

    -- Raw columns from Delivery DIM
    dd.delivery_date AS actual_delivery_date_raw,
    dd.source_city,
    dd.delivery_status,

    -- Raw columns from Sales Order DIM
    so.requested_delivery_date AS requested_delivery_date_raw,
    so.order_quantity,
    so.delivered_quantity,

    -- Raw columns from Warehouse DIM
    wd.quantity AS warehouse_quantity,

    -- Raw columns from Supply Chain KPI
    sk.shipment_affected,
    sk.delivery_impacted_by_route_disruptions,
    sk.delay_reason_for_delivery,
    sk.warehouse_issue,

    -- ARIS Case columns (if any pre-computed fields exist)
    ac.avg_delay_duration_ms

  FROM aris_case ac
  LEFT JOIN sales_order_dim so
    ON ac.sales_order_number = so.sales_order_number
  LEFT JOIN delivery_dim dd
    ON ac.sales_order_number = dd.sales_order_number
  LEFT JOIN warehouse_dim wd
    ON ac.sales_order_number = wd.sales_order_number
  LEFT JOIN supply_chain_kpi sk
    ON ac.sales_order_number = sk.sales_order_number
),

-- ============================================================
-- LAYER 0: Raw source KPIs (direct column mappings)
-- ============================================================
layer_0 AS (
  SELECT
    *,

    -- KPI: Actual Delivery Date | Layer: 0
    -- Definition: Date when the product was actually delivered to the customer.
    actual_delivery_date_raw AS actual_delivery_date,

    -- KPI: Requested Delivery Date | Layer: 0
    -- Definition: Delivery date requested by the customer.
    CAST(requested_delivery_date_raw AS DATE) AS requested_delivery_date,

    -- KPI: Delivery Number | Layer: 0
    -- Definition: Unique identifier for each delivery.
    delivery_number,

    -- KPI: City | Layer: 0
    -- Definition: Source city of shipment.
    source_city AS city,

    -- KPI: InFull | Layer: 0
    -- Definition: Checks whether delivered quantity meets ordered quantity.
    -- Depends on: delivered_quantity, order_quantity
    CASE
      WHEN delivered_quantity >= order_quantity THEN 1
      ELSE 0
    END AS infull,

    -- KPI: Shipment Affected | Layer: 0
    -- Definition: Indicates shipment affected by disruption.
    CASE
      WHEN shipment_affected = 1 THEN 1
      ELSE 0
    END AS shipment_affected_kpi,

    -- KPI: Stock Shortage in Warehouse | Layer: 0
    -- Definition: Detects insufficient warehouse inventory.
    -- Depends on: warehouse_quantity, order_quantity
    CASE
      WHEN warehouse_quantity < order_quantity THEN 1
      ELSE 0
    END AS stock_shortage_in_warehouse,

    -- KPI: Transport Delay | Layer: 0
    -- Definition: Delay caused by route disruptions.
    CASE
      WHEN delivery_impacted_by_route_disruptions = 'Yes' THEN 1
      ELSE 0
    END AS transport_delay,

    -- KPI: Warehouse Issue | Layer: 0
    -- Definition: Operational issues occurring in warehouse.
    CASE
      WHEN warehouse_issue != 'None' AND warehouse_issue IS NOT NULL THEN 1
      ELSE 0
    END AS warehouse_issue_kpi

  FROM base
),

-- ============================================================
-- LAYER 1: First-level derived KPIs
-- ============================================================
layer_1 AS (
  SELECT
    *,

    -- KPI: On Time | Layer: 1
    -- Definition: Checks whether delivery occurred before or on requested date.
    -- Depends on: Actual Delivery Date, Requested Delivery Date
    CASE
      WHEN actual_delivery_date IS NOT NULL
        AND requested_delivery_date IS NOT NULL
        AND actual_delivery_date <= requested_delivery_date
      THEN 1
      ELSE 0
    END AS on_time,

    -- KPI: Avg Delay Duration | Layer: 1
    -- Definition: Time difference between requested and actual delivery when late (in milliseconds).
    -- Depends on: Actual Delivery Date, Requested Delivery Date
    CASE
      WHEN actual_delivery_date > requested_delivery_date
      THEN EXTRACT(EPOCH FROM (actual_delivery_date - requested_delivery_date)) * 1000
      ELSE NULL
    END AS avg_delay_duration,

    -- KPI: Delay Risk | Layer: 1
    -- Definition: Identifies orders delayed and not fully delivered.
    -- Depends on: Actual Delivery Date, Requested Delivery Date, InFull
    CASE
      WHEN actual_delivery_date > requested_delivery_date
        AND infull = 0
      THEN 1
      ELSE 0
    END AS delay_risk,

    -- KPI: Open Orders | Layer: 1
    -- Definition: Orders not yet fully delivered or partially delivered.
    CASE
      WHEN delivery_status != 'Delivered'
        OR delivered_quantity < order_quantity
      THEN 1
      ELSE 0
    END AS open_orders,

    -- KPI: Orders at Risk | Layer: 1
    -- Definition: Orders likely to be delayed due to issues (e.g. Carrier, traffic, weather).
    CASE
      WHEN delivery_status != 'Delivered'
        AND delay_reason_for_delivery IS NOT NULL
      THEN 1
      ELSE 0
    END AS orders_at_risk,

    -- KPI: OTIF | Layer: 1
    -- Definition: Orders delivered On-Time and In-Full (core KPI).
    -- Depends on: Actual Delivery Date, Requested Delivery Date, InFull
    CASE
      WHEN actual_delivery_date <= requested_delivery_date
        AND infull = 1
      THEN 1
      ELSE 0
    END AS otif

  FROM layer_0
),

-- ============================================================
-- LAYER 2: Second-level derived KPIs
-- ============================================================
layer_2 AS (
  SELECT
    *,

    -- KPI: Avg Days Delay Duration | Layer: 2
    -- Definition: Average delay duration converted from milliseconds to days.
    -- Depends on: Avg Delay Duration
    avg_delay_duration / 86400000.0 AS avg_days_delay_duration,

    -- KPI: On Time Probability | Layer: 2
    -- Definition: Percentage of orders delivered on time (aggregated metric).
    -- Depends on: On Time
    SUM(on_time) OVER () * 1.0 / NULLIF(COUNT(*) OVER (), 0) AS on_time_probability,

    -- KPI: In-Full Probability | Layer: 2  [NEW — was missing from original]
    -- Definition: Percentage of orders delivered in full (aggregated metric).
    -- Depends on: InFull
    -- Required by: Predictive OTIF %
    SUM(infull) OVER () * 1.0 / NULLIF(COUNT(*) OVER (), 0) AS in_full_probability,

    -- KPI: Risk Ratio | Layer: 2
    -- Definition: Combined risk indicator (average of three risk signals).
    -- Depends on: Delay Risk, Stock Shortage in Warehouse, Transport Delay
    (delay_risk + stock_shortage_in_warehouse + transport_delay) / 3.0 AS risk_ratio

  FROM layer_1
),

-- ============================================================
-- LAYER 3: Third-level derived KPIs (final layer)
-- ============================================================
layer_3 AS (
  SELECT
    *,

    -- KPI: Days Range Delay | Layer: 3
    -- Definition: Categorizes delay duration into ranges.
    -- Depends on: Avg Days Delay Duration
    CASE
      WHEN avg_days_delay_duration > 0 AND avg_days_delay_duration <= 10 THEN 'A.1-10'
      WHEN avg_days_delay_duration >= 11 AND avg_days_delay_duration <= 20 THEN 'B.11-20'
      WHEN avg_days_delay_duration >= 21 AND avg_days_delay_duration <= 30 THEN 'C.21-30'
      WHEN avg_days_delay_duration >= 31 AND avg_days_delay_duration <= 40 THEN 'D.31-40'
      ELSE 'Delay More than 40 Days'
    END AS days_range_delay,

    -- KPI: Check Predictive OTIF | Layer: 3
    -- Definition: Predicts whether an order is likely to fail OTIF (threshold: 0.4).
    -- Depends on: Risk Ratio
    CASE
      WHEN risk_ratio > 0.4 THEN 1
      ELSE 0
    END AS check_predictive_otif,

    -- KPI: Predictive OTIF % | Layer: 3
    -- Definition: Predicted OTIF success probability.
    -- Depends on: On Time Probability, In-Full Probability
    -- FIX: Was incorrectly on_time_probability * on_time_probability
    --      Corrected to on_time_probability * in_full_probability
    on_time_probability * in_full_probability AS predictive_otif_pct

  FROM layer_2
),

-- ============================================================
-- FINAL: Select all KPIs
-- ============================================================
final AS (
  SELECT
    -- Primary identifiers
    case_id,
    sales_order_number,
    delivery_number,

    -- Layer 0 KPIs (Raw source columns)
    actual_delivery_date,
    requested_delivery_date,
    city,
    infull,
    shipment_affected_kpi AS shipment_affected,
    stock_shortage_in_warehouse,
    transport_delay,
    warehouse_issue_kpi AS warehouse_issue,

    -- Layer 1 KPIs
    on_time,
    avg_delay_duration,
    delay_risk,
    open_orders,
    orders_at_risk,
    otif,

    -- Layer 2 KPIs
    avg_days_delay_duration,
    on_time_probability,
    in_full_probability,        -- NEW: was missing in original
    risk_ratio,

    -- Layer 3 KPIs
    days_range_delay,
    check_predictive_otif,
    predictive_otif_pct AS predictive_otif_percentage,

    -- Additional context columns for debugging/analysis
    order_quantity,
    delivered_quantity,
    delivery_status,
    warehouse_quantity,
    delay_reason_for_delivery

  FROM layer_3
)

-- ============================================================
-- Main query: Return all computed KPIs
-- ============================================================
SELECT * FROM final;

-- ============================================================
-- USAGE NOTES:
-- ============================================================
-- 1. This query computes all 22 KPIs in dependency order using CTEs
--    (21 original + In-Full Probability which was missing)
-- 2. Each layer builds on the previous layer's computed columns
-- 3. NULL handling is applied throughout to prevent computation errors
-- 4. Aggregate KPIs (On Time Probability, In-Full Probability) use window functions
-- 5. For filtered queries, add WHERE clauses to the final SELECT
-- 6. For specific KPI subsets, see generated kpi_<name>_query.sql files
--
-- Example filtered query:
-- SELECT * FROM final
-- WHERE requested_delivery_date >= '2024-01-01'
--   AND requested_delivery_date < '2024-04-01';
--
-- Example aggregated metrics:
-- SELECT
--   AVG(otif) AS otif_rate,
--   AVG(on_time) AS on_time_rate,
--   AVG(infull) AS infull_rate,
--   AVG(in_full_probability) AS in_full_prob,
--   AVG(on_time_probability) AS on_time_prob,
--   AVG(predictive_otif_percentage) AS predictive_otif,
--   COUNT(*) AS total_orders
-- FROM final;
-- ============================================================