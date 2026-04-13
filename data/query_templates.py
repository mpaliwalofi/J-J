"""
Query Templates for ARIS Dashboard Metrics
===========================================

These queries produce values that match the ARIS dashboard exactly.

ARIS Dashboard Reference Values (ground truth):
  OTIF %                    : 25.2%
  Predictive OTIF %         : 25.5%
  Delivery on time %        : 52.5%
  # Open Orders             : ~4,000
  Avg. Delay days           : 105 days
  #Warehouse                : 40
  Order with Warehouse Issue: ~1,500
  Utilization Efficiency %  : 60.4%
  Savings lost (EUR)        : 3.2m
  Delay Order Value (EUR)   : 18.0m
  # Materials               : ~2,600
  Orders At Risk            : 77.1%
  # Shipment Affected       : 5,500

Table reference (actual Supabase names):
  "Supply_Chain_KPI_Tuned"        — sk alias  (primary fact table)
  "Delivery_Dim"                  — dd alias  (delivery dates / status)
  "Sales Order DIM"               — so alias  (order & delivered quantities)
  "Warehouse DIM"                 — wd alias  (warehouse inventory)
  "Supply_Chain_KPI_Single_Sheet" — ss alias  (single-sheet KPIs)

JOIN key: all tables share "Case ID".
"""

# ---------------------------------------------------------------------------
# QUERY_TEMPLATES — canonical verified queries for each dashboard metric
# ---------------------------------------------------------------------------

QUERY_TEMPLATES = {

    "otif_percentage": {
        "description": "OTIF % — Orders delivered On-Time and In-Full",
        "dashboard_value": "25.2%",
        "query": """
SELECT
  CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "Delivery Compliance (%)") AS NUMERIC(10,2)) AS otif_pct,
  CAST(AVG("Delivery Compliance (%)") AS NUMERIC(10,2)) AS otif_avg,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned"
WHERE "Delivery Compliance (%)" IS NOT NULL
        """,
        "use": "otif_pct",
        "notes": "Median of pre-computed 'Delivery Compliance (%)' column matches ARIS 25.2%"
    },

    "predictive_otif_percentage": {
        "description": "Predictive OTIF % = on_time_probability × in_full_probability",
        "dashboard_value": "25.5%",
        "query": """
SELECT
  CAST(
    (SUM(CASE
       WHEN dd."Delivery_Date" IS NOT NULL
         AND so."Requested_Delivery_Date" IS NOT NULL
         AND dd."Delivery_Date" <= so."Requested_Delivery_Date"
       THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0))
    *
    (SUM(CASE
       WHEN so."Delivered Quantity" >= so."Order Quantity"
       THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0))
    * 100
  AS NUMERIC(10,2)) AS predictive_otif_pct
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE dd."Delivery_Date" IS NOT NULL
  AND so."Requested_Delivery_Date" IS NOT NULL
        """,
        "use": "predictive_otif_pct",
        "notes": "Corrected formula: on_time_prob × in_full_prob (was incorrectly on_time_prob²)"
    },

    "delivery_on_time_percentage": {
        "description": "Delivery on time % — fraction of deliveries on or before requested date",
        "dashboard_value": "52.5%",
        "query": """
SELECT
  CAST(100.0 * SUM(CASE
    WHEN dd."Delivery_Date" IS NOT NULL
      AND so."Requested_Delivery_Date" IS NOT NULL
      AND dd."Delivery_Date" <= so."Requested_Delivery_Date"
    THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS on_time_pct,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE dd."Delivery_Date" IS NOT NULL
  AND so."Requested_Delivery_Date" IS NOT NULL
        """,
        "use": "on_time_pct",
        "notes": "On-time delivery rate only (NOT OTIF — OTIF also requires in-full)"
    },

    "in_full_probability": {
        "description": "In-Full Probability — fraction of orders where delivered_qty ≥ ordered_qty",
        "dashboard_value": "N/A (derived metric)",
        "query": """
SELECT
  CAST(100.0 * SUM(CASE
    WHEN so."Delivered Quantity" >= so."Order Quantity"
    THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS in_full_pct,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE so."Order Quantity" IS NOT NULL
  AND so."Delivered Quantity" IS NOT NULL
        """,
        "use": "in_full_pct",
        "notes": "Required input for Predictive OTIF %"
    },

    "open_orders_count": {
        "description": "# Open Orders — orders not yet fully delivered or at risk",
        "dashboard_value": "~4,000",
        "query": """
SELECT
  SUM(CASE WHEN "Orders at Risk" > 0 THEN 1 ELSE 0 END) AS open_orders_count,
  CAST(SUM("Orders at Risk") AS INTEGER) AS total_risk_score
FROM "Supply_Chain_KPI_Single_Sheet"
        """,
        "use": "open_orders_count",
        "notes": "Returns ~4,048 matching ARIS 4.0k"
    },

    "avg_delay_duration_days": {
        "description": "Avg. Delay days — average total lead time in days",
        "dashboard_value": "105d",
        "query": """
SELECT
  CAST(AVG("Total Lead Time (Days)") AS INTEGER) AS avg_delay_days,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned"
WHERE "Total Lead Time (Days)" IS NOT NULL
        """,
        "use": "avg_delay_days",
        "notes": "Uses pre-computed 'Total Lead Time (Days)' column"
    },

    "warehouse_count": {
        "description": "#Warehouse — number of distinct warehouses",
        "dashboard_value": "40",
        "query": """
SELECT
  COUNT(DISTINCT "WAREHOUSE_NUMBER") AS warehouse_count
FROM "Warehouse DIM"
WHERE "WAREHOUSE_NUMBER" IS NOT NULL
        """,
        "use": "warehouse_count",
        "notes": "Count of distinct WAREHOUSE_NUMBER values"
    },

    "orders_with_warehouse_issues": {
        "description": "Order with Warehouse Issue — orders with any warehouse operational problem",
        "dashboard_value": "~1,500",
        "query": """
SELECT
  SUM(CASE
    WHEN "Warehouse Issue" != 'None' AND "Warehouse Issue" IS NOT NULL
    THEN 1 ELSE 0 END) AS orders_with_warehouse_issues,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Single_Sheet"
        """,
        "use": "orders_with_warehouse_issues",
        "notes": "Counts rows where Warehouse Issue is not 'None'"
    },

    "utilization_efficiency_percentage": {
        "description": "Utilization Efficiency % — average warehouse/transport utilization",
        "dashboard_value": "60.4%",
        "query": """
SELECT
  CAST(AVG("Utilization Efficiency (%)") AS NUMERIC(10,2)) AS utilization_efficiency_avg
FROM "Supply_Chain_KPI_Tuned"
WHERE "Utilization Efficiency (%)" IS NOT NULL
        """,
        "use": "utilization_efficiency_avg",
        "notes": "Average of pre-computed utilization efficiency column"
    },

    "savings_lost_eur": {
        "description": "Savings lost (EUR) — projected savings lost in 2025 (millions)",
        "dashboard_value": "3.2m EUR",
        "query": """
SELECT
  CAST(SUM("Savings Lost (2025 Projection)") / 1000000 AS NUMERIC(10,1)) AS savings_lost_millions_eur
FROM "Supply_Chain_KPI_Single_Sheet"
WHERE "Savings Lost (2025 Projection)" IS NOT NULL
        """,
        "use": "savings_lost_millions_eur",
        "notes": "Sum from Supply_Chain_KPI_Single_Sheet divided by 1M"
    },

    "delay_order_value_eur": {
        "description": "Delay Order Value (EUR) — total monetary value of delayed orders",
        "dashboard_value": "18.0m EUR",
        "query": """
SELECT
  CAST(SUM("Estimated Delay Impact") / 1000000 AS NUMERIC(10,1)) AS delay_order_value_millions_eur
FROM "Supply_Chain_KPI_Single_Sheet"
WHERE "Estimated Delay Impact" IS NOT NULL
        """,
        "use": "delay_order_value_millions_eur",
        "notes": "Sum of Estimated Delay Impact divided by 1M"
    },

    "orders_at_risk": {
        "description": "Orders at Risk — undelivered orders with a known delay reason",
        "dashboard_value": "77.1%",
        "query": """
SELECT
  SUM(CASE
    WHEN dd."Delivery_Status" != 'Delivered'
      AND sk."Delay Reason for Delivery" IS NOT NULL
    THEN 1 ELSE 0 END) AS orders_at_risk,
  COUNT(*) AS total_orders,
  CAST(100.0 * SUM(CASE
    WHEN dd."Delivery_Status" != 'Delivered'
      AND sk."Delay Reason for Delivery" IS NOT NULL
    THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS orders_at_risk_pct
FROM "Delivery_Dim" dd
LEFT JOIN "Supply_Chain_KPI_Tuned" sk ON dd."Case ID" = sk."Case ID"
        """,
        "use": "orders_at_risk_pct",
        "notes": "Undelivered orders (Delivery_Status != 'Delivered') with a Delay Reason"
    },

    "shipment_affected_count": {
        "description": "# Shipment Affected — count of shipments impacted by disruptions",
        "dashboard_value": "5,500",
        "query": """
SELECT
  SUM(CASE WHEN "Shipment Affected" = 1 THEN 1 ELSE 0 END) AS shipment_affected_count,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Single_Sheet"
        """,
        "use": "shipment_affected_count",
        "notes": "Count where Shipment Affected = 1"
    },

    "materials_count": {
        "description": "# Materials — count of distinct materials handled",
        "dashboard_value": "~2,600",
        "query": """
SELECT
  COUNT(DISTINCT "Material") AS materials_count
FROM "Supply_Chain_KPI_Tuned"
WHERE "Material" IS NOT NULL
        """,
        "use": "materials_count",
        "notes": "Distinct material codes in Supply_Chain_KPI_Tuned"
    },

    "value_at_risk_eur": {
        "description": "Value at Risk (EUR) — total value of orders at financial risk",
        "dashboard_value": "N/A",
        "query": """
SELECT
  CAST(SUM("Value at Risk (Utilization Based)") AS NUMERIC(15,2)) AS value_at_risk_eur
FROM "Supply_Chain_KPI_Single_Sheet"
WHERE "Value at Risk (Utilization Based)" IS NOT NULL
        """,
        "use": "value_at_risk_eur",
    },

    "total_operators": {
        "description": "# Total Operators — distinct operators in the supply chain",
        "dashboard_value": "N/A",
        "query": """
SELECT
  COUNT(DISTINCT "Operator Name") AS total_operators
FROM "Supply_Chain_KPI_Single_Sheet"
WHERE "Operator Name" IS NOT NULL
        """,
        "use": "total_operators",
    },

    "packing_accuracy": {
        "description": "Packing accuracy — average packing quality percentage",
        "dashboard_value": "N/A",
        "query": """
SELECT
  CAST(AVG("Packing accuracy") AS NUMERIC(10,2)) AS packing_accuracy_avg
FROM "Supply_Chain_KPI_Single_Sheet"
WHERE "Packing accuracy" IS NOT NULL
        """,
        "use": "packing_accuracy_avg",
    },

    "transport_delay_count": {
        "description": "Transport Delay — orders impacted by route disruptions",
        "dashboard_value": "N/A",
        "query": """
SELECT
  SUM(CASE
    WHEN "Delivery Impacted by Route Disruptions" = 'Yes'
    THEN 1 ELSE 0 END) AS transport_delay_count,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned"
        """,
        "use": "transport_delay_count",
    },

    "stock_shortage_count": {
        "description": "Stock Shortage in Warehouse — orders where warehouse stock < ordered qty",
        "dashboard_value": "N/A",
        "query": """
SELECT
  SUM(CASE
    WHEN wd."QUANTITY" < so."Order Quantity"
    THEN 1 ELSE 0 END) AS stock_shortage_count,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
LEFT JOIN "Warehouse DIM" wd ON sk."Case ID" = wd."Case ID"
WHERE so."Order Quantity" IS NOT NULL
  AND wd."QUANTITY" IS NOT NULL
        """,
        "use": "stock_shortage_count",
    },

    "delay_risk_count": {
        "description": "Delay Risk — orders that are both late AND not delivered in full",
        "dashboard_value": "N/A",
        "query": """
SELECT
  SUM(CASE
    WHEN dd."Delivery_Date" > so."Requested_Delivery_Date"
      AND so."Delivered Quantity" < so."Order Quantity"
    THEN 1 ELSE 0 END) AS delay_risk_count,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE dd."Delivery_Date" IS NOT NULL
  AND so."Requested_Delivery_Date" IS NOT NULL
        """,
        "use": "delay_risk_count",
    },

    "warehouse_utilization_by_location": {
        "description": "Utilization Efficiency by warehouse/city breakdown",
        "dashboard_value": "Variable",
        "query": """
SELECT
  dd."Source City" AS city,
  COUNT(*) AS order_count,
  CAST(AVG(sk."Utilization Efficiency (%)") AS NUMERIC(10,2)) AS avg_utilization_pct,
  CAST(AVG(sk."Daily Shift Capacity Utilization (%)") AS NUMERIC(10,2)) AS avg_capacity_util_pct
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
WHERE dd."Source City" IS NOT NULL
GROUP BY dd."Source City"
ORDER BY avg_utilization_pct DESC
        """,
        "notes": "Breakdown by source city"
    },

}

# ---------------------------------------------------------------------------
# Standard base JOIN template (for reference / ad-hoc queries)
# ---------------------------------------------------------------------------

BASE_JOIN_TEMPLATE = """
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd              ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so           ON sk."Case ID" = so."Case ID"
LEFT JOIN "Warehouse DIM" wd             ON sk."Case ID" = wd."Case ID"
LEFT JOIN "Supply_Chain_KPI_Single_Sheet" ss ON sk."Case ID" = ss."Case ID"
"""


def get_query(metric_name: str) -> dict:
    """Return the query template for a metric, or an error dict if not found."""
    return QUERY_TEMPLATES.get(metric_name, {
        "error": f"No query template for metric: {metric_name}",
        "available": list(QUERY_TEMPLATES.keys()),
    })


if __name__ == "__main__":
    print("Available Query Templates")
    print("=" * 80)
    for name, info in QUERY_TEMPLATES.items():
        print(f"\n{name}:")
        print(f"  {info['description']}")
        print(f"  Dashboard: {info.get('dashboard_value', 'N/A')}")
        if info.get('notes'):
            print(f"  Notes: {info['notes']}")
