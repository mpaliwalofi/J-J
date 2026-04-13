"""
Query Templates for ARIS Dashboard Metrics

These queries match the exact metrics shown in the ARIS dashboard.
All queries use the correct table names, column names, and aggregations.

FIX APPLIED:
  - predictive_otif_percentage: now computes on_time_probability * in_full_probability
    (was previously reusing Delivery Compliance (%) as a proxy, which was incorrect)
"""

# Dictionary of query templates for each dashboard metric
QUERY_TEMPLATES = {

    "otif_percentage": {
        "description": "OTIF % - Orders delivered On-Time and In-Full",
        "dashboard_value": "25.2%",
        "query": """
SELECT
  CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "Delivery Compliance (%)") AS NUMERIC(10,2)) AS otif_median,
  CAST(AVG("Delivery Compliance (%)") AS NUMERIC(10,2)) AS otif_average,
  COUNT(*) AS total_cases
FROM "Supply_Chain_KPI_Tuned"
WHERE "Delivery Compliance (%)" IS NOT NULL
        """,
        "use": "otif_median",
        "notes": "ARIS dashboard shows 25.2%, our median calculation gives 24.82% (very close match)"
    },

    "predictive_otif_percentage": {
        "description": "Predictive OTIF % - Predicted OTIF success probability (on_time_probability * in_full_probability)",
        "dashboard_value": "25.5%",
        "query": """
SELECT
  CAST(
    (
      SUM(CASE
        WHEN dd."Delivery_Date" IS NOT NULL
          AND so."Requested_Delivery_Date" IS NOT NULL
          AND dd."Delivery_Date" <= so."Requested_Delivery_Date"
        THEN 1.0 ELSE 0
      END) / NULLIF(COUNT(*), 0)
    )
    *
    (
      SUM(CASE
        WHEN so."Delivered Quantity" >= so."Order Quantity"
        THEN 1.0 ELSE 0
      END) / NULLIF(COUNT(*), 0)
    )
    * 100
  AS NUMERIC(10,2)) AS predictive_otif_pct
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE dd."Delivery_Date" IS NOT NULL
  AND so."Requested_Delivery_Date" IS NOT NULL
        """,
        "use": "predictive_otif_pct",
        "notes": "FIX: Was incorrectly using AVG(Delivery Compliance %) as a proxy. "
                 "Now correctly computes on_time_probability * in_full_probability. "
                 "Formula: (deliveries on time / total) * (deliveries in full / total) * 100"
    },

    "open_orders_count": {
        "description": "# Open Orders - Orders at risk or not yet delivered",
        "dashboard_value": "4.0k (4,000)",
        "query": """
SELECT
  SUM(CASE WHEN "Orders at Risk" > 0 THEN 1 ELSE 0 END) AS open_orders_count,
  CAST(SUM("Orders at Risk") AS INTEGER) AS total_risk_score
FROM "Supply_Chain_KPI_Single_Sheet"
        """,
        "use": "open_orders_count",
        "notes": "Returns 4,048 which matches ARIS 4.0k"
    },

    "total_value_eur": {
        "description": "Total Value (EUR) - Total order value",
        "dashboard_value": "219.0m",
        "query": """
SELECT
  CAST(SUM("Net Value") / 1000000 AS NUMERIC(10,1)) AS total_value_millions_eur
FROM "Sales Order DIM"
WHERE "Net Value" IS NOT NULL
        """,
        "notes": "Assuming Net Value is in EUR"
    },

    "delivery_on_time_percentage": {
        "description": "Delivery on time % - Percentage of deliveries before or on requested date",
        "dashboard_value": "52.5%",
        "query": """
SELECT
  CAST(100.0 * SUM(CASE
    WHEN dd."Delivery_Date" IS NOT NULL
      AND so."Requested_Delivery_Date" IS NOT NULL
      AND dd."Delivery_Date" <= so."Requested_Delivery_Date"
    THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS on_time_percentage,
  COUNT(*) AS total_with_dates
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE dd."Delivery_Date" IS NOT NULL
  AND so."Requested_Delivery_Date" IS NOT NULL
        """,
        "notes": "Returns 56.48%, close to ARIS 52.5%"
    },

    "in_full_probability": {
        "description": "In-Full Probability - Percentage of orders delivered in full",
        "dashboard_value": "N/A (derived metric)",
        "query": """
SELECT
  CAST(
    100.0 * SUM(CASE
      WHEN so."Delivered Quantity" >= so."Order Quantity"
      THEN 1 ELSE 0
    END) / NULLIF(COUNT(*), 0)
  AS NUMERIC(10,2)) AS in_full_probability_pct,
  COUNT(*) AS total_orders
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE so."Order Quantity" IS NOT NULL
  AND so."Delivered Quantity" IS NOT NULL
        """,
        "use": "in_full_probability_pct",
        "notes": "NEW metric — was missing from original templates. "
                 "Required as input to Predictive OTIF %. "
                 "Formula: (orders where delivered_qty >= order_qty) / total_orders * 100"
    },

    "avg_delay_duration_days": {
        "description": "Avg. Delay Duration - Average delay in days for late deliveries",
        "dashboard_value": "105d",
        "query": """
SELECT
  CAST(AVG("Total Lead Time (Days)") AS INTEGER) AS avg_lead_time_days,
  CAST(AVG(
    EXTRACT(EPOCH FROM (dd."Delivery_Date" - so."Requested_Delivery_Date")) / 86400
  ) AS INTEGER) AS avg_delay_days_calculated
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
WHERE dd."Delivery_Date" > so."Requested_Delivery_Date"
        """,
        "notes": "Can use pre-computed Total Lead Time or calculate from delivery dates"
    },

    "warehouse_count": {
        "description": "#Warehouse - Number of unique warehouses",
        "dashboard_value": "40",
        "query": """
SELECT
  COUNT(DISTINCT "WAREHOUSE_NUMBER") AS warehouse_count
FROM "Warehouse DIM"
WHERE "WAREHOUSE_NUMBER" IS NOT NULL
        """,
        "notes": "Count of distinct warehouses in the system"
    },

    "orders_with_warehouse_issues": {
        "description": "Order with Warehouse Issue - Count of orders with warehouse problems",
        "dashboard_value": "1.5k",
        "query": """
SELECT
  SUM(CASE
    WHEN "Warehouse Issue" != 'None' AND "Warehouse Issue" IS NOT NULL
    THEN 1 ELSE 0 END) AS orders_with_warehouse_issues
FROM "Supply_Chain_KPI_Single_Sheet"
        """,
        "notes": "Orders where warehouse issue is not 'None'"
    },

    "utilization_efficiency_percentage": {
        "description": "Utilization Efficiency (%) - Overall utilization efficiency",
        "dashboard_value": "60.4%",
        "query": """
SELECT
  CAST(AVG("Utilization Efficiency (%)") AS NUMERIC(10,2)) AS utilization_efficiency_avg
FROM "Supply_Chain_KPI_Tuned"
WHERE "Utilization Efficiency (%)" IS NOT NULL
        """,
        "notes": "Average of pre-computed utilization efficiency"
    },

    "savings_lost_millions": {
        "description": "Savings lost - Projected savings lost in 2025 (millions)",
        "dashboard_value": "3.2m",
        "query": """
SELECT
  CAST(SUM("Savings Lost (2025 Projection)") / 1000000 AS NUMERIC(10,1)) AS savings_lost_millions
FROM "Supply_Chain_KPI_Single_Sheet"
WHERE "Savings Lost (2025 Projection)" IS NOT NULL
        """,
        "notes": "Sum of savings lost from Single Sheet table (numeric column)"
    },

    "warehouse_utilization_percentage": {
        "description": "Daily Shift Capacity Utilization (%) - Warehouse capacity utilization",
        "dashboard_value": "Variable by warehouse",
        "query": """
SELECT
  CAST(AVG("Daily Shift Capacity Utilization (%)") AS NUMERIC(10,2)) AS warehouse_utilization_avg,
  CAST(MIN("Daily Shift Capacity Utilization (%)") AS NUMERIC(10,2)) AS warehouse_utilization_min,
  CAST(MAX("Daily Shift Capacity Utilization (%)") AS NUMERIC(10,2)) AS warehouse_utilization_max
FROM "Supply_Chain_KPI_Tuned"
WHERE "Daily Shift Capacity Utilization (%)" IS NOT NULL
        """,
        "notes": "Warehouse capacity utilization metric"
    },

}

# Query template for filtering by date/period
DATE_FILTER_TEMPLATES = {
    "by_month": {
        "description": "Filter by specific month (e.g., January 2024)",
        "example": """
SELECT ...
FROM "Supply_Chain_KPI_Tuned" sk
WHERE sk."Warehouse_Record_Date" LIKE '2024-01%'
        """
    },

    "by_year": {
        "description": "Filter by year (e.g., 2024)",
        "example": """
SELECT ...
FROM "Supply_Chain_KPI_Tuned" sk
WHERE sk."Warehouse_Record_Date" LIKE '2024%'
        """
    },

    "by_date_range": {
        "description": "Filter by date range using delivery date",
        "example": """
SELECT ...
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
WHERE dd."Delivery_Date" >= '2024-01-01'
  AND dd."Delivery_Date" < '2024-02-01'
        """
    }
}

# Common JOINs template
BASE_JOIN_TEMPLATE = """
-- Standard base query with all tables joined
FROM "Supply_Chain_KPI_Tuned" sk
LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
LEFT JOIN "Warehouse DIM" wd ON sk."Case ID" = wd."Case ID"
LEFT JOIN "Supply_Chain_KPI_Single_Sheet" ss ON sk."Case ID" = ss."Case ID"
"""


def get_query(metric_name: str) -> dict:
    """
    Get query template for a specific metric.

    Args:
        metric_name: Name of the metric (e.g., 'otif_percentage')

    Returns:
        Dictionary with query, description, and notes
    """
    return QUERY_TEMPLATES.get(metric_name, {
        "error": f"No query template found for metric: {metric_name}",
        "available_metrics": list(QUERY_TEMPLATES.keys())
    })


# Example usage
if __name__ == "__main__":
    print("Available Query Templates:")
    print("=" * 80)
    for metric, info in QUERY_TEMPLATES.items():
        print(f"\n{metric}:")
        print(f"  Description: {info['description']}")
        print(f"  Dashboard Value: {info['dashboard_value']}")
        if 'notes' in info:
            print(f"  Notes: {info['notes']}")