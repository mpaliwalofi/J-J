"""
Query Templates Using the 'cases' Table

The 'cases' table stores ARIS KPI results as key-value pairs.
This is the SOURCE OF TRUTH that matches the ARIS dashboard exactly!

FIX APPLIED:
  - Added 'predictive_otif_percentage' to CASES_TABLE_KPIS
  - Added 'in_full_probability' to CASES_TABLE_KPIS (new KPI that was missing)
"""

# Query template to get ANY KPI from the cases table
def get_kpi_from_cases(kpi_name: str) -> str:
    """
    Get a KPI value from the cases table.

    Args:
        kpi_name: The KPI name as stored in cases.name

    Returns:
        SQL query string
    """
    return f"""
SELECT
    name,
    value,
    created_at
FROM cases
WHERE name = '{kpi_name}'
ORDER BY created_at DESC
LIMIT 1
"""

# All KPIs available in the cases table
CASES_TABLE_KPIS = {
    "delivery_on_time_percentage": {
        "name": "Delivery on time %",
        "query": get_kpi_from_cases("Delivery on time %"),
        "description": "Delivery on time percentage",
        "expected": "52.5%"
    },

    "avg_delay_days": {
        "name": "Avg. Delay days",
        "query": get_kpi_from_cases("Avg. Delay days"),
        "description": "Average delay duration in days",
        "expected": "105d"
    },

    "materials_count": {
        "name": "# Materials",
        "query": get_kpi_from_cases("# Materials"),
        "description": "Number of materials",
        "expected": "2.6k"
    },

    "orders_at_risk_percentage": {
        "name": "Orders At Risk",
        "query": get_kpi_from_cases("Orders At Risk"),
        "description": "Percentage of orders at risk",
        "expected": "77.1%"
    },

    "delay_order_value": {
        "name": "Delay Order Value(EUR)",
        "query": get_kpi_from_cases("Delay Order Value(EUR)"),
        "description": "Value of delayed orders in EUR",
        "expected": "18.0m"
    },

    # FIX: Added Predictive OTIF % — was missing from original
    # This KPI was previously broken (was squaring on_time_probability).
    # If stored in the cases table, this will return the correct ARIS value.
    "predictive_otif_percentage": {
        "name": "Predictive OTIF %",
        "query": get_kpi_from_cases("Predictive OTIF %"),
        "description": "Predicted OTIF success probability (on_time_probability * in_full_probability)",
        "expected": "25.5%",
        "notes": "FIX: Formula corrected. Was on_time_prob^2, now on_time_prob * in_full_prob."
    },

    # FIX: Added In-Full Probability — new KPI that was entirely missing from the graph
    # This feeds directly into Predictive OTIF % as a required input.
    "in_full_probability": {
        "name": "In-Full Probability",
        "query": get_kpi_from_cases("In-Full Probability"),
        "description": "Percentage of orders delivered in full",
        "expected": "N/A — derived metric, check ARIS dashboard",
        "notes": "NEW KPI added to dependency graph. Required by Predictive OTIF %."
    },
}


# Example usage
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from db.query_runner import QueryRunner

    db = QueryRunner()

    print("=== KPIs from cases table ===\n")

    for key, info in CASES_TABLE_KPIS.items():
        print(f"{info['name']}:")
        print(f"  Expected: {info['expected']}")
        if 'notes' in info:
            print(f"  Notes:    {info['notes']}")

        result = db.execute(info['query'])
        if result:
            print(f"  Actual:   {result[0]['value']}")
            match = str(result[0]['value']) == info['expected']
            print(f"  Match:    {'YES ✓' if match else 'NO — verify formula'}")
        else:
            print(f"  Actual:   NOT FOUND in cases table")
        print()