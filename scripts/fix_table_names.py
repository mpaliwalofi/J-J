"""
Fix KPI dependency graph and SQL to use actual database table names.

Actual database tables (from Supabase):
  - Delivery_Dim (not delivery_dim or Delivery DIM_csv)
  - Sales Order DIM (not sales_order_dim or Sales Order DIM_csv)
  - Warehouse DIM (not warehouse_dim or Warehouse DIM_csv)
  - Supply_Chain_KPI_Tuned (not Supply_Chain_KPI_Tuned_5500_csv)
  - Supply_Chain_KPI_Single_Sheet (not Supply_Chain_KPI_Single_Sheet_5500_csv)
  - cases (not aris_case or _ARIS.Case)
"""

import json
from pathlib import Path

# Table name mappings: KPI file name → Actual database name
TABLE_MAPPINGS = {
    "delivery_dim": "Delivery_Dim",
    "sales_order_dim": "Sales Order DIM",
    "warehouse_dim": "Warehouse DIM",
    "supply_chain_kpi": "Supply_Chain_KPI_Tuned",  # Using Tuned as primary
    "aris_case": "cases",
}

# Load the current dependency graph
graph_path = Path(__file__).parent.parent / "data" / "kpi_dependency_graph.json"
with open(graph_path, 'r') as f:
    graph = json.load(f)

print(f"Loaded graph with {graph['metadata']['total_kpis']} KPIs")

# Update all source_tables in the graph
fixed_count = 0
for kpi_name, kpi_info in graph['graph'].items():
    old_tables = kpi_info.get('source_tables', [])
    new_tables = []

    for old_table in old_tables:
        if old_table in TABLE_MAPPINGS:
            new_tables.append(TABLE_MAPPINGS[old_table])
            fixed_count += 1
        else:
            # Keep as-is if not in mapping
            new_tables.append(old_table)

    kpi_info['source_tables'] = new_tables

# Update metadata
graph['metadata']['corrected_table_names'] = True
graph['metadata']['actual_database_tables'] = list(set(TABLE_MAPPINGS.values()))

# Save updated graph
with open(graph_path, 'w') as f:
    json.dump(graph, f, indent=2)

print(f"✅ Fixed {fixed_count} table name references")
print(f"✅ Saved to: {graph_path}")
print("\nTable mappings applied:")
for old, new in TABLE_MAPPINGS.items():
    print(f"  {old} → {new}")
