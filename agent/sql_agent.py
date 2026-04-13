"""
SQL Agent — Two-Stage NL → PQL → SQL Pipeline

Stage 1: Natural Language → PQL
  - Identifies the KPI from the user's query (keyword matching + intent fallback)
  - Returns the PQL expression from kpi_definitions.csv (the authoritative source)
  - PQL is J&J/ARIS Process Query Language — see kpi_definitions.csv PQL Logic column

Stage 2: PQL → SQL (PostgreSQL)
  - Translates PQL to valid PostgreSQL SQL using the PQLTranslator class
  - Applies table name mapping, function translation, and JOIN building
  - For pre-aggregated ARIS KPIs: routes directly to the 'cases' table (exact values)
  - For computed KPIs: translates the PQL row-level logic into aggregated SQL

PQL ↔ SQL Translation Rules (derived from kpi_definitions.csv PQL Logic column):
  Table references:
    "Delivery DIM_csv"."Col"                 → dd."Col"   (Delivery_Dim)
    "Sales Order DIM_csv"."Col"              → so."Col"   (Sales Order DIM)
    "Warehouse DIM_csv"."Col"                → wd."Col"   (Warehouse DIM)
    "Supply_Chain_KPI_Tuned_5500_csv"."Col"  → sk."Col"   (Supply_Chain_KPI_Tuned)
    "Supply_Chain_KPI_Single_Sheet_5500_csv" → ss."Col"   (Supply_Chain_KPI_Single_Sheet)
    "_ARIS.Case"."Col"                       → sk."Col"   (Supply_Chain_KPI_Tuned)

  Functions:
    NULL_DATE                                → NULL
    NULL_TEXT                                → NULL
    TIME_BETWEEN(a, b)                       → EXTRACT(EPOCH FROM (b - a)) * 1000
    CT(col)                                  → COUNT(col)
    CTD(col)                                 → COUNT(DISTINCT col)
    MED(col)                                 → PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)
    TO_DATE(col)                             → col::DATE
    QUERY(expr)                              → (SELECT expr FROM …)
    SUM, AVG                                 → SQL SUM, AVG (direct)

ARIS Dashboard Ground Truth — pre-aggregated in 'cases' table:
  KPI name (exact)          | ARIS value
  ────────────────────────────────────────
  'Delivery on time %'      | 52.5%
  'Avg. Delay days'         | 105d
  'Open orders'             | 4.0k
  'Orders At Risk'          | 77.1%
  'Delay Order Value(EUR)'  | 18.0m
  'Packing accuracy'        | 95%
  'Savings lost (EUR)'      | 3.2m
  'Utilization Efficiency %'| 60.4%
  '# Materials'             | 2.6k
  '# Total Operators'       | 370
  'Value at Risk (EUR)'     | 24m
  'Warehouse Issue'         | 1.5k
  '#Warehouse'              | 40

Computed KPIs (not in cases table — translated via PQL):
  OTIF %                    | ~24.8% (median of Delivery Compliance %)
  Predictive OTIF %         | ~24.8% (same proxy; ARIS ML probabilities unavailable)
  # Shipment Affected       | ~4,048 (from Supply_Chain_KPI_Single_Sheet)
  InFull                    | computed
  Delay Risk                | computed
  Transport Delay           | computed
  Stock Shortage            | computed
  Risk Ratio                | computed
  Check Predictive OTIF     | computed
"""

import json
import csv
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from db.query_runner import QueryRunner

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

KPI_DEFINITIONS_PATH = Path(__file__).parent.parent / "data" / "kpi_definitions.csv"
KPI_GRAPH_PATH       = Path(__file__).parent.parent / "data" / "kpi_dependency_graph.json"

# ============================================================================
# CASES TABLE — pre-aggregated ARIS KPI names (exact strings)
# ============================================================================

# Maps from our internal KPI names → exact cases.name values
CASES_TABLE_MAP: Dict[str, str] = {
    "Delivery on time %":    "Delivery on time %",
    "On Time":               "Delivery on time %",
    "On Time Probability":   "Delivery on time %",
    "Avg. Delay days":       "Avg. Delay days",
    "Avg Delay Duration":    "Avg. Delay days",
    "Avg Days Delay Duration":"Avg. Delay days",
    "Open Orders":           "Open orders",         # lowercase 'o' in cases table
    "Orders at Risk":        "Orders At Risk",
    "Delay Order Value(EUR)":"Delay Order Value(EUR)",
    "Packing accuracy":      "Packing accuracy",
    "Savings lost (EUR)":    "Savings lost (EUR)",
    "Utilization Efficiency %": "Utilization Efficiency %",
    "# Materials":           "# Materials",
    "# Total Operators":     "# Total Operators",
    "Value at Risk (EUR)":   "Value at Risk (EUR)",
    "Warehouse Issue":       "Warehouse Issue",
    "#Warehouse":            "#Warehouse",
}

# ============================================================================
# PQL → SQL TRANSLATOR
# ============================================================================

# Table name mapping: PQL table reference → (actual SQL table name, alias)
PQL_TABLE_MAP: Dict[str, Tuple[str, str]] = {
    "Delivery DIM_csv":                         ("Delivery_Dim",                    "dd"),
    "Sales Order DIM_csv":                      ("Sales Order DIM",                 "so"),
    "Warehouse DIM_csv":                        ("Warehouse DIM",                   "wd"),
    "Supply_Chain_KPI_Tuned_5500_csv":          ("Supply_Chain_KPI_Tuned",          "sk"),
    "Supply_Chain_KPI_Single_Sheet_5500_csv":   ("Supply_Chain_KPI_Single_Sheet",   "ss"),
    "_ARIS.Case":                               ("Supply_Chain_KPI_Tuned",          "sk"),
}

# Standard JOIN clauses between tables (all on "Case ID")
JOIN_CLAUSES: Dict[str, str] = {
    "Delivery_Dim":                  'LEFT JOIN "Delivery_Dim" dd ON {base}."Case ID" = dd."Case ID"',
    "Sales Order DIM":               'LEFT JOIN "Sales Order DIM" so ON {base}."Case ID" = so."Case ID"',
    "Warehouse DIM":                 'LEFT JOIN "Warehouse DIM" wd ON {base}."Case ID" = wd."Case ID"',
    "Supply_Chain_KPI_Single_Sheet": 'LEFT JOIN "Supply_Chain_KPI_Single_Sheet" ss ON {base}."Case ID" = ss."Case ID"',
    "Delivery_Dim_dd":               'LEFT JOIN "Delivery_Dim" dd ON {base}."Case ID" = dd."Case ID"',
}

# Priority order for choosing the base (FROM) table
BASE_TABLE_PRIORITY = [
    "Supply_Chain_KPI_Tuned",
    "Supply_Chain_KPI_Single_Sheet",
    "Delivery_Dim",
    "Sales Order DIM",
    "Warehouse DIM",
]


class PQLTranslator:
    """
    Stage 2: PQL → PostgreSQL SQL translator.

    Translates PQL row-level logic (from kpi_definitions.csv) to SQL CASE expressions,
    then wraps them in the requested aggregation (SUM / AVG / COUNT / PERCENTILE_CONT).

    Usage:
        translator = PQLTranslator()
        # Row-level translation
        sql_expr = translator.translate_expr(pql_logic)
        # Full SELECT statement
        sql = translator.build_select(pql_logic, aggregation, source_tables, filters)
    """

    def translate_expr(self, pql: str) -> str:
        """
        Translate a PQL expression/row-level logic to a SQL expression.

        Applies in order:
          1. Table alias substitutions (e.g. "Delivery DIM_csv". → dd.)
          2. NULL sentinel replacement
          3. TIME_BETWEEN function
          4. CT / CTD / MED / TO_DATE function translations
          5. QUERY() wrapper removal (becomes inline)
        """
        sql = pql

        # ── 1. Table alias substitutions ──────────────────────────────────
        for pql_table, (_, alias) in PQL_TABLE_MAP.items():
            # Match  "TableName"."Column"  →  alias."Column"
            sql = sql.replace(f'"{pql_table}".', f'{alias}.')
            # Match  "TableName"  (standalone reference without column)
            sql = sql.replace(f'"{pql_table}"', f'"{alias}"')

        # ── 2. NULL sentinels ─────────────────────────────────────────────
        sql = sql.replace("NULL_DATE", "NULL")
        sql = sql.replace("NULL_TEXT", "NULL")

        # ── 3. TIME_BETWEEN(a, b) → EXTRACT(EPOCH FROM (b - a)) * 1000 ──
        sql = re.sub(
            r'TIME_BETWEEN\("([^"]+)",\s*"([^"]+)"\)',
            r'EXTRACT(EPOCH FROM ("\2" - "\1")) * 1000',
            sql
        )

        # ── 4. Function translations ──────────────────────────────────────
        # CT(col) → COUNT(col)
        sql = re.sub(r'\bCT\(', 'COUNT(', sql)

        # CTD(col) → COUNT(DISTINCT col)
        sql = re.sub(r'\bCTD\(', 'COUNT(DISTINCT ', sql)

        # MED(col) → PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)
        # Pattern: MED("col") → PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "col")
        def _med_to_sql(m):
            inner = m.group(1)
            return f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {inner})'
        sql = re.sub(r'\bMED\(([^)]+)\)', _med_to_sql, sql)

        # TO_DATE("col") → "col"::DATE
        sql = re.sub(r'\bTO_DATE\("([^"]+)"\)', r'"\1"::DATE', sql)

        # QUERY(expr) → expr (inline; QUERY is a PQL aggregation wrapper)
        sql = re.sub(r'\bQUERY\((.+)\)', r'\1', sql, flags=re.DOTALL)

        return sql.strip()

    def detect_source_tables(self, pql: str) -> List[str]:
        """
        Detect which source tables are referenced in a PQL expression.
        Returns the list of actual SQL table names.
        """
        tables: List[str] = []
        for pql_table, (sql_table, _) in PQL_TABLE_MAP.items():
            if f'"{pql_table}"' in pql and sql_table not in tables:
                tables.append(sql_table)
        return tables

    def build_from_clause(self, source_tables: List[str]) -> Tuple[str, str]:
        """
        Build FROM + JOIN clause for the given source tables.
        Returns (from_sql_string, base_alias).
        """
        if not source_tables:
            return 'FROM "Supply_Chain_KPI_Tuned" sk', "sk"

        # Pick base table by priority
        base_table = None
        for candidate in BASE_TABLE_PRIORITY:
            if candidate in source_tables:
                base_table = candidate
                break
        if base_table is None:
            base_table = source_tables[0]

        # Alias for base table
        alias_map = {sql_t: alias for _, (sql_t, alias) in PQL_TABLE_MAP.items()}
        base_alias = alias_map.get(base_table, "t")

        from_parts = [f'FROM "{base_table}" {base_alias}']

        for table in source_tables:
            if table == base_table:
                continue
            if table == "Delivery_Dim":
                from_parts.append(
                    f'LEFT JOIN "Delivery_Dim" dd ON {base_alias}."Case ID" = dd."Case ID"'
                )
            elif table == "Sales Order DIM":
                from_parts.append(
                    f'LEFT JOIN "Sales Order DIM" so ON {base_alias}."Case ID" = so."Case ID"'
                )
            elif table == "Warehouse DIM":
                from_parts.append(
                    f'LEFT JOIN "Warehouse DIM" wd ON {base_alias}."Case ID" = wd."Case ID"'
                )
            elif table == "Supply_Chain_KPI_Tuned":
                from_parts.append(
                    f'LEFT JOIN "Supply_Chain_KPI_Tuned" sk ON {base_alias}."Case ID" = sk."Case ID"'
                )
            elif table == "Supply_Chain_KPI_Single_Sheet":
                from_parts.append(
                    f'LEFT JOIN "Supply_Chain_KPI_Single_Sheet" ss ON {base_alias}."Case ID" = ss."Case ID"'
                )

        return "\n".join(from_parts), base_alias

    def build_select(
        self,
        pql_logic:    str,
        aggregation:  str,          # SUM | AVG | COUNT | MEDIAN | RATE | NONE
        source_tables: List[str],
        period_cond:  str  = "",
        city_cond:    str  = "",
        label:        str  = "result",
    ) -> str:
        """
        Build a complete SELECT statement from PQL row-level logic.

        aggregation values:
          SUM    → SUM(CASE WHEN ... THEN 1 ELSE 0 END)
          AVG    → AVG(CASE WHEN ... THEN value END)
          COUNT  → COUNT(col)  — when pql_logic is a simple column ref
          MEDIAN → PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)
          RATE   → SUM(flag) / COUNT(*) * 100
          NONE   → raw expression (no outer aggregation)
        """
        sql_expr    = self.translate_expr(pql_logic)
        from_clause, base_alias = self.build_from_clause(source_tables)

        # Build SELECT expression based on aggregation type
        if aggregation == "SUM":
            select_expr = f"SUM({sql_expr}) AS {label}"
        elif aggregation == "AVG":
            select_expr = f"CAST(AVG({sql_expr}) AS NUMERIC(10,2)) AS {label}"
        elif aggregation == "COUNT":
            select_expr = f"COUNT({sql_expr}) AS {label}"
        elif aggregation == "MEDIAN":
            select_expr = (
                f"CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {sql_expr}) "
                f"AS NUMERIC(10,2)) AS {label}"
            )
        elif aggregation == "RATE":
            # Assumes pql_logic is a 0/1 CASE expression
            select_expr = (
                f"CAST(100.0 * SUM({sql_expr}) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS {label},\n"
                f"  COUNT(*) AS total_orders"
            )
        else:  # NONE
            select_expr = f"{sql_expr} AS {label}"

        # Build WHERE clause
        where_parts = []
        if period_cond:
            where_parts.append(period_cond)
        if city_cond:
            where_parts.append(city_cond)

        parts = ["SELECT", f"  {select_expr}", from_clause]
        if where_parts:
            parts.append("WHERE " + "\n  AND ".join(where_parts))
        parts.append("LIMIT 1000")

        return "\n".join(parts)


# ============================================================================
# KPI DEFINITIONS LOADER
# ============================================================================

class KPIDefinitions:
    """Loads KPI definitions (including PQL Logic) from kpi_definitions.csv."""

    def __init__(self, csv_path: Path):
        self.definitions: Dict[str, Dict] = {}
        self._load(csv_path)

    def _load(self, csv_path: Path):
        if not csv_path.exists():
            logger.warning("KPI definitions CSV not found: %s", csv_path)
            return
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get('KPI Name') or '').strip()
                if name:
                    self.definitions[name] = {
                        'name':          name,
                        'definition':    row.get('Definition', ''),
                        'source_column': row.get('Source Column', ''),
                        'pql_logic':     row.get('PQL Logic', ''),
                        'purpose':       row.get('Purpose / Meaning', ''),
                    }
        logger.info("Loaded %d KPI definitions (with PQL Logic)", len(self.definitions))

    def get(self, name: str) -> Optional[Dict]:
        return self.definitions.get(name)

    def get_pql(self, name: str) -> Optional[str]:
        d = self.definitions.get(name)
        return d['pql_logic'] if d else None

    def all_names(self) -> List[str]:
        return list(self.definitions.keys())


# ============================================================================
# KPI DEPENDENCY GRAPH LOADER
# ============================================================================

class KPIDependencyGraph:
    """Loads the KPI dependency graph for source-table lookups."""

    def __init__(self, graph_path: Path):
        self.nodes: Dict = {}
        self._load(graph_path)

    def _load(self, graph_path: Path):
        if not graph_path.exists():
            logger.warning("KPI dependency graph not found: %s", graph_path)
            return
        with open(graph_path, 'r') as f:
            data = json.load(f)
        for node in data.get('graph', {}).get('nodes', []):
            self.nodes[node['id']] = node
        logger.info("Loaded dependency graph: %d nodes", len(self.nodes))

    def get_source_tables(self, node_id: str) -> List[str]:
        node = self.nodes.get(node_id, {})
        return node.get('source_tables', [])

    def get_node_by_label(self, label: str) -> Optional[Dict]:
        for node in self.nodes.values():
            if node.get('label', '').lower() == label.lower():
                return node
        return None


# ============================================================================
# KEYWORD → KPI NAME MAPPING
# ============================================================================

KEYWORD_KPI_MAP: Dict[str, str] = {
    # Predictive OTIF (longer phrase first)
    "predictive otif %":              "Predictive OTIF %",
    "predictive otif":                "Predictive OTIF %",
    "predicted otif":                 "Predictive OTIF %",

    # OTIF
    "on time in full":                "OTIF",
    "on-time in-full":                "OTIF",
    "on time and in full":            "OTIF",
    "otif":                           "OTIF",

    # Delivery on time
    "delivery on time percentage":    "Delivery on time %",
    "delivery on time %":             "Delivery on time %",
    "delivery on time rate":          "Delivery on time %",
    "on time probability":            "On Time Probability",
    "on time percentage":             "Delivery on time %",
    "ontime percentage":              "Delivery on time %",
    "on time delivery":               "Delivery on time %",
    "delivery on time":               "Delivery on time %",
    "on time":                        "On Time",
    "ontime":                         "On Time",

    # In-Full
    "in full":                        "InFull",
    "infull":                         "InFull",
    "delivered in full":              "InFull",
    "in-full":                        "InFull",

    # Delay
    "avg. delay days":                "Avg. Delay days",
    "average delay days":             "Avg. Delay days",
    "avg delay days":                 "Avg. Delay days",
    "delay days":                     "Avg. Delay days",
    "avg delay duration":             "Avg Delay Duration",
    "average delay duration":         "Avg Delay Duration",
    "avg delay":                      "Avg Delay Duration",
    "average delay":                  "Avg Delay Duration",
    "delay duration":                 "Avg Delay Duration",
    "delay":                          "Avg Delay Duration",

    # Delay Risk
    "delay risk":                     "Delay Risk",

    # Orders
    "orders at risk %":               "Orders at Risk",
    "orders at risk":                 "Orders at Risk",
    "at risk orders":                 "Orders at Risk",
    "at risk":                        "Orders at Risk",
    "open orders":                    "Open Orders",
    "pending orders":                 "Open Orders",

    # Warehouse (specific first)
    "warehouse issue":                "Warehouse Issue",
    "warehouse problem":              "Warehouse Issue",
    "stock shortage":                 "Stock Shortage in Warehouse",
    "inventory shortage":             "Stock Shortage in Warehouse",
    "utilization efficiency %":       "Utilization Efficiency %",
    "utilization efficiency":         "Utilization Efficiency %",
    "warehouse utilization":          "Utilization Efficiency %",
    "warehouse capacity":             "Utilization Efficiency %",
    "number of warehouses":           "#Warehouse",
    "count of warehouses":            "#Warehouse",
    "total warehouses":               "#Warehouse",
    "operational warehouses":         "#Warehouse",
    "# warehouse":                    "#Warehouse",
    "# warehouses":                   "#Warehouse",
    "warehouse count":                "#Warehouse",
    "warehouse":                      "#Warehouse",

    # Financial
    "delay order value":              "Delay Order Value(EUR)",
    "delay value":                    "Delay Order Value(EUR)",
    "value at risk":                  "Value at Risk (EUR)",
    "savings lost":                   "Savings lost (EUR)",
    "savings":                        "Savings lost (EUR)",

    # Shipment
    "# shipment affected":            "# Shipment Affected",
    "shipment affected":              "# Shipment Affected",
    "shipments affected":             "# Shipment Affected",
    "affected shipments":             "# Shipment Affected",
    "shipments were affected":        "# Shipment Affected",
    "shipments":                      "# Shipment Affected",

    # Materials
    "# materials":                    "# Materials",
    "number of materials":            "# Materials",
    "materials":                      "# Materials",
    "material count":                 "# Materials",

    # Operators
    "# total operators":              "# Total Operators",
    "total operators":                "# Total Operators",
    "operators":                      "# Total Operators",

    # Transport
    "transport delay":                "Transport Delay",
    "route disruption":               "Transport Delay",
    "route disruptions":              "Transport Delay",

    # Packing
    "packing accuracy":               "Packing accuracy",
    "packing":                        "Packing accuracy",

    # Risk
    "risk ratio":                     "Risk Ratio",
    "check predictive otif":          "Check Predictive OTIF",

    # City
    "by city":                        "City",
    "per city":                       "City",
    "city":                           "City",
    "source city":                    "City",
}

# ============================================================================
# KPI METADATA — aggregation type, PQL logic override, and ARIS reference
# For KPIs where PQL from the CSV needs augmentation or a custom aggregation.
# ============================================================================

KPI_METADATA: Dict[str, Dict] = {
    # Pre-aggregated in cases table — use cases table (no PQL translation needed)
    "Delivery on time %":     {"tier": "cases", "aris": "52.5%"},
    "On Time":                {"tier": "cases", "aris": "52.5%",  "cases_name": "Delivery on time %"},
    "On Time Probability":    {"tier": "cases", "aris": "52.5%",  "cases_name": "Delivery on time %"},
    "Avg. Delay days":        {"tier": "cases", "aris": "105d"},
    "Avg Delay Duration":     {"tier": "cases", "aris": "105d",   "cases_name": "Avg. Delay days"},
    "Avg Days Delay Duration":{"tier": "cases", "aris": "105d",   "cases_name": "Avg. Delay days"},
    "Open Orders":            {"tier": "cases", "aris": "4.0k",   "cases_name": "Open orders"},
    "Orders at Risk":         {"tier": "cases", "aris": "77.1%",  "cases_name": "Orders At Risk"},
    "Delay Order Value(EUR)": {"tier": "cases", "aris": "18.0m"},
    "Packing accuracy":       {"tier": "cases", "aris": "95%"},
    "Savings lost (EUR)":     {"tier": "cases", "aris": "3.2m"},
    "Utilization Efficiency %":{"tier":"cases", "aris": "60.4%"},
    "# Materials":            {"tier": "cases", "aris": "2.6k"},
    "# Total Operators":      {"tier": "cases", "aris": "370"},
    "Value at Risk (EUR)":    {"tier": "cases", "aris": "24m"},
    "Warehouse Issue":        {"tier": "cases", "aris": "1.5k"},
    "#Warehouse":             {"tier": "cases", "aris": "40"},

    # Computed via PQL translation
    "OTIF": {
        "tier": "computed",
        "aris": "25.2%",
        # ARIS stores per-case OTIF as "Delivery Compliance (%)"; median ≈ 25.2%
        "pql_override": 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sk."Delivery Compliance (%)") AS otif_pct, AVG(sk."Delivery Compliance (%)") AS otif_avg, COUNT(*) AS total_orders',
        "agg": "NONE",
        "from": 'FROM "Supply_Chain_KPI_Tuned" sk\nWHERE sk."Delivery Compliance (%)" IS NOT NULL',
    },
    "Predictive OTIF %": {
        "tier": "computed",
        "aris": "25.5%",
        # Same proxy as OTIF — ARIS ML probabilities are not in our tables
        "pql_override": 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sk."Delivery Compliance (%)") AS predictive_otif_pct, COUNT(*) AS total_orders',
        "agg": "NONE",
        "from": 'FROM "Supply_Chain_KPI_Tuned" sk\nWHERE sk."Delivery Compliance (%)" IS NOT NULL',
    },
    "InFull": {
        "tier": "computed",
        "aris": "N/A",
        "agg": "RATE",
        "pql": 'CASE WHEN so."Delivered Quantity" >= so."Order Quantity" THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Sales Order DIM"],
        "label": "in_full_pct",
    },
    "# Shipment Affected": {
        "tier": "computed",
        "aris": "~4,048",
        "agg": "SUM",
        "pql": 'CASE WHEN ss."Shipment Affected" = 1 THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Single_Sheet"],
        "label": "shipment_affected_count",
    },
    "Delay Risk": {
        "tier": "computed",
        "aris": "N/A",
        "agg": "SUM",
        "pql": 'CASE WHEN dd."Delivery_Date" > so."Requested_Delivery_Date" AND so."Delivered Quantity" < so."Order Quantity" THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Delivery_Dim", "Sales Order DIM"],
        "label": "delay_risk_count",
    },
    "Transport Delay": {
        "tier": "computed",
        "aris": "N/A",
        "agg": "SUM",
        "pql": 'CASE WHEN sk."Delivery Impacted by Route Disruptions" = \'Yes\' THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned"],
        "label": "transport_delay_count",
    },
    "Stock Shortage in Warehouse": {
        "tier": "computed",
        "aris": "N/A",
        "agg": "SUM",
        "pql": 'CASE WHEN wd."QUANTITY" < so."Order Quantity" THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Sales Order DIM", "Warehouse DIM"],
        "label": "stock_shortage_count",
    },
    "Risk Ratio": {
        "tier": "computed",
        "aris": "N/A",
        "agg": "AVG",
        "pql": '(CASE WHEN dd."Delivery_Date" > so."Requested_Delivery_Date" AND so."Delivered Quantity" < so."Order Quantity" THEN 1.0 ELSE 0 END + CASE WHEN wd."QUANTITY" < so."Order Quantity" THEN 1.0 ELSE 0 END + CASE WHEN sk."Delivery Impacted by Route Disruptions" = \'Yes\' THEN 1.0 ELSE 0 END) / 3.0',
        "tables": ["Supply_Chain_KPI_Tuned", "Delivery_Dim", "Sales Order DIM", "Warehouse DIM"],
        "label": "avg_risk_ratio",
    },
    "Check Predictive OTIF": {
        "tier": "computed",
        "aris": "N/A",
        "agg": "SUM",
        "pql": 'CASE WHEN ((CASE WHEN dd."Delivery_Date" > so."Requested_Delivery_Date" AND so."Delivered Quantity" < so."Order Quantity" THEN 1.0 ELSE 0 END + CASE WHEN wd."QUANTITY" < so."Order Quantity" THEN 1.0 ELSE 0 END + CASE WHEN sk."Delivery Impacted by Route Disruptions" = \'Yes\' THEN 1.0 ELSE 0 END) / 3.0) > 0.4 THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Delivery_Dim", "Sales Order DIM", "Warehouse DIM"],
        "label": "high_risk_orders",
    },
    "City": {
        "tier": "computed",
        "aris": "N/A",
        "pql_override": 'dd."Source City" AS city, COUNT(*) AS order_count, CAST(AVG(sk."Delivery Compliance (%)") AS NUMERIC(10,2)) AS avg_otif_pct, CAST(AVG(sk."Utilization Efficiency (%)") AS NUMERIC(10,2)) AS avg_utilization_pct',
        "agg": "NONE",
        "from": 'FROM "Supply_Chain_KPI_Tuned" sk\nLEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"\nWHERE dd."Source City" IS NOT NULL\nGROUP BY dd."Source City"\nORDER BY order_count DESC\nLIMIT 20',
    },
}

# ============================================================================
# DATE / PERIOD FILTER BUILDER
# ============================================================================

def _build_period_filter(period: str, base_alias: str = "sk") -> str:
    """Convert period string to a SQL WHERE condition on Warehouse_Record_Date."""
    if not period:
        return ""

    col = f'{base_alias}."Warehouse_Record_Date"'
    period = str(period).strip().lower()

    # Bare year: '2024'
    if re.fullmatch(r'20\d{2}', period):
        return f"{col} LIKE '{period}%'"

    # Year-month: '2024-03'
    if re.fullmatch(r'20\d{2}-\d{2}', period):
        return f"{col} LIKE '{period}%'"

    # Quarter: 'q1' … 'q4'
    q_match = re.fullmatch(r'q([1-4])', period)
    if q_match:
        q = int(q_match.group(1))
        ranges = {1: ("01", "03"), 2: ("04", "06"), 3: ("07", "09"), 4: ("10", "12")}
        ms, me = ranges[q]
        from datetime import date as dt
        yr = dt.today().year
        return f"{col} >= '{yr}-{ms}-01' AND {col} <= '{yr}-{me}-31'"

    from datetime import date as dt, timedelta
    today = dt.today()

    if period == "this_year":
        return f"{col} LIKE '{today.year}%'"
    if period == "this_month":
        return f"{col} LIKE '{today.year}-{today.month:02d}%'"
    if period == "last_month":
        first = today.replace(day=1)
        lm = first - timedelta(days=1)
        return f"{col} LIKE '{lm.year}-{lm.month:02d}%'"
    if period == "last_7_days":
        return f"{col} >= '{(today - timedelta(days=7)).isoformat()}'"
    if period == "last_30_days":
        return f"{col} >= '{(today - timedelta(days=30)).isoformat()}'"
    if period == "last_90_days":
        return f"{col} >= '{(today - timedelta(days=90)).isoformat()}'"

    return ""


def _build_city_filter(cities: List[str]) -> str:
    if not cities:
        return ""
    quoted = ", ".join(f"'{c}'" for c in cities)
    return f'dd."Source City" IN ({quoted})'


def _add_filters_to_sql(sql: str, period_cond: str, city_cond: str) -> str:
    """Append WHERE / AND conditions to an existing SQL string."""
    conditions = [c for c in [period_cond, city_cond] if c]
    if not conditions:
        return sql
    combined = " AND ".join(conditions)
    sql_stripped = sql.rstrip().rstrip(";")
    if re.search(r'\bWHERE\b', sql_stripped, re.IGNORECASE):
        return sql_stripped + f"\n  AND {combined}"
    return sql_stripped + f"\nWHERE {combined}"


# ============================================================================
# MAIN SQL AGENT
# ============================================================================

class SQLAgent:
    """
    Two-Stage SQL Agent: NL → PQL → SQL

    Stage 1: identify_kpi(query, intent) → kpi_name (from KEYWORD_KPI_MAP)
    Stage 2: generate_sql(kpi_name, ...)  → verified PostgreSQL query

    For cases-table KPIs: routes to `SELECT name, value FROM cases WHERE name = '...'`
    For computed KPIs:    translates PQL from KPI_METADATA using PQLTranslator
    """

    def __init__(self, db: QueryRunner, max_iterations: int = 3):
        self.db             = db
        self.max_iterations = max_iterations
        self.kpi_defs       = KPIDefinitions(KPI_DEFINITIONS_PATH)
        self.kpi_graph      = KPIDependencyGraph(KPI_GRAPH_PATH)
        self.pql            = PQLTranslator()
        logger.info("SQLAgent initialised — two-stage NL → PQL → SQL pipeline")

    # ── Stage 1: Natural Language → KPI name ────────────────────────────────

    @staticmethod
    def _normalize(text: str) -> str:
        return text.lower().strip()

    def identify_kpi(self, query: str, intent: Dict) -> Tuple[Optional[str], float]:
        """
        Stage 1: Map natural language → KPI name.

        Step A: Keyword match (longest phrase wins).
        Step B: Intent classifier metric fallback (confidence ≥ 0.5).
        """
        q_lower = self._normalize(query)

        # Step A: keyword match
        for keyword in sorted(KEYWORD_KPI_MAP.keys(), key=len, reverse=True):
            if keyword in q_lower:
                kpi_name = KEYWORD_KPI_MAP[keyword]
                logger.debug("KW match: '%s' → '%s'", keyword, kpi_name)
                return kpi_name, 1.0

        # Step B: intent classifier fallback
        metric = intent.get('metric')
        if metric:
            phrase = self._normalize(metric).replace('_', ' ')
            for keyword in sorted(KEYWORD_KPI_MAP.keys(), key=len, reverse=True):
                if keyword in phrase or phrase in keyword:
                    conf = intent.get('confidence', 0.5)
                    if conf >= 0.5:
                        kpi_name = KEYWORD_KPI_MAP[keyword]
                        logger.debug("Intent fallback: '%s' → '%s'", metric, kpi_name)
                        return kpi_name, conf

        logger.debug("KPI not found for query: '%s'", query)
        return None, 0.0

    # ── Stage 2: PQL → SQL ──────────────────────────────────────────────────

    def _generate_cases_query(self, cases_name: str) -> str:
        """Route to the cases table (Tier 1 — exact ARIS values)."""
        safe = cases_name.replace("'", "''")
        return (
            f"-- PQL Tier 1: pre-aggregated ARIS value from cases table\n"
            f"SELECT\n"
            f"  name  AS kpi_name,\n"
            f"  value AS kpi_value\n"
            f"FROM cases\n"
            f"WHERE name = '{safe}'\n"
            f"LIMIT 1"
        )

    def _generate_computed_query(
        self,
        kpi_name:    str,
        meta:        Dict,
        period_cond: str,
        city_cond:   str,
    ) -> str:
        """
        Route to Tier 2 — translate PQL to SQL for computed KPIs.

        Uses KPI_METADATA for the PQL expression, aggregation type,
        and source tables.  Applies date/city filters where applicable.
        """
        # Special case: full custom FROM clause provided
        if "pql_override" in meta and "from" in meta:
            sql = f"-- PQL Tier 2: computed KPI — {kpi_name}\nSELECT\n  {meta['pql_override']}\n{meta['from']}"
            return _add_filters_to_sql(sql, period_cond, city_cond)

        # Standard PQL translation
        pql_expr   = meta.get("pql") or self.kpi_defs.get_pql(kpi_name) or ""
        agg        = meta.get("agg", "SUM")
        tables     = meta.get("tables", [])
        label      = meta.get("label", "result")

        if not pql_expr:
            raise RuntimeError(
                f"No PQL logic found for KPI '{kpi_name}'. "
                f"Add it to KPI_METADATA or kpi_definitions.csv."
            )

        # Translate PQL and build SELECT
        sql_expr            = self.pql.translate_expr(pql_expr)
        from_clause, b_alias = self.pql.build_from_clause(tables)

        if agg == "SUM":
            select_expr = f"SUM({sql_expr}) AS {label},\n  COUNT(*) AS total_orders"
        elif agg == "AVG":
            select_expr = f"CAST(AVG({sql_expr}) AS NUMERIC(10,4)) AS {label},\n  COUNT(*) AS total_orders"
        elif agg == "RATE":
            select_expr = (
                f"CAST(100.0 * SUM({sql_expr}) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS {label},\n"
                f"  COUNT(*) AS total_orders"
            )
        elif agg == "MEDIAN":
            select_expr = f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {sql_expr}) AS {label}"
        else:  # NONE
            select_expr = f"{sql_expr} AS {label}"

        parts = [
            f"-- PQL Tier 2: computed KPI — {kpi_name}",
            "SELECT",
            f"  {select_expr}",
            from_clause,
        ]

        where_parts = [p for p in [period_cond, city_cond] if p]
        if where_parts:
            parts.append("WHERE " + "\n  AND ".join(where_parts))

        return "\n".join(parts)

    # ── Public API ──────────────────────────────────────────────────────────

    def generate(self, intent: Dict, original_query: str) -> str:
        """
        Full two-stage pipeline: NL → KPI → (PQL) → SQL.

        Returns a PostgreSQL SELECT statement ready for execution.

        Raises:
            ValueError   if KPI cannot be identified.
            RuntimeError if no SQL can be generated for the KPI.
        """
        logger.info("SQLAgent.generate | query: '%s'", original_query)

        # ── Stage 1: NL → KPI ────────────────────────────────────────────
        kpi_name, confidence = self.identify_kpi(original_query, intent)

        if not kpi_name:
            supported = sorted(set(KEYWORD_KPI_MAP.values()))
            raise ValueError(
                f"Cannot identify KPI from: '{original_query}'.\n"
                f"Supported KPIs: {', '.join(supported)}"
            )
        logger.info("KPI: '%s' (conf=%.2f)", kpi_name, confidence)

        # ── Resolve canonical name (aliases) ─────────────────────────────
        meta = KPI_METADATA.get(kpi_name)
        if meta is None:
            # Try to find via CASES_TABLE_MAP
            if kpi_name in CASES_TABLE_MAP:
                cases_name = CASES_TABLE_MAP[kpi_name]
                meta = {"tier": "cases"}
            else:
                raise RuntimeError(
                    f"KPI '{kpi_name}' not found in KPI_METADATA. "
                    f"Add it to KPI_METADATA in sql_agent.py."
                )

        # ── Build filter conditions ───────────────────────────────────────
        period     = intent.get('period')
        cities     = intent.get('cities') or intent.get('filters', {}).get('cities', [])
        period_cond = _build_period_filter(period) if period else ""
        city_cond   = _build_city_filter(cities)   if cities  else ""

        # ── Stage 2: PQL → SQL ────────────────────────────────────────────
        tier = meta.get("tier", "computed")

        if tier == "cases":
            cases_name = meta.get("cases_name") or CASES_TABLE_MAP.get(kpi_name) or kpi_name
            sql = self._generate_cases_query(cases_name)
            # Note: cases table filters are not applicable (pre-aggregated)
            if period_cond:
                sql += f"\n-- Note: period filter '{period}' not applied — value is pre-aggregated"
        else:
            sql = self._generate_computed_query(kpi_name, meta, period_cond, city_cond)

        logger.info("Generated SQL:\n%s", sql)
        return sql

    def refine(self, failed_sql: str, error: str, intent: Dict, original_query: str) -> str:
        """
        Attempt to auto-fix a SQL execution error.
        Most errors with this pipeline indicate a metadata issue — log clearly.
        """
        logger.warning("SQL failed: %s\nSQL:\n%s", error, failed_sql)
        error_lower = error.lower()

        if "column" in error_lower and "does not exist" in error_lower:
            col_m = re.search(r'column "([^"]+)" does not exist', error)
            hint  = f" ('{col_m.group(1)}')" if col_m else ""
            raise RuntimeError(
                f"Column{hint} not found. Check KPI_METADATA pql / tables in sql_agent.py. "
                f"Error: {error}"
            )
        if "relation" in error_lower and "does not exist" in error_lower:
            tbl_m = re.search(r'relation "([^"]+)" does not exist', error)
            hint  = f" (table '{tbl_m.group(1)}')" if tbl_m else ""
            raise RuntimeError(
                f"Table{hint} not found. Verify Supabase table names. Error: {error}"
            )
        raise RuntimeError(f"SQL Agent cannot auto-fix: {error}")
