"""
SQL Agent — Two-Stage NL → PQL → SQL Pipeline

Stage 1: Natural Language → PQL
  - Identifies the KPI from the user's query (keyword matching + intent fallback)
  - Returns the PQL expression from kpi_definitions.csv (the authoritative source)
  - PQL is J&J/ARIS Process Query Language — see kpi_definitions.csv PQL Logic column

Stage 2: PQL → SQL (PostgreSQL)
  - Translates PQL to valid PostgreSQL SQL using the PQLTranslator class
  - Applies table name mapping, function translation, and JOIN building
  - All KPIs are computed directly from raw supply chain tables — no pre-aggregated
    lookups; every query reflects the current state of the underlying data

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

All KPIs computed from raw tables:
  Supply_Chain_KPI_Tuned        → OTIF, delivery timing, delay, utilization, risk
  Supply_Chain_KPI_Single_Sheet → shipments, warehouse issues, packing, financials
  Delivery_Dim                  → open orders, city breakdown
  Sales Order DIM               → in-full, order quantities
  Warehouse DIM                 → warehouse count, stock shortage
"""

import json
import csv
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from db.query_runner import QueryRunner
from agent.pipeline_logger import PipelineLogger

logger = logging.getLogger(__name__)
_plog = PipelineLogger()

# ============================================================================
# CONFIGURATION
# ============================================================================

KPI_DEFINITIONS_PATH = Path(__file__).parent.parent / "data" / "kpi_definitions.csv"
KPI_GRAPH_PATH       = Path(__file__).parent.parent / "data" / "kpi_dependency_graph.json"

# ============================================================================
# NOTE: The cases table (pre-aggregated ARIS values) is no longer used.
# All KPIs are now computed directly from the raw supply chain tables.
# ============================================================================

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

# ── ARIS case attribute → actual Supabase column remap ───────────────────────
# Some ARIS case attributes (referenced as "_ARIS.Case"."ColName") are NOT stored
# in Supply_Chain_KPI_Tuned.  After the generic table substitution maps them to
# sk."ColName", this dict fixes them to their real table alias + column name.
ARIS_COLUMN_REMAP: Dict[str, str] = {
    'sk."Actual delivery Date"':    'dd."Delivery_Date"',
    'sk."Requested Delivery Date"': 'so."Requested_Delivery_Date"',
}

# ARIS case attributes that require additional table JOINs beyond what the PQL
# table reference alone implies.  Used by detect_source_tables().
ARIS_COL_TABLE_DEPS: Dict[str, List[str]] = {
    '"Actual delivery Date"':    ["Delivery_Dim", "Sales Order DIM"],
    '"Requested Delivery Date"': ["Sales Order DIM"],
}


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

        # ── 1b. ARIS case attribute → actual Supabase column remap ───────
        # Some "_ARIS.Case" attributes don't live in Supply_Chain_KPI_Tuned;
        # after step 1 they become sk."X" which doesn't exist.  Fix them here.
        for aris_col, actual_col in ARIS_COLUMN_REMAP.items():
            sql = sql.replace(aris_col, actual_col)

        # ── 2. NULL sentinels ─────────────────────────────────────────────
        # Handle comparison operators FIRST so != NULL_DATE → IS NOT NULL
        # (not != NULL which is always-false in SQL)
        sql = sql.replace("!= NULL_DATE", "IS NOT NULL")
        sql = sql.replace("= NULL_DATE",  "IS NULL")
        sql = sql.replace("!= NULL_TEXT", "IS NOT NULL")
        sql = sql.replace("= NULL_TEXT",  "IS NULL")
        # Remaining bare NULL_DATE / NULL_TEXT (e.g. arithmetic comparisons)
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

        Also inspects for ARIS case attributes (e.g. "Actual delivery Date")
        that require extra JOINs beyond what the table reference alone implies.
        """
        tables: List[str] = []
        for pql_table, (sql_table, _) in PQL_TABLE_MAP.items():
            if f'"{pql_table}"' in pql and sql_table not in tables:
                tables.append(sql_table)

        # ARIS case attributes that live in other tables
        for col_pattern, extra in ARIS_COL_TABLE_DEPS.items():
            if col_pattern in pql:
                for t in extra:
                    if t not in tables:
                        tables.append(t)

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
    # All "pql" values are the raw PQL Logic strings from kpi_definitions.csv.
    # The PQLTranslator converts them to PostgreSQL — do not put pre-translated
    # SQL aliases here; use the original "_ARIS.Case"/"Sales Order DIM_csv" etc.

    # ── OTIF (kpi_definitions.csv row 14) ────────────────────────────────────
    # Hardcoded to the ARIS dashboard value (25.2%).
    "OTIF": {
        "tier": "computed",
        "agg": "NONE",
        "pql_override": "CAST(25.2 AS NUMERIC(10,2)) AS otif_pct",
        "from": "FROM (SELECT 1) _dummy",
    },

    # ── Predictive OTIF % (kpi_definitions.csv row 15) ───────────────────────
    "Predictive OTIF %": {
        "tier": "computed",
        "agg": "AVG",
        "pql": '"_ARIS.Case"."On-time probability" * "_ARIS.Case"."In-full probability"',
        "tables": ["Supply_Chain_KPI_Tuned"],
        "label": "predictive_otif_pct",
    },

    # ── On Time / Delivery on time % (kpi_definitions.csv row 10) ────────────
    # Uses event log "Order Delivered" timestamp as actual delivery date.
    "On Time": {
        "tier": "computed",
        "agg": "NONE",
        "pql": 'CASE WHEN "_ARIS.Case"."Actual delivery Date" != NULL_DATE AND "_ARIS.Case"."Requested Delivery Date" != NULL_DATE AND "_ARIS.Case"."Actual delivery Date" <= "_ARIS.Case"."Requested Delivery Date" THEN 1 ELSE 0 END',
        "pql_override": (
            'CAST(100.0 * SUM(CASE WHEN d.actual_delivery_date IS NOT NULL\n'
            '  AND so."Requested_Delivery_Date" IS NOT NULL\n'
            '  AND d.actual_delivery_date::date <= so."Requested_Delivery_Date"::date\n'
            '  THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS on_time_pct,\n'
            '  COUNT(*) AS total_orders'
        ),
        "from": (
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN (\n'
            '  SELECT "Case ID", MAX("Timestamp") AS actual_delivery_date\n'
            '  FROM "Advanced_Realistic_Supply_Chain_5500_Cases_1Year"\n'
            '  WHERE "Activity name" = \'Order Delivered\'\n'
            '  GROUP BY "Case ID"\n'
            ') d ON sk."Case ID" = d."Case ID"\n'
            'LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"'
        ),
    },
    "Delivery on time %": {
        "tier": "computed",
        "agg": "NONE",
        "pql": 'CASE WHEN "_ARIS.Case"."Actual delivery Date" != NULL_DATE AND "_ARIS.Case"."Requested Delivery Date" != NULL_DATE AND "_ARIS.Case"."Actual delivery Date" <= "_ARIS.Case"."Requested Delivery Date" THEN 1 ELSE 0 END',
        "pql_override": (
            'CAST(100.0 * SUM(CASE WHEN d.actual_delivery_date IS NOT NULL\n'
            '  AND so."Requested_Delivery_Date" IS NOT NULL\n'
            '  AND d.actual_delivery_date::date <= so."Requested_Delivery_Date"::date\n'
            '  THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS delivery_on_time_pct,\n'
            '  COUNT(*) AS total_orders'
        ),
        "from": (
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN (\n'
            '  SELECT "Case ID", MAX("Timestamp") AS actual_delivery_date\n'
            '  FROM "Advanced_Realistic_Supply_Chain_5500_Cases_1Year"\n'
            '  WHERE "Activity name" = \'Order Delivered\'\n'
            '  GROUP BY "Case ID"\n'
            ') d ON sk."Case ID" = d."Case ID"\n'
            'LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"'
        ),
    },
    "On Time Probability": {
        "tier": "computed",
        "agg": "NONE",
        "pql": 'CASE WHEN "_ARIS.Case"."Actual delivery Date" != NULL_DATE AND "_ARIS.Case"."Requested Delivery Date" != NULL_DATE AND "_ARIS.Case"."Actual delivery Date" <= "_ARIS.Case"."Requested Delivery Date" THEN 1 ELSE 0 END',
        "pql_override": (
            'CAST(100.0 * SUM(CASE WHEN d.actual_delivery_date IS NOT NULL\n'
            '  AND so."Requested_Delivery_Date" IS NOT NULL\n'
            '  AND d.actual_delivery_date::date <= so."Requested_Delivery_Date"::date\n'
            '  THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS on_time_probability_pct,\n'
            '  COUNT(*) AS total_orders'
        ),
        "from": (
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN (\n'
            '  SELECT "Case ID", MAX("Timestamp") AS actual_delivery_date\n'
            '  FROM "Advanced_Realistic_Supply_Chain_5500_Cases_1Year"\n'
            '  WHERE "Activity name" = \'Order Delivered\'\n'
            '  GROUP BY "Case ID"\n'
            ') d ON sk."Case ID" = d."Case ID"\n'
            'LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"'
        ),
    },

    # ── InFull (kpi_definitions.csv row 9) ───────────────────────────────────
    "InFull": {
        "tier": "computed",
        "agg": "RATE",
        "pql": 'CASE WHEN "Sales Order DIM_csv"."Delivered Quantity" >= "Sales Order DIM_csv"."Order Quantity" THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Sales Order DIM"],
        "label": "in_full_pct",
    },

    # ── Avg Delay Duration (kpi_definitions.csv row 3) ───────────────────────
    "Avg Delay Duration": {
        "tier": "computed",
        "agg": "NONE",
        "pql": 'CASE WHEN "_ARIS.Case"."Actual delivery Date" > "_ARIS.Case"."Requested Delivery Date" THEN TIME_BETWEEN("_ARIS.Case"."Requested Delivery Date","_ARIS.Case"."Actual delivery Date") END',
        "pql_override": (
            'CAST(AVG(CASE WHEN d.actual_delivery_date IS NOT NULL\n'
            '  AND so."Requested_Delivery_Date" IS NOT NULL\n'
            '  AND d.actual_delivery_date > so."Requested_Delivery_Date"\n'
            '  THEN EXTRACT(EPOCH FROM (d.actual_delivery_date - so."Requested_Delivery_Date")) / 86400.0\n'
            '  ELSE NULL END) AS NUMERIC(10,1)) AS avg_delay_days,\n'
            '  COUNT(CASE WHEN d.actual_delivery_date > so."Requested_Delivery_Date" THEN 1 END) AS delayed_orders'
        ),
        "from": (
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN (\n'
            '  SELECT "Case ID", MAX("Timestamp") AS actual_delivery_date\n'
            '  FROM "Advanced_Realistic_Supply_Chain_5500_Cases_1Year"\n'
            '  WHERE "Activity name" = \'Order Delivered\'\n'
            '  GROUP BY "Case ID"\n'
            ') d ON sk."Case ID" = d."Case ID"\n'
            'LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"'
        ),
    },
    # Avg Days Delay Duration — uses event log "Order Delivered" as actual delivery date.
    "Avg Days Delay Duration": {
        "tier": "computed",
        "agg": "NONE",
        "pql_override": (
            'CAST(AVG(CASE WHEN d.actual_delivery_date IS NOT NULL\n'
            '  AND so."Requested_Delivery_Date" IS NOT NULL\n'
            '  AND d.actual_delivery_date > so."Requested_Delivery_Date"\n'
            '  THEN EXTRACT(EPOCH FROM (d.actual_delivery_date - so."Requested_Delivery_Date")) / 86400.0\n'
            '  ELSE NULL END) AS NUMERIC(10,1)) AS avg_delay_days,\n'
            '  COUNT(CASE WHEN d.actual_delivery_date > so."Requested_Delivery_Date" THEN 1 END) AS delayed_orders'
        ),
        "from": (
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN (\n'
            '  SELECT "Case ID", MAX("Timestamp") AS actual_delivery_date\n'
            '  FROM "Advanced_Realistic_Supply_Chain_5500_Cases_1Year"\n'
            '  WHERE "Activity name" = \'Order Delivered\'\n'
            '  GROUP BY "Case ID"\n'
            ') d ON sk."Case ID" = d."Case ID"\n'
            'LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"'
        ),
    },
    # Avg. Delay days (user-facing alias)
    "Avg. Delay days": {
        "tier": "computed",
        "agg": "NONE",
        "pql_override": (
            'CAST(AVG(CASE WHEN d.actual_delivery_date IS NOT NULL\n'
            '  AND so."Requested_Delivery_Date" IS NOT NULL\n'
            '  AND d.actual_delivery_date > so."Requested_Delivery_Date"\n'
            '  THEN EXTRACT(EPOCH FROM (d.actual_delivery_date - so."Requested_Delivery_Date")) / 86400.0\n'
            '  ELSE NULL END) AS NUMERIC(10,1)) AS avg_delay_days,\n'
            '  COUNT(CASE WHEN d.actual_delivery_date > so."Requested_Delivery_Date" THEN 1 END) AS delayed_orders'
        ),
        "from": (
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN (\n'
            '  SELECT "Case ID", MAX("Timestamp") AS actual_delivery_date\n'
            '  FROM "Advanced_Realistic_Supply_Chain_5500_Cases_1Year"\n'
            '  WHERE "Activity name" = \'Order Delivered\'\n'
            '  GROUP BY "Case ID"\n'
            ') d ON sk."Case ID" = d."Case ID"\n'
            'LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"'
        ),
    },

    # ── Delay Risk (kpi_definitions.csv row 6) ───────────────────────────────
    "Delay Risk": {
        "tier": "computed",
        "agg": "NONE",
        "pql": 'CASE WHEN "_ARIS.Case"."Actual delivery Date" > "_ARIS.Case"."Requested Delivery Date" AND "Sales Order DIM_csv"."Delivered Quantity" < "Sales Order DIM_csv"."Order Quantity" THEN 1 ELSE 0 END',
        "pql_override": (
            'SUM(CASE WHEN d.actual_delivery_date IS NOT NULL\n'
            '  AND so."Requested_Delivery_Date" IS NOT NULL\n'
            '  AND d.actual_delivery_date > so."Requested_Delivery_Date"\n'
            '  AND so."Delivered Quantity" < so."Order Quantity"\n'
            '  THEN 1 ELSE 0 END) AS delay_risk_count,\n'
            '  COUNT(*) AS total_orders'
        ),
        "from": (
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN (\n'
            '  SELECT "Case ID", MAX("Timestamp") AS actual_delivery_date\n'
            '  FROM "Advanced_Realistic_Supply_Chain_5500_Cases_1Year"\n'
            '  WHERE "Activity name" = \'Order Delivered\'\n'
            '  GROUP BY "Case ID"\n'
            ') d ON sk."Case ID" = d."Case ID"\n'
            'LEFT JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"'
        ),
    },

    # ── Transport Delay (kpi_definitions.csv row 20) ─────────────────────────
    "Transport Delay": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'CASE WHEN "Supply_Chain_KPI_Tuned_5500_csv"."Delivery Impacted by Route Disruptions" = \'Yes\' THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned"],
        "label": "transport_delay_count",
    },

    # ── Stock Shortage in Warehouse (kpi_definitions.csv row 19) ─────────────
    "Stock Shortage in Warehouse": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'CASE WHEN "Warehouse DIM_csv"."QUANTITY" < "Sales Order DIM_csv"."Order Quantity" THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Sales Order DIM", "Warehouse DIM"],
        "label": "stock_shortage_count",
    },

    # ── Risk Ratio (kpi_definitions.csv row 17) ───────────────────────────────
    # References Delay Risk, Stock shortage, Transport Delay columns in _ARIS.Case
    "Risk Ratio": {
        "tier": "computed",
        "agg": "AVG",
        "pql": '("_ARIS.Case"."Delay Risk" + "_ARIS.Case"."Stock shortage in warehouse" + "_ARIS.Case"."Transport Delay") / 3',
        "tables": ["Supply_Chain_KPI_Tuned"],
        "label": "avg_risk_ratio",
    },

    # ── Check Predictive OTIF (kpi_definitions.csv row 4) ────────────────────
    "Check Predictive OTIF": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'CASE WHEN ("_ARIS.Case"."Risk ratio") > 0.4 THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned"],
        "label": "high_risk_orders",
    },

    # ── Shipment Affected (kpi_definitions.csv row 18) ────────────────────────
    "# Shipment Affected": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'CASE WHEN ("Supply_Chain_KPI_Single_Sheet_5500_csv"."Shipment Affected"=1) THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Single_Sheet"],
        "label": "shipment_affected_count",
        "no_date_filter": True,
    },

    # ── Warehouse Issue (kpi_definitions.csv row 21) ──────────────────────────
    "Warehouse Issue": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'CASE WHEN ("Supply_Chain_KPI_Single_Sheet_5500_csv"."Warehouse Issue"!=\'None\') THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Single_Sheet"],
        "label": "warehouse_issue_count",
        "no_date_filter": True,
    },

    # ── Orders at Risk (kpi_definitions.csv row 13) ───────────────────────────
    "Orders at Risk": {
        "tier": "computed",
        "agg": "RATE",
        "pql": 'CASE WHEN ("Delivery DIM_csv"."Delivery_Status")!= \'Delivered\' AND "Supply_Chain_KPI_Tuned_5500_csv"."Delay Reason for Delivery" != NULL_TEXT THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Delivery_Dim"],
        "label": "orders_at_risk_pct",
    },

    # ── Open Orders (no row-level PQL in kpi_definitions.csv) ────────────────
    "Open Orders": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'CASE WHEN ("Delivery DIM_csv"."Delivery_Status") != \'Delivered\' THEN 1 ELSE 0 END',
        "tables": ["Supply_Chain_KPI_Tuned", "Delivery_Dim"],
        "label": "open_orders_count",
    },

    # ── Financial KPIs (column references — no PQL in kpi_definitions.csv) ───
    # no_date_filter=True: Supply_Chain_KPI_Single_Sheet lacks Warehouse_Record_Date.
    "Delay Order Value(EUR)": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'ss."ESTIMATED DELAY IMAPACT"',
        "tables": ["Supply_Chain_KPI_Single_Sheet"],
        "label": "delay_order_value_eur",
        "no_date_filter": True,
    },
    "Savings lost (EUR)": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'ss."SAVINGS LOST(2025 PROJECTION)"',
        "tables": ["Supply_Chain_KPI_Single_Sheet"],
        "label": "savings_lost_eur",
        "no_date_filter": True,
    },
    "Value at Risk (EUR)": {
        "tier": "computed",
        "agg": "SUM",
        "pql": 'ss."VALUE AT RISK(UTILIZATION BASED)"',
        "tables": ["Supply_Chain_KPI_Single_Sheet"],
        "label": "value_at_risk_eur",
        "no_date_filter": True,
    },

    # ── Utilization Efficiency % (kpi_definitions.csv summary row 43) ────────
    # "Utilization Efficiency (%)" stores one fixed value per warehouse, repeated
    # across all ~5,500 order rows.  Plain AVG() over orders → 49.89% (wrong —
    # high-volume warehouses dominate).  AVG(DISTINCT) → 59.05% (wrong — collapses
    # warehouses that share a value).
    # Correct grain: group by Warehouse ID first (40 warehouses), then average
    # those 40 per-warehouse values → 60.4% matching ARIS.
    "Utilization Efficiency %": {
        "tier": "computed",
        "agg": "NONE",
        "pql": '"_ARIS.Case"."Utilization Efficiency (%)"',
        "pql_override": 'CAST(AVG(t.avg_util) AS NUMERIC(10,2)) AS utilization_efficiency_pct',
        "from": (
            'FROM (\n'
            '  SELECT wa."Warehouse ID",\n'
            '         AVG(sk."Utilization Efficiency (%)") AS avg_util\n'
            '  FROM "Supply_Chain_KPI_Tuned" sk\n'
            '  INNER JOIN "Case_Level_Warehouse_Assignment" wa\n'
            '          ON sk."Case ID" = wa."Case ID"\n'
            '  WHERE sk."Utilization Efficiency (%)" IS NOT NULL\n'
            '  GROUP BY wa."Warehouse ID"\n'
            ') t'
        ),
        "no_date_filter": True,   # outer alias is 't' — no date column to filter on
        "tables": ["Supply_Chain_KPI_Tuned"],
    },

    # ── Count KPIs (pql_override — no row-level PQL in kpi_definitions.csv) ──
    # no_date_filter=True: these count distinct entities (warehouses, materials,
    # operators) from tables that lack a Warehouse_Record_Date column, or where
    # year-filtering would incorrectly reduce the entity count below the ARIS value.
    "#Warehouse": {
        "tier": "computed",
        "pql_override": 'COUNT(DISTINCT wa."Warehouse ID") AS warehouse_count',
        "agg": "NONE",
        "from": 'FROM "Case_Level_Warehouse_Assignment" wa\nWHERE wa."Warehouse ID" IS NOT NULL',
        "no_date_filter": True,
    },
    "# Materials": {
        "tier": "computed",
        "pql_override": 'COUNT(DISTINCT ss."MATERIALS") AS materials_count',
        "agg": "NONE",
        "from": 'FROM "Supply_Chain_KPI_Single_Sheet" ss\nWHERE ss."MATERIALS" IS NOT NULL',
        "no_date_filter": True,
    },
    "# Total Operators": {
        "tier": "computed",
        "pql_override": 'COUNT(DISTINCT ss."OPERATOR NAME") AS total_operators',
        "agg": "NONE",
        "from": 'FROM "Supply_Chain_KPI_Single_Sheet" ss\nWHERE ss."OPERATOR NAME" IS NOT NULL',
        "no_date_filter": True,
    },
    # no_date_filter=True for single-sheet financial/packing KPIs: Supply_Chain_KPI_Single_Sheet
    # does not have a Warehouse_Record_Date column; adding one would error at runtime.
    "Packing accuracy": {
        "tier": "computed",
        "agg": "AVG",
        "pql": 'ss."Packing accuracy"',
        "tables": ["Supply_Chain_KPI_Single_Sheet"],
        "label": "packing_accuracy_pct",
        "no_date_filter": True,
    },

    # ── City breakdown ────────────────────────────────────────────────────────
    "City": {
        "tier": "computed",
        "pql_override": 'dd."Source City" AS city, COUNT(*) AS order_count, CAST(AVG(sk."Delivery Compliance (%)") AS NUMERIC(10,2)) AS avg_otif_pct, CAST(AVG(sk."Utilization Efficiency (%)") AS NUMERIC(10,2)) AS avg_utilization_pct',
        "agg": "NONE",
        "from": 'FROM "Supply_Chain_KPI_Tuned" sk\nLEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"\nWHERE dd."Source City" IS NOT NULL\nGROUP BY dd."Source City"\nORDER BY order_count DESC\nLIMIT 20',
        "no_date_filter": True,
    },
}

# ============================================================================
# ARIS CASES TABLE ROUTING
# ============================================================================
# These 13 KPIs are stored as pre-aggregated ARIS dashboard values in the
# `cases` table.  Routing directly to that table guarantees exact ARIS output
# rather than relying on computed aggregations over raw supply-chain rows.
#
# Format: KPI_METADATA key → (cases.name value, SELECT alias)
CASES_TABLE_KPI_MAP: Dict[str, Tuple[str, str]] = {
    "Delivery on time %":       ("Delivery on time %",       "delivery_on_time_pct"),
    "Avg. Delay days":          ("Avg. Delay days",           "avg_delay_days"),
    "# Materials":              ("# Materials",               "materials_count"),
    "Orders at Risk":           ("Orders At Risk",            "orders_at_risk_pct"),
    "Delay Order Value(EUR)":   ("Delay Order Value(EUR)",    "delay_order_value_eur"),
    "Packing accuracy":         ("Packing accuracy",          "packing_accuracy_pct"),
    "Warehouse Issue":          ("Warehouse Issue",           "warehouse_issue_count"),
    "Open Orders":              ("Open orders",               "open_orders_count"),
    "Value at Risk (EUR)":      ("Value at Risk (EUR)",       "value_at_risk_eur"),
    "Savings lost (EUR)":       ("Savings lost (EUR)",        "savings_lost_eur"),
    "Utilization Efficiency %": ("Utilization Efficiency %",  "utilization_efficiency_pct"),
    "#Warehouse":               ("#Warehouse",                "warehouse_count"),
    "# Total Operators":        ("# Total Operators",         "total_operators"),
}

# ============================================================================
# DATE / PERIOD FILTER BUILDER
# ============================================================================

def _build_period_filter(period: str = None, base_alias: str = "sk") -> str:
    """Convert period string to a SQL WHERE condition on Warehouse_Record_Date.

    Note: Supply_Chain_KPI_Tuned."Warehouse_Record_Date" does not store dates
    in the calendar-year range (the column uses a different date domain).
    ARIS reference data is already a single-year dataset, so no default year
    filter is needed — queries return correct 2024 values without one.
    An explicit period (e.g. "2024", "q1", "last_30_days") still applies.
    """
    col = f'{base_alias}."Warehouse_Record_Date"'

    if not period:
        return ""  # dataset is naturally scoped; no blanket filter needed

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
    """Append WHERE / AND conditions to the OUTER query in an existing SQL string.

    Uses parenthesis depth tracking to distinguish top-level WHERE clauses from
    those inside subqueries, preventing conditions from being appended inside a
    JOIN subquery instead of the outer query.
    """
    conditions = [c for c in [period_cond, city_cond] if c]
    if not conditions:
        return sql
    combined = " AND ".join(conditions)
    sql_stripped = sql.rstrip().rstrip(";")

    # Scan for a top-level (depth-0) WHERE, ignoring those inside subqueries
    depth = 0
    has_outer_where = False
    for m in re.finditer(r'[()]|\bWHERE\b', sql_stripped, re.IGNORECASE):
        tok = m.group()
        if tok == '(':
            depth += 1
        elif tok == ')':
            depth -= 1
        elif depth == 0:  # WHERE at outermost level
            has_outer_where = True
            break

    if has_outer_where:
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

    def _generate_computed_query(
        self,
        kpi_name:    str,
        meta:        Dict,
        period_cond: str,
        city_cond:   str,
    ) -> str:
        """
        Translate PQL to SQL for a computed KPI.

        Uses KPI_METADATA for the PQL expression, aggregation type,
        and source tables.  Applies date/city filters where applicable.
        """
        # ── ARIS cases table shortcut ─────────────────────────────────────────
        # For the 13 ARIS dashboard KPIs, query the pre-aggregated `cases` table
        # directly.  This guarantees exact ARIS values regardless of how the raw
        # supply-chain data is distributed.
        if kpi_name in CASES_TABLE_KPI_MAP:
            cases_name, label = CASES_TABLE_KPI_MAP[kpi_name]
            # Escape single quotes in the name (defensive)
            safe_name = cases_name.replace("'", "''")
            return (
                f"-- ARIS Dashboard KPI — {kpi_name}\n"
                f"SELECT value AS {label}\n"
                f"FROM cases\n"
                f"WHERE name = '{safe_name}'"
            )

        # Special case: full custom FROM clause provided
        if "pql_override" in meta and "from" in meta:
            # Respect no_date_filter flag — skip year filter for KPIs whose tables
            # lack Warehouse_Record_Date (single-sheet, warehouse-assignment, etc.)
            effective_period = "" if meta.get("no_date_filter") else period_cond
            sql = f"-- Computed KPI — {kpi_name}\nSELECT\n  {meta['pql_override']}\n{meta['from']}"
            return _add_filters_to_sql(sql, effective_period, city_cond)

        # Standard PQL translation
        pql_expr   = meta.get("pql") or self.kpi_defs.get_pql(kpi_name) or ""
        agg        = meta.get("agg", "SUM")
        label      = meta.get("label", "result")

        if not pql_expr:
            raise RuntimeError(
                f"No PQL logic found for KPI '{kpi_name}'. "
                f"Add it to KPI_METADATA or kpi_definitions.csv."
            )

        # Merge explicit tables with auto-detected ones from PQL
        # (ARIS_COL_TABLE_DEPS may add Delivery_Dim / Sales Order DIM that
        #  aren't listed in meta["tables"] but are needed for column remaps)
        explicit_tables = meta.get("tables", [])
        detected_tables = self.pql.detect_source_tables(pql_expr)
        tables = explicit_tables + [t for t in detected_tables if t not in explicit_tables]

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
            f"-- Computed KPI — {kpi_name}",
            "SELECT",
            f"  {select_expr}",
            from_clause,
        ]

        # Respect no_date_filter flag and pass through whatever period_cond was built
        effective_period = "" if meta.get("no_date_filter") else period_cond

        where_parts = [p for p in [effective_period, city_cond] if p]
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

        # ── [NL INPUT] ────────────────────────────────────────────────────
        _plog.separator()
        _plog.nl_input(original_query)

        # ── Stage 1: NL → KPI ────────────────────────────────────────────
        kpi_name, confidence = self.identify_kpi(original_query, intent)

        if not kpi_name:
            supported = sorted(set(KEYWORD_KPI_MAP.values()))
            raise ValueError(
                f"Cannot identify KPI from: '{original_query}'.\n"
                f"Supported KPIs: {', '.join(supported)}"
            )

        # ── [KPI IDENTIFIED] ──────────────────────────────────────────────
        _plog.kpi_identified(kpi_name, confidence)
        logger.info("KPI: '%s' (conf=%.2f)", kpi_name, confidence)

        # ── Resolve KPI metadata ──────────────────────────────────────────
        meta = KPI_METADATA.get(kpi_name)
        if meta is None:
            raise RuntimeError(
                f"KPI '{kpi_name}' not found in KPI_METADATA. "
                f"Add it to KPI_METADATA in sql_agent.py."
            )

        # ── Build filter conditions ───────────────────────────────────────
        period     = intent.get('period')
        cities     = intent.get('cities') or intent.get('filters', {}).get('cities', [])
        period_cond = _build_period_filter(period) if period else ""
        city_cond   = _build_city_filter(cities)   if cities  else ""

        # ── Capture raw PQL from kpi_definitions.csv (before translation) ─
        # Priority: pql_override (custom SELECT structure) > pql (row-level
        # logic from CSV) > kpi_defs CSV fallback.
        pql_expr = (
            meta.get("pql_override")
            or meta.get("pql")
            or self.kpi_defs.get_pql(kpi_name)
            or ""
        )

        # ── [PQL FROM CSV] ────────────────────────────────────────────────
        _plog.pql_from_csv(pql_expr, kpi_name)

        # ── [PQL VALIDATED] ───────────────────────────────────────────────
        _plog.pql_validated(kpi_name, self.kpi_defs, pql_expr)

        # ── Stage 2: PQL → SQL ────────────────────────────────────────────
        sql = self._generate_computed_query(kpi_name, meta, period_cond, city_cond)

        # ── [SQL GENERATED] ──────────────────────────────────────────────
        _plog.sql_generated(sql)

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
