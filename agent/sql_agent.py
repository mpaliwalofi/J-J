"""
SQL Agent - Strict KPI-Driven SQL Generation

This module generates SQL queries STRICTLY from KPI definitions in CSV.
NO LLM-based SQL generation. NO guessing. NO approximations.

All SQL logic comes from data/kpi_definitions.csv and kpi_dependency_graph.json.

FIXES APPLIED (2026-04-13):
  1. KEYWORD_KPI_MAP: removed phantom 'Utilization Efficiency' (no such KPI);
     mapped to correct cases-table name 'Utilization Efficiency %'.
  2. CASES_MAPPING: fixed 'otif' -> 'Delivery on time %' was semantically wrong;
     'otif' now routes to the OTIF computed KPI, not the on-time % pre-agg value.
     All 13 cases-table KPI names are now covered.
  3. _generate_cases_query: was querying non-existent columns 'kpi_name'/'kpi_value';
     fixed to use actual schema columns 'name'/'value'.
  4. sql_logic in graph for On Time / Delay Risk / OTIF / Avg Delay Duration:
     was referencing alias 'sk' (Supply_Chain_KPI_Tuned alias) instead of the
     correct Delivery_Dim (dd) and Sales Order DIM (so) aliases.
  5. _build_from_clause: 'cases' source table was not handled; now supported.
  6. pql_to_sql: '_ARIS.Case' was replaced with alias 'sk' (wrong table);
     for computed KPIs the correct base comes from Delivery_Dim + Sales Order DIM.
"""

import json
import csv
import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from db.query_runner import QueryRunner

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

KPI_DEFINITIONS_PATH = Path(__file__).parent.parent / "data" / "kpi_definitions.csv"
KPI_GRAPH_PATH = Path(__file__).parent.parent / "data" / "kpi_dependency_graph.json"

# ============================================================================
# CASES TABLE KPI NAMES (exact strings as stored in cases.name column)
# ============================================================================

CASES_TABLE_KPI_NAMES: set = {
    "Delivery on time %",
    "Avg. Delay days",
    "# Materials",
    "Orders At Risk",
    "Delay Order Value(EUR)",
    "Packing accuracy",
    "Warehouse Issue",
    "# Shipment Affected",
    "Value at Risk (EUR)",
    "Savings lost (EUR)",
    "Utilization Efficiency %",
    "#Warehouse",
    "# Total Operators",
}

# ============================================================================
# KEYWORD-TO-KPI MAPPING (Step 1: Keyword Match)
# Priority: longer / more specific phrases must appear BEFORE shorter ones.
# ============================================================================

KEYWORD_KPI_MAP = {
    # ── Cases-table KPIs (pre-aggregated ARIS values) ──────────────────────
    "utilization efficiency":        "Utilization Efficiency %",   # FIX: was 'Utilization Efficiency' (didn't exist)
    "utilization efficiency %":      "Utilization Efficiency %",
    "packing accuracy":              "Packing accuracy",
    "value at risk":                 "Value at Risk (EUR)",
    "savings lost":                  "Savings lost (EUR)",
    "delay order value":             "Delay Order Value(EUR)",
    "# materials":                   "# Materials",
    "number of materials":           "# Materials",
    "materials":                     "# Materials",
    "# shipment affected":           "# Shipment Affected",
    "shipment affected":             "# Shipment Affected",
    "number of warehouses":          "#Warehouse",
    "# warehouse":                   "#Warehouse",
    "total operators":               "# Total Operators",
    "# total operators":             "# Total Operators",

    # ── Computed KPIs (derived from dimensional tables) ────────────────────
    # Warehouse
    "warehouse issue":               "Warehouse Issue",
    "warehouse problem":             "Warehouse Issue",
    "warehouse":                     "Warehouse Issue",

    # OTIF  — NOTE: 'otif' maps to the COMPUTED OTIF KPI (On-Time AND In-Full),
    #         NOT to 'Delivery on time %' (which is on-time only, pre-aggregated).
    "otif":                          "OTIF",
    "on time in full":               "OTIF",
    "on-time in-full":               "OTIF",

    # On Time (computed flag per order)
    "delivery on time percentage":   "On Time Probability",
    "ontime percentage":             "On Time Probability",
    "on time percentage":            "On Time Probability",
    "on time probability":           "On Time Probability",
    "delivery on time":              "On Time",
    "on time":                       "On Time",
    "ontime":                        "On Time",

    # Delivery on time % (pre-aggregated cases table value)
    "delivery on time %":            "Delivery on time %",

    # Delay
    "avg delay duration":            "Avg Delay Duration",
    "average delay duration":        "Avg Delay Duration",
    "avg delay":                     "Avg Delay Duration",
    "average delay":                 "Avg Delay Duration",
    "delay duration":                "Avg Delay Duration",
    "avg. delay days":               "Avg. Delay days",
    "average delay days":            "Avg. Delay days",
    "delay days":                    "Avg. Delay days",
    "delay":                         "Avg Delay Duration",

    # Orders at Risk (computed)
    "orders at risk":                "Orders at Risk",
    "at risk":                       "Orders at Risk",
    "risk orders":                   "Orders at Risk",

    # Orders At Risk (cases table pre-agg)
    "orders at risk %":              "Orders At Risk",

    # Open Orders
    "open orders":                   "Open Orders",
    "pending orders":                "Open Orders",

    # InFull
    "in full":                       "InFull",
    "infull":                        "InFull",
    "delivered in full":             "InFull",

    # Predictive OTIF
    "predictive otif":               "Predictive OTIF %",
    "predicted otif":                "Predictive OTIF %",

    # Transport Delay
    "transport delay":               "Transport Delay",
    "route disruption":              "Transport Delay",

    # Stock Shortage
    "stock shortage":                "Stock Shortage in Warehouse",
    "inventory shortage":            "Stock Shortage in Warehouse",

    # Delay Risk
    "delay risk":                    "Delay Risk",
}

# ============================================================================
# DATE COLUMN MAPPING PER TABLE ALIAS
# ============================================================================

DATE_COLUMN_MAP = {
    'sk':  '"Delivery Date"',        # Supply_Chain_KPI_Tuned
    'dd':  '"Delivery_Date"',        # Delivery_Dim
    'so':  '"Requested_Delivery_Date"',  # Sales Order DIM
    'wd':  '"Warehouse_Record_Date"',    # Warehouse DIM
    'ss':  '"Warehouse_Record_Date"',    # Supply_Chain_KPI_Single_Sheet
    't':   '"Warehouse_Record_Date"',    # fallback
}

# ============================================================================
# LOAD KPI DEFINITIONS FROM CSV
# ============================================================================

class KPIDefinitions:
    """Loads and manages KPI definitions from CSV."""

    def __init__(self, csv_path: Path):
        self.definitions = {}
        self.load_from_csv(csv_path)

    def load_from_csv(self, csv_path: Path):
        if not csv_path.exists():
            logger.error(f"KPI definitions CSV not found: {csv_path}")
            return

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                kpi_name = row['KPI Name'].strip()
                self.definitions[kpi_name] = {
                    'name': kpi_name,
                    'definition': row['Definition'],
                    'source_column': row['Source Column'],
                    'pql_logic': row['PQL Logic'],
                    'purpose': row['Purpose / Meaning']
                }

        logger.info(f"Loaded {len(self.definitions)} KPI definitions from CSV")

    def get(self, kpi_name: str) -> Optional[Dict]:
        return self.definitions.get(kpi_name)

    def exists(self, kpi_name: str) -> bool:
        return kpi_name in self.definitions

    def all_names(self) -> List[str]:
        return list(self.definitions.keys())


# ============================================================================
# LOAD KPI DEPENDENCY GRAPH
# ============================================================================

class KPIDependencyGraph:
    """Manages KPI dependencies and layering."""

    def __init__(self, graph_path: Path):
        self.graph = {}
        self.layers = {}
        self.adjacency = {}
        self.load_from_json(graph_path)

    def load_from_json(self, graph_path: Path):
        if not graph_path.exists():
            logger.error(f"KPI dependency graph not found: {graph_path}")
            return

        with open(graph_path, 'r') as f:
            data = json.load(f)
            self.graph = data.get('graph', {})
            self.layers = data.get('layers', {})
            self.adjacency = data.get('adjacency_list', {})

        logger.info(f"Loaded dependency graph with {len(self.graph)} KPIs")

    def get_dependencies(self, kpi_name: str) -> List[str]:
        kpi_info = self.graph.get(kpi_name, {})
        return kpi_info.get('depends_on', [])

    def get_sql_logic(self, kpi_name: str) -> Optional[str]:
        kpi_info = self.graph.get(kpi_name, {})
        return kpi_info.get('sql_logic')

    def get_source_tables(self, kpi_name: str) -> List[str]:
        kpi_info = self.graph.get(kpi_name, {})
        return kpi_info.get('source_tables', [])

    def is_cases_kpi(self, kpi_name: str) -> bool:
        """Return True if this KPI is served directly from the cases table."""
        tables = self.get_source_tables(kpi_name)
        return tables == ['cases']


# ============================================================================
# SQL GENERATOR - DETERMINISTIC, NO LLM
# ============================================================================

class DeterministicSQLGenerator:
    """Generates SQL strictly from KPI definitions — no LLM involved."""

    def __init__(self, kpi_defs: KPIDefinitions, kpi_graph: KPIDependencyGraph):
        self.kpi_defs = kpi_defs
        self.kpi_graph = kpi_graph

    def pql_to_sql(self, pql_logic: str, kpi_name: str) -> str:
        """
        Convert PQL logic to SQL, replacing ARIS-specific syntax and aliases.

        FIX: '_ARIS.Case' was previously replaced with alias 'sk' which is the
        Supply_Chain_KPI_Tuned alias. Computed KPIs that depend on delivery dates
        use dd (Delivery_Dim) and so (Sales Order DIM) — their sql_logic in the
        dependency graph is now correct, so we prefer that over PQL translation.
        """
        sql = pql_logic

        # Table alias substitutions
        sql = sql.replace('"_ARIS.Case".', '')        # strip ARIS case prefix; columns resolved via graph sql_logic
        sql = sql.replace('"_ARIS.Case"', '')
        sql = sql.replace('"Delivery DIM_csv".', 'dd.')
        sql = sql.replace('"Sales Order DIM_csv".', 'so.')
        sql = sql.replace('"Warehouse DIM_csv".', 'wd.')
        sql = sql.replace('"Supply_Chain_KPI_Tuned_5500_csv".', 'sk.')
        sql = sql.replace('"Supply_Chain_KPI_Single_Sheet_5500_csv".', 'ss.')

        # ARIS null sentinel replacement
        sql = sql.replace('NULL_DATE', 'NULL')
        sql = sql.replace('NULL_TEXT', 'NULL')

        # ARIS TIME_BETWEEN -> PostgreSQL interval expression
        # TIME_BETWEEN(a, b) = b - a in milliseconds
        import re
        sql = re.sub(
            r'TIME_BETWEEN\("([^"]+)",\s*"([^"]+)"\)',
            r'EXTRACT(EPOCH FROM ("\2" - "\1")) * 1000',
            sql
        )

        return sql

    def determine_aggregation(self, query: str, kpi_name: str) -> str:
        query_lower = query.lower()

        if any(w in query_lower for w in ['how many', 'count', 'number of', 'total']):
            return 'SUM'
        if any(w in query_lower for w in ['percentage', 'rate', '%', 'percent', 'probability']):
            return 'AVG'
        if any(w in query_lower for w in ['average', 'mean', 'avg']):
            return 'AVG'

        kpi_def = self.kpi_defs.get(kpi_name)
        if kpi_def:
            pql = kpi_def['pql_logic']
            if 'THEN 1 ELSE 0 END' in pql:
                return 'SUM'
            if any(w in pql for w in ['QUERY(', 'AVG(', 'SUM(', 'COUNT(']):
                return 'NONE'

        return 'AVG'

    def build_sql(self, kpi_name: str, query: str, filters: Dict = None) -> str:
        """
        Build SQL for a KPI.  Always prefers sql_logic from the dependency graph
        (which has already been corrected) over PQL translation.
        """
        # Prefer graph sql_logic (already correct)
        sql_logic = self.kpi_graph.get_sql_logic(kpi_name)

        if not sql_logic:
            # Fallback: translate PQL
            kpi_def = self.kpi_defs.get(kpi_name)
            if not kpi_def:
                raise ValueError(f"KPI '{kpi_name}' not found in definitions or graph")
            sql_logic = self.pql_to_sql(kpi_def['pql_logic'], kpi_name)

        source_tables = self.kpi_graph.get_source_tables(kpi_name)

        # Cases-table KPIs are returned as-is (they are complete SELECT statements)
        if source_tables == ['cases']:
            return sql_logic

        agg_type = self.determine_aggregation(query, kpi_name)

        if agg_type == 'NONE':
            select_expr = sql_logic
        elif agg_type == 'SUM':
            select_expr = f"SUM({sql_logic})"
        else:
            select_expr = f"AVG({sql_logic})"

        from_clause, base_alias = self._build_from_clause(source_tables)
        where_clause = self._build_where_clause(filters, base_alias, source_tables)

        sql_parts = ["SELECT", f"  {select_expr} AS result", from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append("LIMIT 100")

        return "\n".join(sql_parts)

    def _build_from_clause(self, source_tables: List[str]) -> Tuple[str, str]:
        """Build FROM + JOIN clause.  Returns (sql_string, base_alias)."""

        # FIX: 'cases' table is handled separately — should not reach here
        # but guard just in case
        if not source_tables or source_tables == ['cases']:
            return 'FROM "Supply_Chain_KPI_Tuned" sk', 'sk'

        # Choose primary (base) table by priority
        if 'Delivery_Dim' in source_tables:
            base = '"Delivery_Dim" dd'
            base_alias = 'dd'
            base_name = 'Delivery_Dim'
        elif 'Supply_Chain_KPI_Tuned' in source_tables:
            base = '"Supply_Chain_KPI_Tuned" sk'
            base_alias = 'sk'
            base_name = 'Supply_Chain_KPI_Tuned'
        elif 'Sales Order DIM' in source_tables:
            base = '"Sales Order DIM" so'
            base_alias = 'so'
            base_name = 'Sales Order DIM'
        elif 'Warehouse DIM' in source_tables:
            base = '"Warehouse DIM" wd'
            base_alias = 'wd'
            base_name = 'Warehouse DIM'
        else:
            base = f'"{source_tables[0]}"'
            base_alias = 't'
            base_name = source_tables[0]

        from_parts = [f"FROM {base}"]

        for table in source_tables:
            if table == base_name:
                continue
            if table == 'Delivery_Dim':
                from_parts.append(
                    f'LEFT JOIN "Delivery_Dim" dd ON {base_alias}."Case ID" = dd."Case ID"'
                )
            elif table == 'Sales Order DIM':
                from_parts.append(
                    f'LEFT JOIN "Sales Order DIM" so ON {base_alias}."Case ID" = so."Case ID"'
                )
            elif table == 'Warehouse DIM':
                from_parts.append(
                    f'LEFT JOIN "Warehouse DIM" wd ON {base_alias}."Case ID" = wd."Case ID"'
                )
            elif table == 'Supply_Chain_KPI_Tuned':
                from_parts.append(
                    f'LEFT JOIN "Supply_Chain_KPI_Tuned" sk ON {base_alias}."Case ID" = sk."Case ID"'
                )
            elif table == 'Supply_Chain_KPI_Single_Sheet':
                from_parts.append(
                    f'LEFT JOIN "Supply_Chain_KPI_Single_Sheet" ss ON {base_alias}."Case ID" = ss."Case ID"'
                )

        return "\n".join(from_parts), base_alias

    def _build_where_clause(self, filters: Dict, base_alias: str, source_tables: List[str]) -> str:
        if not filters:
            return ""

        where_parts = []

        if filters.get('period'):
            date_col = DATE_COLUMN_MAP.get(base_alias, '"Warehouse_Record_Date"')
            where_parts.append(f'{base_alias}.{date_col} LIKE \'{filters["period"]}%\'')

        if filters.get('cities'):
            if 'Delivery_Dim' in source_tables:
                cities = "', '".join(filters['cities'])
                where_parts.append(f'dd."Source City" IN (\'{cities}\')')

        if filters.get('regions'):
            regions = "', '".join(filters['regions'])
            where_parts.append(f'{base_alias}."Region of the Country" IN (\'{regions}\')')

        return ("WHERE " + " AND ".join(where_parts)) if where_parts else ""


# ============================================================================
# MAIN SQL AGENT CLASS
# ============================================================================

class SQLAgent:
    """
    SQL Agent - Strict KPI-Driven SQL Generation

    Flow:
      1. Check if the metric maps to a cases-table KPI (exact pre-agg value).
      2. Keyword match against KEYWORD_KPI_MAP.
      3. Intent classifier fallback (only when confidence > 0.6).
      4. Validate KPI exists in definitions/graph.
      5. Generate SQL from dependency graph sql_logic (preferred) or PQL translation.
    """

    def __init__(self, db: QueryRunner, max_iterations: int = 3):
        self.db = db
        self.max_iterations = max_iterations
        self.kpi_defs = KPIDefinitions(KPI_DEFINITIONS_PATH)
        self.kpi_graph = KPIDependencyGraph(KPI_GRAPH_PATH)
        self.sql_gen = DeterministicSQLGenerator(self.kpi_defs, self.kpi_graph)
        logger.info("SQLAgent initialized with strict KPI-driven mode")

    # ── KPI identification ──────────────────────────────────────────────────

    def _normalize(self, text: str) -> str:
        return text.lower().strip()

    def _cases_kpi_for_metric(self, metric: str) -> Optional[str]:
        """
        Check whether a raw metric string (from the intent classifier) resolves
        to a cases-table pre-aggregated KPI.

        FIX: previously this mapping was incomplete and mapped 'otif' to
        'Delivery on time %' which is semantically wrong — OTIF requires both
        on-time AND in-full, whereas 'Delivery on time %' is on-time only.
        """
        CASES_METRIC_MAP: Dict[str, str] = {
            # intent classifier metric strings -> exact cases.name values
            "delivery_on_time":          "Delivery on time %",
            "delivery on time":          "Delivery on time %",
            "delivery on time %":        "Delivery on time %",
            "avg_delay":                 "Avg. Delay days",
            "avg_delay_duration":        "Avg. Delay days",
            "avg. delay days":           "Avg. Delay days",
            "delay days":                "Avg. Delay days",
            "materials":                 "# Materials",
            "# materials":               "# Materials",
            "orders_at_risk":            "Orders At Risk",
            "orders at risk %":          "Orders At Risk",
            "delay_order_value":         "Delay Order Value(EUR)",
            "delay order value":         "Delay Order Value(EUR)",
            "packing_accuracy":          "Packing accuracy",
            "packing accuracy":          "Packing accuracy",
            "warehouse_issue":           "Warehouse Issue",
            "shipment_affected":         "# Shipment Affected",
            "# shipment affected":       "# Shipment Affected",
            "value_at_risk":             "Value at Risk (EUR)",
            "value at risk":             "Value at Risk (EUR)",
            "savings_lost":              "Savings lost (EUR)",
            "savings lost":              "Savings lost (EUR)",
            "utilization_efficiency":    "Utilization Efficiency %",
            "utilization efficiency":    "Utilization Efficiency %",
            "utilization efficiency %":  "Utilization Efficiency %",
            "warehouse":                 "#Warehouse",
            "#warehouse":                "#Warehouse",
            "total_operators":           "# Total Operators",
            "# total operators":         "# Total Operators",
        }
        return CASES_METRIC_MAP.get(self._normalize(metric))

    def identify_kpi(self, query: str, intent: Dict) -> Tuple[Optional[str], float]:
        """
        2-step KPI identification.

        Step 1: Keyword match (longest match wins — iterate sorted by length desc).
        Step 2: Intent classifier metric (only if confidence > 0.6).
        """
        query_lower = self._normalize(query)

        # Step 1: keyword match — sort by descending key length to prefer longer phrases
        for keyword in sorted(KEYWORD_KPI_MAP.keys(), key=len, reverse=True):
            if keyword in query_lower:
                kpi_name = KEYWORD_KPI_MAP[keyword]
                print(f"[DEBUG] Keyword match: '{keyword}' -> KPI: '{kpi_name}'")
                return kpi_name, 1.0

        # Step 2: intent classifier
        if intent.get('metric') and intent.get('confidence', 0) > 0.6:
            metric = intent['metric']
            metric_title = metric.replace('_', ' ').title()
            if self.kpi_defs.exists(metric_title):
                print(f"[DEBUG] Intent match: '{metric}' -> KPI: '{metric_title}'")
                return metric_title, intent['confidence']

        print(f"[DEBUG] KPI NOT FOUND for query: '{query}'")
        return None, 0.0

    # ── SQL generation ──────────────────────────────────────────────────────

    def generate(self, intent: Dict, original_query: str) -> str:
        print(f"[DEBUG] Query: {original_query}")
        print(f"[DEBUG] Intent: {intent}")

        # Priority: check if intent metric maps to a cases-table pre-agg KPI
        if intent.get('metric'):
            cases_kpi = self._cases_kpi_for_metric(intent['metric'])
            if cases_kpi:
                print(f"[DEBUG] Using cases table for '{intent['metric']}' -> '{cases_kpi}'")
                return self._generate_cases_query(cases_kpi)

        # Standard KPI identification
        kpi_name, confidence = self.identify_kpi(original_query, intent)

        if not kpi_name:
            available = self.kpi_defs.all_names()
            raise ValueError(
                f"KPI NOT FOUND for query: '{original_query}'. "
                f"Available KPIs: {', '.join(available[:10])}..."
            )

        if not self.kpi_defs.exists(kpi_name) and kpi_name not in self.kpi_graph.graph:
            raise ValueError(f"KPI '{kpi_name}' not found in definitions or dependency graph")

        print(f"[DEBUG] KPI: {kpi_name}")

        # If this KPI is a cases-table KPI, route directly
        if self.kpi_graph.is_cases_kpi(kpi_name):
            print(f"[DEBUG] KPI '{kpi_name}' resolved to cases table — routing directly")
            return self._generate_cases_query(kpi_name)

        deps = self.kpi_graph.get_dependencies(kpi_name)
        if deps:
            print(f"[DEBUG] Dependencies: {', '.join(deps)}")

        filters = {
            'period':  intent.get('period'),
            'cities':  intent.get('cities', []),
            'regions': intent.get('regions', [])
        }

        sql = self.sql_gen.build_sql(kpi_name, original_query, filters)
        print(f"[DEBUG] SQL: {sql}")
        return sql

    def _generate_cases_query(self, kpi_name: str) -> str:
        """
        Generate a query against the cases table.

        FIX: original code queried columns 'kpi_name' and 'kpi_value' which do
        not exist.  The actual schema is: id, created_at, name, value.
        """
        # Escape single quotes in kpi_name for safety
        safe_name = kpi_name.replace("'", "''")
        return (
            f"SELECT\n"
            f"    name AS kpi_name,\n"
            f"    value AS kpi_value,\n"
            f"    created_at\n"
            f"FROM cases\n"
            f"WHERE name = '{safe_name}'\n"
            f"ORDER BY created_at DESC\n"
            f"LIMIT 1"
        )

    # ── SQL refinement (error recovery) ────────────────────────────────────

    def refine(self, failed_sql: str, error: str, intent: Dict, original_query: str) -> str:
        """Attempt to fix common SQL errors without LLM involvement."""
        print(f"[DEBUG] SQL execution failed: {error}")
        print(f"[DEBUG] Failed SQL: {failed_sql}")

        error_lower = error.lower()

        if "column" in error_lower and "does not exist" in error_lower:
            fixed = failed_sql.replace('"."', '.')
            print("[DEBUG] Attempting fix: removing incorrect quoting")
            return fixed

        if "relation" in error_lower and "does not exist" in error_lower:
            raise RuntimeError(f"Table not found. SQL cannot be fixed automatically. Error: {error}")

        if "missing from-clause" in error_lower:
            raise RuntimeError(
                f"Alias not found in FROM clause. "
                f"This usually means the sql_logic in kpi_dependency_graph.json "
                f"references a table alias that is not joined. Error: {error}"
            )

        raise RuntimeError(f"SQL Agent cannot fix this error: {error}")