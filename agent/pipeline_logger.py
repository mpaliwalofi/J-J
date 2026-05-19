"""
Pipeline Logger — Structured terminal output for the NL → PQL → SQL → DB flow.

Stages printed in order for every query:

  [NL INPUT]       → raw user query
  [KPI IDENTIFIED] → matched KPI name and confidence
  [PQL FROM CSV]   → raw PQL Logic string from kpi_definitions.csv
  [PQL VALIDATED]  → PASS  or  WARN: <reason>
  [SQL GENERATED]  → translated PostgreSQL (first 200 chars)
  [DB HIT]         → Supabase PostgreSQL → <table(s)>
  [RESULT]         → N rows  (sample: {...})

Usage:
    from agent.pipeline_logger import PipelineLogger
    log = PipelineLogger()
    log.nl_input(query)
    log.kpi_identified(kpi_name, confidence)
    log.pql_from_csv(pql_expr, kpi_name)
    log.pql_validated(kpi_name, kpi_defs, pql_expr)
    log.sql_generated(sql)
    log.db_hit(sql)
    log.result(rows)
"""

import re
import logging
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from agent.sql_agent import KPIDefinitions

logger = logging.getLogger(__name__)

# Width of the label column — long enough for "[KPI IDENTIFIED]" (18 chars)
_LABEL_WIDTH = 18

# Known column names across all source tables — used for PQL field validation.
# These are the column names as they appear in kpi_definitions.csv PQL Logic
# (i.e. the raw names inside double-quotes, before table alias substitution).
_KNOWN_COLUMNS = {
    # _ARIS.Case / Supply_Chain_KPI_Tuned
    "Case ID",
    "Actual delivery Date",
    "Requested Delivery Date",
    "Avg delay duration",
    "On- time",
    "On-time probability",
    "In-full probability",
    "Delivery Compliance (%)",
    "Utilization Efficiency (%)",
    "Daily Shift Capacity Utilization (%)",
    "Region of the Country",
    "Delivery Impacted by Route Disruptions",
    "Risk ratio",
    "Delay Risk",
    "Stock shortage in warehouse",
    "Transport Delay",
    # Supply_Chain_KPI_Tuned_5500_csv (same table, different PQL alias)
    "Delay Reason for Delivery",
    # Delivery DIM_csv / Delivery_Dim
    "Delivery Number",
    "Source City",
    "Delivery_Date",
    "Delivery_Status",
    # Sales Order DIM_csv / Sales Order DIM
    "Sales Order Number",
    "Delivered Quantity",
    "Order Quantity",
    "Requested_Delivery_Date",
    # Warehouse DIM_csv / Warehouse DIM
    "QUANTITY",
    "WAREHOUSE ID",
    # Supply_Chain_KPI_Single_Sheet_5500_csv / Supply_Chain_KPI_Single_Sheet
    "Shipment Affected",
    "Warehouse Issue",
    "SAVINGS LOST(2025 PROJECTION)",
    "VALUE AT RISK(UTILIZATION BASED)",
    "OPERATOR NAME",
    "MATERIALS",
    "Packing accuracy",
    "ESTIMATED DELAY IMAPACT",
    "Warehouse_Record_Date",
}


def _fmt_label(label: str) -> str:
    """Format label to fixed column width for aligned output."""
    bracketed = f"[{label}]"
    return bracketed.ljust(_LABEL_WIDTH)


def _emit(label: str, value: str) -> None:
    """Print one pipeline stage line and mirror it to the logger."""
    line = f"  {_fmt_label(label)} → {value}"
    print(line)
    logger.info(line)


def _extract_tables_from_sql(sql: str) -> List[str]:
    """
    Extract table names from FROM / JOIN clauses in a SQL string.
    Handles both quoted ("TableName") and unquoted forms.
    """
    pattern = r'(?:FROM|JOIN)\s+"([^"]+)"|(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_ ]*?)(?:\s+\w+)?\s*(?:ON|WHERE|LEFT|RIGHT|INNER|OUTER|$)'
    tables: List[str] = []
    for m in re.finditer(pattern, sql, re.IGNORECASE):
        name = (m.group(1) or m.group(2) or "").strip()
        if name and name.upper() not in ("SELECT", "WHERE", "ON") and name not in tables:
            tables.append(name)
    return tables if tables else ["(unknown)"]


class PipelineLogger:
    """
    Emits each pipeline stage in a consistent, aligned format.
    All methods are safe to call with None / empty values.
    """

    # ── Stage emitters ────────────────────────────────────────────────────────

    @staticmethod
    def separator() -> None:
        """Print a visual separator before a new pipeline run."""
        line = "  " + "─" * 72
        print(line)
        logger.info(line)

    @staticmethod
    def nl_input(query: str) -> None:
        """[NL INPUT] — raw user query received by the pipeline."""
        _emit("NL INPUT", query or "(empty)")

    @staticmethod
    def kpi_identified(kpi_name: str, confidence: float) -> None:
        """[KPI IDENTIFIED] — matched KPI name and confidence score."""
        _emit("KPI IDENTIFIED", f"{kpi_name}  (conf={confidence:.2f})")

    @staticmethod
    def pql_from_csv(pql_expr: str, kpi_name: str) -> None:
        """[PQL FROM CSV] — raw PQL Logic string from kpi_definitions.csv."""
        display = pql_expr or "(none)"
        if len(display) > 120:
            display = display[:117] + "..."
        _emit("PQL FROM CSV", f"({kpi_name})  {display}")

    @staticmethod
    def pql_validated(
        kpi_name: str,
        kpi_defs: "KPIDefinitions",
        pql_expr: str,
    ) -> None:
        """
        [PQL VALIDATED] — validate PQL against kpi_definitions.csv.

        Checks:
          1. KPI name exists in kpi_definitions.csv.
          2. Column names extracted from PQL table references
             ("TableName"."ColumnName" format) are in the known column set.
             Unknown column names each produce an individual WARN line.
        """
        warnings: List[str] = []

        # ── Check 1: KPI name exists in kpi_definitions.csv ──────────────
        known_def = kpi_defs.get(kpi_name) if kpi_defs else None
        if known_def is None:
            warnings.append(f"metric '{kpi_name}' not found in kpi_definitions.csv")

        # ── Check 2: column references in PQL ────────────────────────────
        if pql_expr:
            # Build extended known set from source_column field in kpi_defs
            extra_known: set = set()
            if kpi_defs:
                for defn in kpi_defs.definitions.values():
                    src = defn.get("source_column", "")
                    # "TableName.ColumnName" → take last segment
                    parts = src.split(".")
                    if len(parts) >= 2:
                        col = parts[-1].strip()
                        if col:
                            extra_known.add(col)

            all_known = _KNOWN_COLUMNS | extra_known

            # Match both raw PQL form: "TableName"."ColumnName"
            # and post-translation SQL form: alias."ColumnName"
            raw_refs  = re.findall(r'"[^"]+"\."([^"]+)"', pql_expr)
            sql_refs  = re.findall(r'(?:sk|dd|so|wd|ss)\."([^"]+)"', pql_expr)
            col_refs  = raw_refs + sql_refs

            seen_unknown: set = set()
            for col in col_refs:
                if col not in all_known and col not in seen_unknown:
                    warnings.append(
                        f"unknown field '{col}' — not in kpi_definitions.csv"
                    )
                    seen_unknown.add(col)

        if warnings:
            for w in warnings:
                _emit("PQL VALIDATED", f"WARN: {w}")
        else:
            _emit("PQL VALIDATED", "PASS")

    @staticmethod
    def sql_generated(sql_str: str) -> None:
        """[SQL GENERATED] — final PostgreSQL query translated from PQL."""
        display = (sql_str or "(none)").replace("\n", " ").strip()
        if len(display) > 200:
            display = display[:197] + "..."
        _emit("SQL GENERATED", display)

    @staticmethod
    def db_hit(sql_str: str) -> None:
        """[DB HIT] — SQL dispatched to the database; shows targeted table(s)."""
        tables = _extract_tables_from_sql(sql_str or "")
        _emit("DB HIT", "Supabase PostgreSQL → " + ", ".join(tables))

    @staticmethod
    def result(rows: list) -> None:
        """[RESULT] — row count returned from the database with a data sample."""
        count = len(rows) if rows is not None else 0
        if count == 0:
            _emit("RESULT", "0 rows returned")
        elif count == 1:
            sample = str(rows[0])
            if len(sample) > 150:
                sample = sample[:147] + "..."
            _emit("RESULT", f"1 row → {sample}")
        else:
            sample = str(rows[0])
            if len(sample) > 100:
                sample = sample[:97] + "..."
            _emit("RESULT", f"{count} rows  (sample: {sample})")
