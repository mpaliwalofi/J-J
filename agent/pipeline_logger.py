"""
Pipeline Logger — Structured terminal output for the NL → PQL → SQL → DB flow.

Each stage is printed as a fixed-width label block so the terminal output can
be scanned at a glance:

  [NL INPUT]  → ...raw user query...
  [PQL]       → (KPI name)  ...PQL expression...
  [PQL VALID] → PASS  or  WARN: <reason>
  [SQL]       → ...generated SQL (first 200 chars)...
  [DB HIT]    → Supabase PostgreSQL → <table(s)>
  [RESULT]    → N rows  (sample: {...})

Usage (from any module in the pipeline):
    from agent.pipeline_logger import PipelineLogger
    log = PipelineLogger()
    log.nl_input(query)
    log.pql(pql_expr, kpi_name)
    log.pql_validation(kpi_name, kpi_defs, pql_expr)
    log.sql(sql)
    log.db_hit(sql)
    log.result(rows)
"""

import re
import logging
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from agent.sql_agent import KPIDefinitions

logger = logging.getLogger(__name__)

# Width of the label column (including brackets), e.g. "[NL INPUT] " → 11 chars
_LABEL_WIDTH = 11

# Known column names across all source tables — used for PQL field validation.
# Populated from the PQL table mapping and common column names seen in the schema.
_KNOWN_COLUMNS = {
    # Supply_Chain_KPI_Tuned / _ARIS.Case
    "Case ID",
    "Delivery Compliance (%)",
    "Warehouse_Record_Date",
    "Daily Shift Capacity Utilization (%)",
    "Utilization Efficiency (%)",
    "Region of the Country",
    "Delivery Impacted by Route Disruptions",
    "Risk ratio",
    "On- time",
    "On-time probability",
    "In-full probability",
    "Avg delay duration",
    "Stock shortage in warehouse",
    "Transport Delay",
    "Actual delivery Date",
    "Requested Delivery Date",
    # Delivery_Dim
    "Delivery Number",
    "Source City",
    "Delivery_Date",
    "Delivery_Status",
    # Sales Order DIM
    "Sales Order Number",
    "Delivered Quantity",
    "Order Quantity",
    "Requested_Delivery_Date",
    # Warehouse DIM
    "QUANTITY",
    "WAREHOUSE ID",
    # Supply_Chain_KPI_Single_Sheet
    "Shipment Affected",
    "Warehouse Issue",
    "SAVINGS LOST(2025 PROJECTION)",
    "VALUE AT RISK(UTILIZATION BASED)",
    "OPERATOR NAME",
    "MATERIALS",
    "Packing accuracy",
    # Supply_Chain_KPI_Tuned_5500
    "Delay Reason for Delivery",
    "ESTIMATED DELAY IMAPACT",
    # cases table
    "name",
    "value",
}


def _fmt_label(label: str) -> str:
    """Left-pad the label to a fixed column width for aligned output."""
    bracketed = f"[{label}]"
    return bracketed.ljust(_LABEL_WIDTH)


def _emit(label: str, value: str) -> None:
    """Print one pipeline stage line and mirror it to the logger."""
    line = f"  {_fmt_label(label)} → {value}"
    print(line)
    logger.info(line)


def _extract_tables_from_sql(sql: str) -> List[str]:
    """
    Extract table names from FROM / JOIN clauses.
    Handles both  FROM "TableName" alias  and  FROM TableName alias  forms.
    """
    # Match: FROM|JOIN  "Quoted Name"  or  UnquotedName
    pattern = r'(?:FROM|JOIN)\s+"([^"]+)"|(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_ ]*?)(?:\s+\w+)?\s*(?:ON|WHERE|LEFT|RIGHT|INNER|OUTER|$)'
    tables: List[str] = []
    for m in re.finditer(pattern, sql, re.IGNORECASE):
        name = (m.group(1) or m.group(2) or "").strip()
        if name and name.upper() not in ("SELECT", "WHERE", "ON") and name not in tables:
            tables.append(name)
    return tables if tables else ["(unknown)"]


class PipelineLogger:
    """
    Thin wrapper that emits each pipeline stage in a consistent format.

    All methods are safe to call with None / empty values — they will
    emit a placeholder rather than raise.
    """

    # ── Stage emitters ────────────────────────────────────────────────────────

    @staticmethod
    def separator() -> None:
        """Print a visual separator before a new pipeline run."""
        line = "  " + "─" * 68
        print(line)
        logger.info(line)

    @staticmethod
    def nl_input(query: str) -> None:
        """[NL INPUT] — raw user query received by the pipeline."""
        _emit("NL INPUT", query or "(empty)")

    @staticmethod
    def pql(pql_expr: str, kpi_name: str) -> None:
        """[PQL] — intermediate PQL expression identified for this KPI."""
        display = pql_expr or "(none)"
        if len(display) > 120:
            display = display[:117] + "..."
        _emit("PQL", f"({kpi_name})  {display}")

    @staticmethod
    def pql_validation(
        kpi_name: str,
        kpi_defs: "KPIDefinitions",
        pql_expr: str,
    ) -> None:
        """
        [PQL VALID] — validate PQL against kpi_definitions.csv.

        Checks:
          1. KPI name exists in kpi_definitions.csv.
          2. Column references in the PQL (alias."Column") are in the known
             column set derived from the schema and kpi_definitions source
             columns.  Unknown column names trigger individual WARNs.
        """
        warnings: List[str] = []

        # ── Check 1: KPI name in kpi_definitions.csv ─────────────────────
        known_def = kpi_defs.get(kpi_name) if kpi_defs else None
        if known_def is None:
            warnings.append(f"metric '{kpi_name}' not found in kpi_definitions.csv")

        # ── Check 2: column references in PQL ────────────────────────────
        if pql_expr:
            # Build extended known set from kpi_defs source columns
            extra_known: set = set()
            if kpi_defs:
                for defn in kpi_defs.definitions.values():
                    src = defn.get("source_column", "")
                    # Source column field may be "TableName.ColumnName"
                    parts = src.split(".")
                    if len(parts) >= 2:
                        col = parts[-1].strip()
                        if col:
                            extra_known.add(col)

            all_known = _KNOWN_COLUMNS | extra_known

            # Extract  alias."Column Name"  patterns from the PQL expression
            col_refs = re.findall(r'(?:sk|dd|so|wd|ss)\."([^"]+)"', pql_expr)
            seen_unknown: set = set()
            for col in col_refs:
                if col not in all_known and col not in seen_unknown:
                    warnings.append(f"unknown field '{col}' not in kpi_definitions.csv")
                    seen_unknown.add(col)

        if warnings:
            for w in warnings:
                _emit("PQL VALID", f"WARN: {w}")
        else:
            _emit("PQL VALID", "PASS")

    @staticmethod
    def sql(sql_str: str) -> None:
        """[SQL] — final SQL query generated from PQL."""
        display = (sql_str or "(none)").replace("\n", " ").strip()
        if len(display) > 200:
            display = display[:197] + "..."
        _emit("SQL", display)

    @staticmethod
    def db_hit(sql_str: str) -> None:
        """[DB HIT] — SQL dispatched to the database; shows targeted table(s)."""
        tables = _extract_tables_from_sql(sql_str or "")
        _emit("DB HIT", "Supabase PostgreSQL → " + ", ".join(tables))

    @staticmethod
    def result(rows: list) -> None:
        """[RESULT] — row count returned from the database, with a data sample."""
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
