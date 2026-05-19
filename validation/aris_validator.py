# validation/aris_validator.py
#
# ARIS Dashboard Validation
#
# Validates computed KPI values against known ARIS dashboard reference values.
# Reference values are read directly from the ARIS dashboard screenshots.
#
# The previous implementation re-ran the same SQL and compared the result to
# itself — which always passed and was meaningless. This version compares
# against real ARIS reference values.

import re
import logging
from decimal import Decimal
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ── ARIS Dashboard reference values ──────────────────────────────────────────
# Source: ARIS dashboard — data year 2024.
# Keys match KPI label column names returned by the SQL agent.
# Values are the exact numbers shown on the ARIS dashboard.
ARIS_REFERENCE: Dict[str, float] = {
    # Column name → ARIS dashboard value (2024)
    "otif_pct":                      25.2,
    "predictive_otif_pct":           25.5,
    "open_orders_count":             4000.0,
    "total_value_eur":               219000000.0,   # 219.0m
    "delivery_on_time_pct":          52.5,
    "on_time_pct":                   52.5,
    "on_time_probability_pct":       52.5,
    "avg_delay_days":                105.0,
    "warehouse_count":               40.0,
    "warehouse_issue_count":         1500.0,        # 1.5k
    "utilization_efficiency_pct":    60.4,
    "savings_lost_eur":              3200000.0,     # 3.2m (matches sql_agent label)
    "savings_lost":                  3200000.0,     # legacy alias
    "materials_count":               2600.0,
    "orders_at_risk_pct":            77.1,
    "delay_order_value_eur":         18000000.0,    # 18m
    "packing_accuracy_pct":          95.0,
    "value_at_risk_eur":             24000000.0,    # 24m
    "total_operators":               370.0,
    "in_full_pct":                   None,          # not shown on main dashboard
    "delay_risk_count":              None,
}

# Tolerance per KPI (absolute, same units as the value)
ARIS_TOLERANCE: Dict[str, float] = {
    "otif_pct":                      2.0,    # percentage points
    "predictive_otif_pct":           2.0,
    "delivery_on_time_pct":          2.0,
    "on_time_pct":                   2.0,
    "on_time_probability_pct":       2.0,
    "utilization_efficiency_pct":    2.0,
    "in_full_pct":                   2.0,
    "orders_at_risk_pct":            2.0,
    "packing_accuracy_pct":          2.0,
    "avg_delay_days":                5.0,    # days
    "warehouse_count":               2.0,
    "warehouse_issue_count":         100.0,  # count
    "open_orders_count":             200.0,
    "materials_count":               100.0,
    "total_operators":               10.0,
    "total_value_eur":               5000000.0,
    "savings_lost_eur":              200000.0,
    "savings_lost":                  200000.0,
    "delay_order_value_eur":         1000000.0,
    "value_at_risk_eur":             1000000.0,
    "delay_risk_count":              100.0,
}

DEFAULT_TOLERANCE = 1.0


class ARISValidator:
    """
    Validates computed KPI values against ARIS dashboard reference values.

    For each query result, extracts the primary numeric KPI column, looks it up
    in ARIS_REFERENCE, and reports PASS / FAIL with the actual difference.
    """

    def validate_response(
        self,
        query: str,
        sql: str,
        ai_result: list,
        ai_answer: str,
    ) -> Dict[str, Any]:
        """
        Validate the primary numeric value in ai_result against ARIS reference.

        Returns a validation dict or {"validated": False} when not applicable.
        """
        logger.info("ARIS Validation starting...")
        try:
            if not ai_result:
                return {"validated": False, "reason": "no result rows"}

            first_row = ai_result[0]

            # Find the primary KPI column and its value
            kpi_col, computed_value = self._extract_kpi_value(first_row)
            if computed_value is None or kpi_col is None:
                logger.info("Validation skipped: non-numeric result")
                return {"validated": False, "reason": "non-numeric result"}

            logger.info(f"Extracted AI value: {computed_value} from column '{kpi_col}'")

            # Look up ARIS reference value
            aris_value = ARIS_REFERENCE.get(kpi_col)
            if aris_value is None:
                # Column not in reference table — skip validation
                logger.info(f"No ARIS reference for column '{kpi_col}' — skipping")
                return {
                    "validated": False,
                    "reason": f"no ARIS reference value for '{kpi_col}'",
                }

            tolerance = ARIS_TOLERANCE.get(kpi_col, DEFAULT_TOLERANCE)
            difference = abs(computed_value - aris_value)
            pct_diff = abs((computed_value - aris_value) / aris_value * 100) if aris_value != 0 else 0
            passed = difference <= tolerance

            logger.info(
                f"Validation complete: passed={passed}, "
                f"computed={computed_value}, aris={aris_value}, "
                f"diff={difference:.2f}, tolerance={tolerance}"
            )

            return {
                "validated":      True,
                "passed":         passed,
                "aris_value":     aris_value,
                "ai_value":       computed_value,
                "difference":     difference,
                "percentage_diff": pct_diff,
                "tolerance":      tolerance,
                "kpi_column":     kpi_col,
                "source":         "ARIS Dashboard",
                "unit":           self._infer_unit(kpi_col),
                "match_emoji":    "+" if passed else "x",
            }

        except Exception as e:
            logger.warning(f"Validation error: {e}", exc_info=True)
            return {"validated": False, "reason": str(e)}

    # ── helpers ───────────────────────────────────────────────────────────────

    def _extract_kpi_value(self, row: dict):
        """
        Return (column_name, float_value) for the first numeric column in the row
        that has a known ARIS reference.  Falls back to the first numeric column.
        """
        candidates = []
        for key, value in row.items():
            if value is None:
                continue
            if isinstance(value, (int, float, Decimal)):
                candidates.append((key, float(value)))
            elif isinstance(value, str):
                try:
                    # Strip currency / formatting characters
                    cleaned = value.replace('%', '').replace(',', '').replace('$', '').strip()
                    # Handle suffix shorthands used by the cases table:
                    #   "2.6k" → 2600,  "18.0m" → 18000000,  "105d" → 105
                    multiplier = 1.0
                    if cleaned.lower().endswith('k'):
                        multiplier = 1_000.0
                        cleaned = cleaned[:-1]
                    elif cleaned.lower().endswith('m'):
                        multiplier = 1_000_000.0
                        cleaned = cleaned[:-1]
                    elif cleaned.lower().endswith('d'):   # days suffix
                        cleaned = cleaned[:-1]
                    candidates.append((key, float(cleaned) * multiplier))
                except ValueError:
                    pass

        if not candidates:
            return None, None

        # Prefer a column that has a known ARIS reference
        for key, val in candidates:
            logger.info(f"Checking column '{key}': value={val}, type={type(val)}")
            if key in ARIS_REFERENCE and ARIS_REFERENCE[key] is not None:
                logger.info(f"Extracted numeric value: {val} from column '{key}'")
                return key, val

        # Fall back to first numeric column
        key, val = candidates[0]
        logger.info(f"Extracted numeric value: {val} from column '{key}' (fallback)")
        return key, val

    def _infer_unit(self, col_name: str) -> str:
        col_lower = col_name.lower()
        if 'pct' in col_lower or 'percentage' in col_lower or 'rate' in col_lower:
            return '%'
        if 'days' in col_lower or 'duration' in col_lower:
            return 'days'
        if col_lower in ('materials_count', 'warehouse_count', 'warehouse_issue_count',
                         'open_orders_count', 'total_operators', 'shipment_affected_count',
                         'transport_delay_count', 'high_risk_orders'):
            return 'count'
        if 'count' in col_lower or 'number' in col_lower or col_lower.startswith('#'):
            return 'count'
        if 'eur' in col_lower or 'value' in col_lower or 'savings' in col_lower or 'delay_order' in col_lower:
            return 'EUR'
        return ''

    def get_validation_summary(self, validation: Dict[str, Any]) -> str:
        """Format validation result as a readable CLI block."""
        if not validation.get("validated"):
            return ""

        status = "PASSED" if validation["passed"] else "FAILED"
        lines = [
            "-" * 60,
            "ARIS Dashboard Validation",
            "-" * 60,
            f"Match: {status}",
            f"   AI Value:    {validation['ai_value']} {validation.get('unit', '')}",
            f"   ARIS Value:  {validation['aris_value']} {validation.get('unit', '')}",
            f"   Difference:  {validation['difference']:.2f} (tolerance: +/-{validation['tolerance']})",
            f"   % Diff:      {validation['percentage_diff']:.2f}%",
            f"   Source:      {validation['source']}",
            "-" * 60,
        ]
        return "\n".join(lines)
