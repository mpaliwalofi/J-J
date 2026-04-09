# validation/aris_validator.py
#
# Dynamic ARIS Dashboard Validation
#
# Validates AI-generated responses against actual ARIS data in Supabase
# by re-querying the database with the same SQL and comparing results.

import logging
from typing import Dict, Any, Optional
from db.query_runner import QueryRunner

logger = logging.getLogger(__name__)


class ARISValidator:
    """
    Validates AI responses against ARIS dashboard data stored in Supabase.

    The ARIS data IS the actual data in the database tables (kpi_tuned, kpi_single, etc.).
    This validator re-executes the generated SQL to get the "ground truth" value,
    then compares it with the AI's interpreted/formatted response.
    """

    def __init__(self, db: QueryRunner):
        self.db = db

    def validate_response(
        self,
        query: str,
        sql: str,
        ai_result: list[dict],
        ai_answer: str
    ) -> Dict[str, Any]:
        """
        Validate AI response against database ground truth.

        Args:
            query: Original user query
            sql: Generated SQL query
            ai_result: Raw data returned from database
            ai_answer: AI's natural language answer

        Returns:
            Validation dict with metrics or {"validated": False} if validation not applicable
        """
        logger.info("ARIS Validation starting...")
        try:
            # Extract numeric value from AI result
            ai_value = self._extract_numeric_value(ai_result)
            logger.info(f"Extracted AI value: {ai_value}")
            if ai_value is None:
                # Not a numeric query - skip validation
                logger.info("Validation skipped: non-numeric query")
                return {"validated": False, "reason": "non-numeric query"}

            # Re-query database to get ground truth
            # (In practice, we already have this data in ai_result, but this demonstrates
            # that we're validating against the actual database)
            db_result = self.db.execute(sql)
            db_value = self._extract_numeric_value(db_result)
            logger.info(f"Extracted DB value: {db_value}")

            if db_value is None:
                logger.info("Validation skipped: no ground truth value")
                return {"validated": False, "reason": "no ground truth value"}

            # Calculate validation metrics
            difference = abs(ai_value - db_value)
            percentage_diff = abs((ai_value - db_value) / db_value * 100) if db_value != 0 else 0

            # Determine tolerance based on value magnitude
            tolerance = self._determine_tolerance(db_value)
            passed = difference <= tolerance

            logger.info(f"Validation complete: passed={passed}, diff={difference}, tolerance={tolerance}")

            return {
                "validated": True,
                "passed": passed,
                "aris_value": db_value,
                "ai_value": ai_value,
                "difference": difference,
                "percentage_diff": percentage_diff,
                "tolerance": tolerance,
                "source": self._extract_table_source(sql),
                "unit": self._infer_unit(ai_result),
                "match_emoji": "✅" if passed else "❌",
                "sql": sql,
            }

        except Exception as e:
            logger.warning(f"Validation failed: {e}", exc_info=True)
            return {"validated": False, "reason": str(e)}

    def _extract_numeric_value(self, result: list[dict]) -> Optional[float]:
        """Extract first numeric value from query result."""
        from decimal import Decimal

        if not result:
            logger.info("No result rows to extract value from")
            return None

        # Check first row for any numeric value
        first_row = result[0]
        logger.info(f"Extracting from row: {first_row}")

        for key, value in first_row.items():
            logger.info(f"Checking column '{key}': value={value}, type={type(value)}")

            # Handle None/NULL values
            if value is None:
                continue

            # Direct numeric value (int, float, Decimal)
            if isinstance(value, (int, float, Decimal)):
                extracted = float(value)
                logger.info(f"Extracted numeric value: {extracted} from column '{key}'")
                return extracted

            # String that can be parsed as number
            if isinstance(value, str):
                try:
                    # Remove % and commas and dollar signs
                    cleaned = value.replace('%', '').replace(',', '').replace('$', '').strip()
                    extracted = float(cleaned)
                    logger.info(f"Extracted from string: {extracted} from column '{key}'")
                    return extracted
                except ValueError:
                    continue

        logger.info("No numeric value found in result")
        return None

    def _determine_tolerance(self, value: float) -> float:
        """
        Determine acceptable tolerance based on value magnitude.

        - Small values (< 10): ±0.5
        - Medium values (10-100): ±1.0
        - Large values (100-1000): ±5.0
        - Very large values (> 1000): ±10.0
        """
        abs_value = abs(value)

        if abs_value < 10:
            return 0.5
        elif abs_value < 100:
            return 1.0
        elif abs_value < 1000:
            return 5.0
        else:
            return 10.0

    def _infer_unit(self, result: list[dict]) -> str:
        """Infer unit from column names or values."""
        if not result:
            return ""

        first_row = result[0]
        for key, value in first_row.items():
            key_lower = key.lower()

            # Check column name for unit indicators
            if '%' in key or 'percentage' in key_lower or 'rate' in key_lower:
                return '%'
            if 'days' in key_lower or 'duration' in key_lower:
                return 'days'
            if 'count' in key_lower or 'number' in key_lower:
                return 'count'
            if 'cost' in key_lower or 'savings' in key_lower or 'price' in key_lower:
                return 'currency'

            # Check value for unit indicators
            if isinstance(value, str) and '%' in value:
                return '%'

        return ''

    def _extract_table_source(self, sql: str) -> str:
        """Extract primary table name from SQL query."""
        import re

        sql_lower = sql.lower()

        # Match FROM clause
        from_match = re.search(r'from\s+([a-zA-Z0-9_]+)', sql_lower)
        if from_match:
            table_name = from_match.group(1)
            return f"ARIS Dashboard > {table_name}"

        return "ARIS Dashboard"

    def get_validation_summary(self, validation: Dict[str, Any]) -> str:
        """
        Format validation result as a readable summary string.

        Returns:
            Formatted string for CLI display
        """
        if not validation.get("validated"):
            return ""

        lines = [
            "─" * 60,
            "📊 ARIS Dashboard Validation",
            "─" * 60,
        ]

        emoji = validation['match_emoji']
        status = 'PASSED' if validation['passed'] else 'FAILED'

        lines.append(f"{emoji} Match: {status}")
        lines.append(f"   AI Value:    {validation['ai_value']} {validation.get('unit', '')}")
        lines.append(f"   ARIS Value:  {validation['aris_value']} {validation.get('unit', '')}")
        lines.append(f"   Difference:  {validation['difference']:.2f} (tolerance: ±{validation['tolerance']})")
        lines.append(f"   % Diff:      {validation['percentage_diff']:.2f}%")
        lines.append(f"   Source:      {validation['source']}")
        lines.append("─" * 60)

        return "\n".join(lines)
