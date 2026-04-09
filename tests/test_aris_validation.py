# tests/test_aris_validation.py
#
# ARIS Dashboard Validation Tests
#
# Purpose: Validate that our AI-generated responses match the source ARIS dashboard data
# to ensure data accuracy and system reliability.
#
# Usage:
#   pytest tests/test_aris_validation.py -v
#   pytest tests/test_aris_validation.py -v --html=reports/validation_report.html

import pytest
import logging
from datetime import datetime
from typing import Dict, List, Any
import json
import os
from pathlib import Path

# Import your app components
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from app import handle_query
from db.query_runner import QueryRunner

logger = logging.getLogger(__name__)

# ── Test Data Directory ───────────────────────────────────────────────────────

ARIS_REFERENCE_DATA_DIR = Path(__file__).parent / "aris_reference_data"
VALIDATION_REPORTS_DIR = Path(__file__).parent.parent / "reports"

# Create directories if they don't exist
ARIS_REFERENCE_DATA_DIR.mkdir(exist_ok=True)
VALIDATION_REPORTS_DIR.mkdir(exist_ok=True)


# ── ARIS Reference Data Structure ────────────────────────────────────────────
#
# This represents the ground truth from ARIS dashboard
# You'll populate this with actual ARIS data

ARIS_GROUND_TRUTH = {
    "otif_2024": {
        "query": "What is OTIF in 2024?",
        "expected_value": 85.3,  # Replace with actual ARIS value
        "tolerance": 0.5,  # Allow 0.5% difference
        "unit": "percentage",
        "source": "ARIS Dashboard > KPIs > OTIF > 2024"
    },
    "warehouse_utilization_2024": {
        "query": "What is warehouse utilization in 2024?",
        "expected_value": 78.2,
        "tolerance": 1.0,
        "unit": "percentage",
        "source": "ARIS Dashboard > Warehouse > Utilization > 2024"
    },
    "delivery_delays_reasons": {
        "query": "Give me reasons for delivery delays",
        "expected_categories": [
            "Carrier Issue",
            "Weather Disruption",
            "Traffic Congestion",
            "Warehouse Delay",
            "Route Disruption"
        ],
        "expected_top_reason": "Carrier Issue",  # Most common reason
        "source": "ARIS Dashboard > Deliveries > Delay Reasons"
    },
    "shipments_affected_total": {
        "query": "Give me number of shipment affected",
        "expected_value": 1245,  # Replace with actual count from ARIS
        "tolerance": 10,  # Allow ±10 shipments difference
        "unit": "count",
        "source": "ARIS Dashboard > Shipments > Affected Count"
    },
    "on_time_delivery_rate_2024": {
        "query": "What is the on-time delivery rate in 2024?",
        "expected_value": 82.7,
        "tolerance": 0.5,
        "unit": "percentage",
        "source": "ARIS Dashboard > Deliveries > On-Time Rate > 2024"
    },
}


# ── Validation Helper Functions ───────────────────────────────────────────────

def extract_numeric_value(result: Dict[str, Any]) -> float:
    """Extract numeric value from query result."""
    if not result.get("success"):
        raise ValueError(f"Query failed: {result.get('error')}")

    data = result.get("result", [])
    if not data:
        raise ValueError("No data returned from query")

    # Try to find numeric value in first row
    first_row = data[0]
    for key, value in first_row.items():
        if isinstance(value, (int, float)):
            return float(value)
        # Try to parse string as number
        if isinstance(value, str):
            try:
                return float(value.replace('%', '').replace(',', '').strip())
            except ValueError:
                continue

    raise ValueError(f"Could not extract numeric value from: {first_row}")


def extract_categories(result: Dict[str, Any]) -> List[str]:
    """Extract list of categories from query result."""
    if not result.get("success"):
        raise ValueError(f"Query failed: {result.get('error')}")

    data = result.get("result", [])
    categories = []

    for row in data:
        # Find the categorical column (typically first non-numeric column)
        for key, value in row.items():
            if isinstance(value, str) and value.strip():
                categories.append(value.strip())
                break

    return categories


def calculate_percentage_difference(actual: float, expected: float) -> float:
    """Calculate percentage difference between actual and expected values."""
    if expected == 0:
        return abs(actual - expected)
    return abs((actual - expected) / expected * 100)


# ── Validation Test Cases ─────────────────────────────────────────────────────

class TestARISValidation:
    """
    Validation tests comparing AI system output against ARIS dashboard ground truth.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        self.validation_results = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        yield
        # Generate report after all tests
        self._generate_validation_report()

    def _record_validation(self, test_name: str, passed: bool, details: Dict):
        """Record validation result for reporting."""
        self.validation_results.append({
            "test_name": test_name,
            "passed": passed,
            "timestamp": datetime.now().isoformat(),
            "details": details
        })

    def _generate_validation_report(self):
        """Generate HTML validation report."""
        report_path = VALIDATION_REPORTS_DIR / f"aris_validation_{self.timestamp}.json"

        with open(report_path, 'w') as f:
            json.dump({
                "timestamp": self.timestamp,
                "total_tests": len(self.validation_results),
                "passed": sum(1 for r in self.validation_results if r["passed"]),
                "failed": sum(1 for r in self.validation_results if not r["passed"]),
                "results": self.validation_results
            }, f, indent=2)

        logger.info(f"Validation report saved to: {report_path}")
        print(f"\n📊 Validation Report: {report_path}")

    # ── Numeric KPI Validation Tests ──────────────────────────────────────────

    @pytest.mark.parametrize("test_case_key", [
        "otif_2024",
        "warehouse_utilization_2024",
        "shipments_affected_total",
        "on_time_delivery_rate_2024"
    ])
    def test_numeric_kpi_accuracy(self, test_case_key):
        """
        Validate numeric KPI values against ARIS ground truth.

        This test:
        1. Queries the AI system with the natural language question
        2. Extracts the numeric result
        3. Compares against ARIS dashboard expected value
        4. Validates within tolerance threshold
        """
        test_case = ARIS_GROUND_TRUTH[test_case_key]

        # Query our AI system
        result = handle_query(test_case["query"])

        # Extract actual value
        try:
            actual_value = extract_numeric_value(result)
        except ValueError as e:
            self._record_validation(test_case_key, False, {
                "error": str(e),
                "query": test_case["query"],
                "expected": test_case["expected_value"]
            })
            pytest.fail(f"Failed to extract value: {e}")

        # Calculate difference
        diff = abs(actual_value - test_case["expected_value"])
        percentage_diff = calculate_percentage_difference(actual_value, test_case["expected_value"])

        # Validate within tolerance
        passed = diff <= test_case["tolerance"]

        self._record_validation(test_case_key, passed, {
            "query": test_case["query"],
            "expected_value": test_case["expected_value"],
            "actual_value": actual_value,
            "difference": diff,
            "percentage_difference": percentage_diff,
            "tolerance": test_case["tolerance"],
            "unit": test_case["unit"],
            "source": test_case["source"],
            "sql": result.get("sql"),
            "row_count": result.get("row_count")
        })

        assert passed, (
            f"\n❌ ARIS Validation Failed for {test_case_key}\n"
            f"Query: {test_case['query']}\n"
            f"Expected: {test_case['expected_value']} {test_case['unit']}\n"
            f"Actual: {actual_value} {test_case['unit']}\n"
            f"Difference: {diff} (±{test_case['tolerance']} allowed)\n"
            f"Percentage Diff: {percentage_diff:.2f}%\n"
            f"ARIS Source: {test_case['source']}\n"
            f"Generated SQL: {result.get('sql')}"
        )

    # ── Categorical Data Validation Tests ─────────────────────────────────────

    def test_delay_reasons_categories(self):
        """
        Validate delivery delay reasons match ARIS dashboard categories.

        This test:
        1. Queries for delay reasons
        2. Extracts categorical data
        3. Validates all expected categories are present
        4. Validates the top reason matches ARIS
        """
        test_case = ARIS_GROUND_TRUTH["delivery_delays_reasons"]

        # Query our AI system
        result = handle_query(test_case["query"])

        # Extract categories
        try:
            actual_categories = extract_categories(result)
        except ValueError as e:
            self._record_validation("delivery_delays_reasons", False, {
                "error": str(e),
                "query": test_case["query"]
            })
            pytest.fail(f"Failed to extract categories: {e}")

        # Validate all expected categories are present
        missing_categories = [
            cat for cat in test_case["expected_categories"]
            if cat not in actual_categories
        ]

        # Check if top reason matches
        top_reason_matches = (
            len(actual_categories) > 0 and
            actual_categories[0] == test_case["expected_top_reason"]
        )

        passed = len(missing_categories) == 0 and top_reason_matches

        self._record_validation("delivery_delays_reasons", passed, {
            "query": test_case["query"],
            "expected_categories": test_case["expected_categories"],
            "actual_categories": actual_categories,
            "missing_categories": missing_categories,
            "expected_top_reason": test_case["expected_top_reason"],
            "actual_top_reason": actual_categories[0] if actual_categories else None,
            "top_reason_matches": top_reason_matches,
            "source": test_case["source"],
            "sql": result.get("sql")
        })

        assert len(missing_categories) == 0, (
            f"\n❌ Missing categories from ARIS: {missing_categories}\n"
            f"Expected: {test_case['expected_categories']}\n"
            f"Actual: {actual_categories}"
        )

        assert top_reason_matches, (
            f"\n❌ Top delay reason mismatch\n"
            f"Expected: {test_case['expected_top_reason']}\n"
            f"Actual: {actual_categories[0] if actual_categories else 'None'}"
        )

    # ── SQL Correctness Tests ─────────────────────────────────────────────────

    def test_sql_uses_correct_tables(self):
        """
        Validate that generated SQL uses the correct source tables
        matching ARIS data structure.
        """
        # Test a few queries and verify they use expected tables
        test_cases = [
            {
                "query": "What is OTIF in 2024?",
                "expected_tables": ["Supply_Chain_KPI_Tuned", "Supply_Chain_KPI_Single_Sheet"],  # One of these
                "must_not_use": ["Warehouse_DIM"]  # Should not use this table
            },
            {
                "query": "Give me reasons for delivery delays",
                "expected_tables": ["Supply_Chain_KPI_Tuned", "Supply_Chain_KPI_Single_Sheet"],
                "must_not_use": []
            }
        ]

        for test_case in test_cases:
            result = handle_query(test_case["query"])
            sql = result.get("sql", "").lower()

            # Check if at least one expected table is used
            uses_expected_table = any(
                table.lower() in sql for table in test_case["expected_tables"]
            )

            # Check if forbidden tables are NOT used
            uses_forbidden_table = any(
                table.lower() in sql for table in test_case["must_not_use"]
            )

            assert uses_expected_table, (
                f"\n❌ SQL does not use expected tables\n"
                f"Query: {test_case['query']}\n"
                f"Expected tables: {test_case['expected_tables']}\n"
                f"Generated SQL: {sql}"
            )

            assert not uses_forbidden_table, (
                f"\n❌ SQL uses forbidden table\n"
                f"Query: {test_case['query']}\n"
                f"Must not use: {test_case['must_not_use']}\n"
                f"Generated SQL: {sql}"
            )


# ── ARIS Reference Data Loader ────────────────────────────────────────────────

def load_aris_reference_data_from_csv():
    """
    Load ARIS reference data from CSV file.

    Create a CSV file at tests/aris_reference_data/ground_truth.csv with:
    - query: Natural language query
    - expected_value: Expected numeric value from ARIS
    - tolerance: Allowed difference
    - unit: Unit of measurement
    - source: ARIS dashboard path
    """
    csv_path = ARIS_REFERENCE_DATA_DIR / "ground_truth.csv"

    if not csv_path.exists():
        logger.warning(f"ARIS reference data CSV not found at {csv_path}")
        logger.warning("Using default test cases from code")
        return ARIS_GROUND_TRUTH

    import csv
    reference_data = {}

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row['test_case_id']
            reference_data[key] = {
                "query": row['query'],
                "expected_value": float(row['expected_value']) if row.get('expected_value') else None,
                "tolerance": float(row.get('tolerance', 0.5)),
                "unit": row.get('unit', ''),
                "source": row.get('source', ''),
                "expected_categories": row.get('expected_categories', '').split('|') if row.get('expected_categories') else []
            }

    return reference_data


# ── Standalone Validation Runner ──────────────────────────────────────────────

if __name__ == "__main__":
    """
    Run validation tests standalone and generate visual report.

    Usage:
        python tests/test_aris_validation.py
    """
    print("\n" + "="*80)
    print("ARIS DASHBOARD VALIDATION TEST SUITE")
    print("="*80 + "\n")

    # Run pytest programmatically
    import subprocess
    result = subprocess.run([
        "pytest",
        __file__,
        "-v",
        "--tb=short",
        f"--html={VALIDATION_REPORTS_DIR}/validation_report.html",
        "--self-contained-html"
    ])

    print("\n" + "="*80)
    print(f"📊 Validation Report: {VALIDATION_REPORTS_DIR}/validation_report.html")
    print("="*80 + "\n")

    sys.exit(result.returncode)
