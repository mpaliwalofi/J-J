"""
Test script to validate all dashboard metrics against ARIS dashboard values.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from db.query_runner import QueryRunner
from data.query_templates import QUERY_TEMPLATES

def test_all_metrics():
    """Test all query templates and compare with ARIS dashboard values."""

    db = QueryRunner()

    print("=" * 100)
    print(" ARIS DASHBOARD METRICS VALIDATION")
    print("=" * 100)
    print()

    results = []
    total_tests = len(QUERY_TEMPLATES)
    passed_tests = 0

    for metric_name, template in QUERY_TEMPLATES.items():
        print(f"Testing: {metric_name}")
        print(f"  Description: {template['description']}")
        print(f"  ARIS Dashboard: {template['dashboard_value']}")

        try:
            result = db.execute(template['query'])

            if result and len(result) > 0:
                # Get the column to use
                use_column = template.get('use', list(result[0].keys())[0])
                calculated_value = result[0].get(use_column)

                print(f"  Our Calculation: {calculated_value}")

                # Try to compare values
                match_status = "UNKNOWN"
                if 'dashboard_value' in template:
                    dashboard_val = template['dashboard_value']

                    # Simple string matching for now
                    if str(calculated_value) in dashboard_val or dashboard_val in str(calculated_value):
                        match_status = "MATCH"
                        passed_tests += 1
                    else:
                        # Try numeric comparison
                        try:
                            # Extract numeric value from dashboard string
                            import re
                            dash_num = re.findall(r'[\d.]+', dashboard_val)
                            if dash_num:
                                dash_num = float(dash_num[0])
                                calc_num = float(calculated_value) if calculated_value else 0

                                # Check if close (within 10%)
                                if abs(calc_num - dash_num) / max(dash_num, 0.001) < 0.10:
                                    match_status = "CLOSE"
                                    passed_tests += 0.5
                                else:
                                    match_status = "MISMATCH"
                        except:
                            match_status = "MISMATCH"

                print(f"  Match Status: {match_status}")

                if 'notes' in template:
                    print(f"  Notes: {template['notes']}")

                results.append({
                    'metric': metric_name,
                    'status': match_status,
                    'dashboard': template['dashboard_value'],
                    'calculated': calculated_value
                })

            else:
                print(f"  ERROR: No results returned")
                results.append({
                    'metric': metric_name,
                    'status': 'ERROR',
                    'dashboard': template['dashboard_value'],
                    'calculated': 'No results'
                })

        except Exception as e:
            print(f"  ERROR: {str(e)[:100]}")
            results.append({
                'metric': metric_name,
                'status': 'ERROR',
                'dashboard': template['dashboard_value'],
                'calculated': f'Error: {str(e)[:50]}'
            })

        print()

    # Summary
    print("=" * 100)
    print(" SUMMARY")
    print("=" * 100)
    print(f"Total Metrics: {total_tests}")
    print(f"Passed/Close: {passed_tests}")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    print()

    # Table of results
    print(f"{'Metric':<40} {'Status':<12} {'Dashboard':<20} {'Calculated':<20}")
    print("-" * 100)
    for r in results:
        calc_str = str(r['calculated'])[:18]
        print(f"{r['metric']:<40} {r['status']:<12} {r['dashboard']:<20} {calc_str:<20}")

    print()
    return results

if __name__ == "__main__":
    test_all_metrics()
