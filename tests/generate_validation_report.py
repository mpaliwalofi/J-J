#!/usr/bin/env python3
# tests/generate_validation_report.py
#
# Generate a visual HTML report comparing AI system output vs ARIS dashboard data

import json
import sys
from pathlib import Path
from datetime import datetime

def generate_html_report(validation_results_json: Path) -> Path:
    """
    Generate a beautiful HTML report from validation results.

    Args:
        validation_results_json: Path to JSON file with validation results

    Returns:
        Path to generated HTML report
    """
    with open(validation_results_json, 'r') as f:
        data = json.load(f)

    total = data['total_tests']
    passed = data['passed']
    failed = data['failed']
    pass_rate = (passed / total * 100) if total > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ARIS Dashboard Validation Report</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}

        .header h1 {{
            font-size: 32px;
            margin-bottom: 10px;
        }}

        .header p {{
            opacity: 0.9;
            font-size: 14px;
        }}

        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f8f9fa;
        }}

        .summary-card {{
            background: white;
            padding: 24px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            text-align: center;
        }}

        .summary-card h3 {{
            font-size: 14px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }}

        .summary-card .value {{
            font-size: 36px;
            font-weight: bold;
            color: #2d3748;
        }}

        .summary-card.passed .value {{
            color: #48bb78;
        }}

        .summary-card.failed .value {{
            color: #f56565;
        }}

        .summary-card.pass-rate .value {{
            color: {('#48bb78' if pass_rate >= 80 else '#ed8936' if pass_rate >= 60 else '#f56565')};
        }}

        .results {{
            padding: 40px;
        }}

        .results h2 {{
            font-size: 24px;
            margin-bottom: 20px;
            color: #2d3748;
        }}

        .test-case {{
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            margin-bottom: 16px;
            overflow: hidden;
        }}

        .test-case.passed {{
            border-left: 4px solid #48bb78;
        }}

        .test-case.failed {{
            border-left: 4px solid #f56565;
        }}

        .test-header {{
            padding: 20px;
            background: #f8f9fa;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
        }}

        .test-header:hover {{
            background: #e9ecef;
        }}

        .test-name {{
            font-weight: 600;
            font-size: 16px;
            color: #2d3748;
        }}

        .test-status {{
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }}

        .test-status.passed {{
            background: #c6f6d5;
            color: #22543d;
        }}

        .test-status.failed {{
            background: #fed7d7;
            color: #742a2a;
        }}

        .test-details {{
            padding: 20px;
            display: none;
            border-top: 1px solid #e2e8f0;
        }}

        .test-details.expanded {{
            display: block;
        }}

        .detail-row {{
            display: grid;
            grid-template-columns: 180px 1fr;
            padding: 8px 0;
            border-bottom: 1px solid #f7fafc;
        }}

        .detail-row:last-child {{
            border-bottom: none;
        }}

        .detail-label {{
            font-weight: 600;
            color: #4a5568;
            font-size: 14px;
        }}

        .detail-value {{
            color: #2d3748;
            font-family: 'Courier New', monospace;
            font-size: 13px;
        }}

        .sql-block {{
            background: #2d3748;
            color: #e2e8f0;
            padding: 16px;
            border-radius: 6px;
            overflow-x: auto;
            font-size: 13px;
            line-height: 1.6;
            margin-top: 8px;
        }}

        .footer {{
            text-align: center;
            padding: 20px;
            background: #f8f9fa;
            color: #6c757d;
            font-size: 14px;
        }}

        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
        }}

        .badge.success {{
            background: #c6f6d5;
            color: #22543d;
        }}

        .badge.error {{
            background: #fed7d7;
            color: #742a2a;
        }}
    </style>
    <script>
        function toggleDetails(id) {{
            const details = document.getElementById('details-' + id);
            details.classList.toggle('expanded');
        }}
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 ARIS Dashboard Validation Report</h1>
            <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
        </div>

        <div class="summary">
            <div class="summary-card">
                <h3>Total Tests</h3>
                <div class="value">{total}</div>
            </div>
            <div class="summary-card passed">
                <h3>Passed</h3>
                <div class="value">{passed}</div>
            </div>
            <div class="summary-card failed">
                <h3>Failed</h3>
                <div class="value">{failed}</div>
            </div>
            <div class="summary-card pass-rate">
                <h3>Pass Rate</h3>
                <div class="value">{pass_rate:.1f}%</div>
            </div>
        </div>

        <div class="results">
            <h2>Test Results</h2>
"""

    # Add each test result
    for idx, result in enumerate(data['results']):
        status_class = 'passed' if result['passed'] else 'failed'
        status_text = 'PASSED' if result['passed'] else 'FAILED'

        details = result.get('details', {})

        html += f"""
            <div class="test-case {status_class}">
                <div class="test-header" onclick="toggleDetails({idx})">
                    <span class="test-name">{result['test_name']}</span>
                    <span class="test-status {status_class}">{status_text}</span>
                </div>
                <div class="test-details" id="details-{idx}">
"""

        # Add query
        if 'query' in details:
            html += f"""
                    <div class="detail-row">
                        <div class="detail-label">Query</div>
                        <div class="detail-value">{details['query']}</div>
                    </div>
"""

        # Add expected vs actual for numeric tests
        if 'expected_value' in details and 'actual_value' in details:
            difference_class = 'success' if result['passed'] else 'error'
            html += f"""
                    <div class="detail-row">
                        <div class="detail-label">Expected Value</div>
                        <div class="detail-value">{details['expected_value']} {details.get('unit', '')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Actual Value</div>
                        <div class="detail-value">{details['actual_value']} {details.get('unit', '')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Difference</div>
                        <div class="detail-value">
                            <span class="badge {difference_class}">{details.get('difference', 0):.2f}</span>
                            (Tolerance: ±{details.get('tolerance', 0)})
                        </div>
                    </div>
"""

        # Add ARIS source
        if 'source' in details:
            html += f"""
                    <div class="detail-row">
                        <div class="detail-label">ARIS Source</div>
                        <div class="detail-value">{details['source']}</div>
                    </div>
"""

        # Add SQL
        if 'sql' in details:
            html += f"""
                    <div class="detail-row">
                        <div class="detail-label">Generated SQL</div>
                        <div class="detail-value">
                            <div class="sql-block">{details['sql']}</div>
                        </div>
                    </div>
"""

        # Add error if present
        if 'error' in details:
            html += f"""
                    <div class="detail-row">
                        <div class="detail-label">Error</div>
                        <div class="detail-value">
                            <span class="badge error">{details['error']}</span>
                        </div>
                    </div>
"""

        html += """
                </div>
            </div>
"""

    html += f"""
        </div>

        <div class="footer">
            <p>J&J Supply Chain NLP-to-SQL Agent | ARIS Dashboard Validation</p>
            <p>Report ID: {data['timestamp']}</p>
        </div>
    </div>
</body>
</html>
"""

    # Save HTML report
    report_path = validation_results_json.parent / f"validation_report_{data['timestamp']}.html"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html)

    return report_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_validation_report.py <validation_results.json>")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    if not json_path.exists():
        print(f"Error: {json_path} not found")
        sys.exit(1)

    report_path = generate_html_report(json_path)
    print(f"✅ Report generated: {report_path}")
