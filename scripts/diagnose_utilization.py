"""
Diagnostic script — Utilization Efficiency % column analysis.

Runs three queries to understand why AVG("Utilization Efficiency (%)") from
Supply_Chain_KPI_Tuned returns 49.89% while the ARIS dashboard shows 60.4%.

Run:
    python scripts/diagnose_utilization.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from db.query_runner import QueryRunner

db = QueryRunner()

DIVIDER = "-" * 60

# ── Q1: Distribution — nulls, zeros, min/max/avg/median ──────────────────────
print(f"\n{DIVIDER}")
print("Q1: Distribution of \"Utilization Efficiency (%)\"")
print(DIVIDER)

q1 = """
SELECT
  COUNT(*)                                                    AS total_rows,
  COUNT("Utilization Efficiency (%)")                        AS non_null_rows,
  COUNT(*) - COUNT("Utilization Efficiency (%)")             AS null_rows,
  SUM(CASE WHEN "Utilization Efficiency (%)" = 0 THEN 1 ELSE 0 END) AS zero_rows,
  CAST(MIN("Utilization Efficiency (%)")  AS NUMERIC(10,2))  AS min_val,
  CAST(MAX("Utilization Efficiency (%)")  AS NUMERIC(10,2))  AS max_val,
  CAST(AVG("Utilization Efficiency (%)")  AS NUMERIC(10,4))  AS avg_all_rows,
  CAST(AVG("Utilization Efficiency (%)") FILTER (
       WHERE "Utilization Efficiency (%)" > 0)
       AS NUMERIC(10,4))                                      AS avg_nonzero,
  CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (
       ORDER BY "Utilization Efficiency (%)")
       AS NUMERIC(10,4))                                      AS median_val
FROM "Supply_Chain_KPI_Tuned"
"""
rows = db.execute(q1)
for k, v in rows[0].items():
    print(f"  {k:<30} {v}")

# ── Q2: Distinct values — how many unique values? ────────────────────────────
print(f"\n{DIVIDER}")
print("Q2: How many distinct values? (grain check)")
print(DIVIDER)

q2 = """
SELECT
  COUNT(DISTINCT "Utilization Efficiency (%)")  AS distinct_values,
  COUNT(DISTINCT "Warehouse_Record_Date")        AS distinct_dates,
  COUNT(DISTINCT "Region of the Country")        AS distinct_regions
FROM "Supply_Chain_KPI_Tuned"
"""
rows = db.execute(q2)
for k, v in rows[0].items():
    print(f"  {k:<30} {v}")

# ── Q3: Average grouped by region/date — warehouse-level grain ───────────────
print(f"\n{DIVIDER}")
print("Q3: AVG per region (check if 60.4% appears at grouped grain)")
print(DIVIDER)

q3 = """
SELECT
  "Region of the Country"                                        AS region,
  COUNT(*)                                                        AS row_count,
  CAST(AVG("Utilization Efficiency (%)")  AS NUMERIC(10,2))      AS avg_utilization,
  CAST(AVG("Utilization Efficiency (%)") FILTER (
       WHERE "Utilization Efficiency (%)" > 0)
       AS NUMERIC(10,2))                                          AS avg_nonzero
FROM "Supply_Chain_KPI_Tuned"
GROUP BY "Region of the Country"
ORDER BY row_count DESC
LIMIT 10
"""
rows = db.execute(q3)
if rows:
    headers = list(rows[0].keys())
    print("  " + "  ".join(f"{h:<22}" for h in headers))
    print("  " + "  ".join("-" * 22 for _ in headers))
    for r in rows:
        print("  " + "  ".join(f"{str(v):<22}" for v in r.values()))

# ── Q4: Check "Daily Shift Capacity Utilization (%)" as alternative ──────────
print(f"\n{DIVIDER}")
print("Q4: \"Daily Shift Capacity Utilization (%)\" — alternative column?")
print(DIVIDER)

q4 = """
SELECT
  COUNT(*)                                                           AS total_rows,
  COUNT("Daily Shift Capacity Utilization (%)")                     AS non_null_rows,
  CAST(AVG("Daily Shift Capacity Utilization (%)")  AS NUMERIC(10,4)) AS avg_val,
  CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (
       ORDER BY "Daily Shift Capacity Utilization (%)")
       AS NUMERIC(10,4))                                             AS median_val
FROM "Supply_Chain_KPI_Tuned"
"""
try:
    rows = db.execute(q4)
    for k, v in rows[0].items():
        print(f"  {k:<30} {v}")
except Exception as e:
    print(f"  Column not found or error: {e}")

print(f"\n{DIVIDER}")
print("INTERPRETATION GUIDE")
print(DIVIDER)
print("""
  If avg_nonzero (Q1) ≈ 60.4%  → ARIS excludes zero rows; fix: add WHERE > 0
  If median_val   (Q1) ≈ 60.4%  → ARIS uses median not mean; fix: use PERCENTILE_CONT
  If avg_val      (Q4) ≈ 60.4%  → wrong column; fix: use Daily Shift Capacity Utilization
  If avg per region (Q3) ≈ 60.4% → ARIS averages by region first, then overall
  If distinct_values (Q2) is small → column is a shift/day metric repeated per order
""")
