"""
Grain diagnostic — understand why KPI values differ from ARIS dashboard.

Checks:
  1. Are Case IDs unique in Supply_Chain_KPI_Tuned? (grain = order or event?)
  2. Does joining Delivery_Dim / Sales Order DIM cause row multiplication?
  3. What does OTIF look like with deduplication vs raw AVG?
  4. Delivery on time % with dedup vs raw?

Run:
    python scripts/diagnose_grain.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from db.query_runner import QueryRunner

db = QueryRunner()

DIV = "-" * 70

# ── Q1: Case ID grain in Supply_Chain_KPI_Tuned ──────────────────────────────
print(f"\n{DIV}")
print("Q1: Is 'Case ID' unique in Supply_Chain_KPI_Tuned?")
print(DIV)

q1 = """
SELECT
  COUNT(*)                         AS total_rows,
  COUNT(DISTINCT "Case ID")        AS distinct_case_ids,
  COUNT(*) - COUNT(DISTINCT "Case ID") AS duplicate_rows,
  CASE WHEN COUNT(*) = COUNT(DISTINCT "Case ID")
       THEN 'YES - one row per case'
       ELSE 'NO - multiple rows per case (grain != case)' END AS grain_check
FROM "Supply_Chain_KPI_Tuned"
"""
rows = db.execute(q1)
for k, v in rows[0].items():
    print(f"  {k:<40} {v}")

# ── Q2: Join multiplier — how many rows after joining Delivery_Dim? ──────────
print(f"\n{DIV}")
print("Q2: Row count after JOIN with Delivery_Dim (fan-out check)")
print(DIV)

q2 = """
SELECT
  COUNT(*)                                   AS joined_rows,
  COUNT(DISTINCT sk."Case ID")               AS distinct_cases_after_join,
  CAST(COUNT(*) AS FLOAT)
    / NULLIF(COUNT(DISTINCT sk."Case ID"), 0) AS avg_rows_per_case
FROM "Supply_Chain_KPI_Tuned" sk
JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"
"""
try:
    rows = db.execute(q2)
    for k, v in rows[0].items():
        print(f"  {k:<40} {v}")
except Exception as e:
    print(f"  JOIN failed: {e}")

# ── Q3: Join multiplier — Sales Order DIM ───────────────────────────────────
print(f"\n{DIV}")
print("Q3: Row count after JOIN with Sales Order DIM (fan-out check)")
print(DIV)

q3 = """
SELECT
  COUNT(*)                                   AS joined_rows,
  COUNT(DISTINCT sk."Case ID")               AS distinct_cases_after_join,
  CAST(COUNT(*) AS FLOAT)
    / NULLIF(COUNT(DISTINCT sk."Case ID"), 0) AS avg_rows_per_case
FROM "Supply_Chain_KPI_Tuned" sk
JOIN "Sales Order DIM" so ON sk."Case ID" = so."Case ID"
"""
try:
    rows = db.execute(q3)
    for k, v in rows[0].items():
        print(f"  {k:<40} {v}")
except Exception as e:
    print(f"  JOIN failed (may need different key): {e}")

# ── Q4: OTIF % — raw vs deduplicated ─────────────────────────────────────────
print(f"\n{DIV}")
print("Q4: OTIF % — raw AVG vs case-deduplicated (ARIS shows 25.2%)")
print(DIV)

q4 = """
SELECT
  -- Raw AVG over all rows (what we currently do)
  CAST(100.0 * AVG(
    CASE WHEN "Actual delivery Date" IS NOT NULL
          AND "Requested Delivery Date" IS NOT NULL
          AND CAST("Actual delivery Date" AS DATE) <= CAST("Requested Delivery Date" AS DATE)
          AND "Delivery Compliance (%)" >= 100
    THEN 1.0 ELSE 0.0 END
  ) AS NUMERIC(10,2)) AS otif_raw_avg,

  -- Count approach (same thing, explicit)
  CAST(100.0 * SUM(
    CASE WHEN "Actual delivery Date" IS NOT NULL
          AND "Requested Delivery Date" IS NOT NULL
          AND CAST("Actual delivery Date" AS DATE) <= CAST("Requested Delivery Date" AS DATE)
          AND "Delivery Compliance (%)" >= 100
    THEN 1 ELSE 0 END
  ) / NULLIF(COUNT(*), 0) AS NUMERIC(10,2)) AS otif_count_based,

  COUNT(*) AS total_rows,
  COUNT(DISTINCT "Case ID") AS distinct_cases
FROM "Supply_Chain_KPI_Tuned"
"""
try:
    rows = db.execute(q4)
    for k, v in rows[0].items():
        print(f"  {k:<40} {v}")
except Exception as e:
    print(f"  Error: {e}")

# ── Q5: Delivery on time % — check against 52.5% ─────────────────────────────
print(f"\n{DIV}")
print("Q5: Delivery on time % (ARIS shows 52.5%)")
print(DIV)

q5 = """
SELECT
  CAST(100.0 * AVG(
    CASE WHEN "Actual delivery Date" IS NOT NULL
          AND "Requested Delivery Date" IS NOT NULL
          AND CAST("Actual delivery Date" AS DATE) <= CAST("Requested Delivery Date" AS DATE)
    THEN 1.0 ELSE 0.0 END
  ) AS NUMERIC(10,2)) AS on_time_raw,

  CAST(100.0 * AVG(
    CASE WHEN "On- time" IS NOT NULL THEN "On- time" ELSE 0 END
  ) AS NUMERIC(10,2))  AS on_time_column_direct,

  COUNT(DISTINCT "Case ID") AS distinct_cases,
  COUNT(*) AS total_rows
FROM "Supply_Chain_KPI_Tuned"
"""
try:
    rows = db.execute(q5)
    for k, v in rows[0].items():
        print(f"  {k:<40} {v}")
except Exception as e:
    print(f"  Error: {e}")

# ── Q6: What columns does Supply_Chain_KPI_Tuned have that look like OTIF? ───
print(f"\n{DIV}")
print("Q6: Sample of date columns + Delivery Compliance to understand OTIF grain")
print(DIV)

q6 = """
SELECT
  "Case ID",
  "Actual delivery Date",
  "Requested Delivery Date",
  "Delivery Compliance (%)",
  "On- time"
FROM "Supply_Chain_KPI_Tuned"
LIMIT 5
"""
try:
    rows = db.execute(q6)
    if rows:
        headers = list(rows[0].keys())
        print("  " + "  ".join(f"{h:<30}" for h in headers))
        print("  " + "  ".join("-" * 30 for _ in headers))
        for r in rows:
            print("  " + "  ".join(f"{str(v):<30}" for v in r.values()))
except Exception as e:
    print(f"  Error: {e}")

print(f"\n{DIV}")
print("INTERPRETATION")
print(DIV)
print("""
  Q1 grain_check = 'NO'  → rows are events/line-items, not orders.
                            Fix: deduplicate with DISTINCT ON("Case ID") before AVG.

  Q2/Q3 avg_rows_per_case > 1 → JOINs cause fan-out, distorting all aggregations.
                                  Fix: use subquery to aggregate before joining.

  Q4/Q5 values close to ARIS → grain is fine, just need correct column/formula.
""")
