import json
import re
import os
import logging
from groq import Groq
from db.query_runner import QueryRunner

logger = logging.getLogger(__name__)
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """You are an intelligent SQL query generator for a supply chain PostgreSQL database.

Your job: Convert ANY natural language question into a valid SQL SELECT query that answers the question using the available database schema.

Return ONLY JSON — no explanation, no markdown:

To produce a query:
{"action": "generate_sql", "query": "SELECT ..."}

If you cannot generate a valid query:
{"action": "error", "reason": "..."}

CRITICAL — How to Handle Different Question Types:

1. **KPI/Metric Questions** (e.g. "What is OTIF in 2024?", "warehouse utilization last month")
   - Use aggregation (AVG, SUM, COUNT) on the relevant KPI column
   - Filter by the specified time period
   - Example: SELECT AVG(CAST("Daily Shift Capacity Utilization (%)" AS numeric)) FROM "Supply_Chain_KPI_Tuned" WHERE "Warehouse_Record_Date" LIKE '2024%'

2. **Categorical/List Questions** (e.g. "give me reasons for delays", "material types in warehouse")
   - Query the relevant categorical column with GROUP BY and COUNT to show frequency
   - Order by count descending to show most common first
   - Example: SELECT "Delay Reason for Delivery", COUNT(*) as count FROM "Supply_Chain_KPI_Tuned" WHERE "Delay Reason for Delivery" IS NOT NULL GROUP BY "Delay Reason for Delivery" ORDER BY count DESC LIMIT 50

3. **Exploratory Questions** (e.g. "show me deliveries", "what data do you have?")
   - Select relevant columns (NOT *) from the most appropriate table
   - ALWAYS use LIMIT 50 for exploratory queries
   - Example: SELECT "Case ID", "Source City", "Delivery_Date" FROM "Delivery_Dim" LIMIT 50

4. **Comparison Questions** (e.g. "compare Mumbai and Delhi deliveries")
   - Use GROUP BY on the comparison dimension
   - Include relevant metrics for each group
   - Example: SELECT "Source City", COUNT(*) FROM "Delivery_Dim" WHERE "Source City" IN ('Mumbai', 'Delhi') GROUP BY "Source City"

5. **Trend/Time-Series Questions** (e.g. "show OTIF by month in 2024")
   - SELECT the date column AND the metric
   - GROUP BY the date column
   - ORDER BY date
   - Example: SELECT "Warehouse_Record_Date", AVG(CAST("Daily Shift Capacity Utilization (%)" AS numeric)) FROM "Supply_Chain_KPI_Tuned" WHERE "Warehouse_Record_Date" LIKE '2024%' GROUP BY "Warehouse_Record_Date" ORDER BY "Warehouse_Record_Date"

CRITICAL — Common Columns by Question Type:
- Delay reasons: "Delay Reason for Delivery" (Supply_Chain_KPI_Tuned/Supply_Chain_KPI_Single_Sheet)
- Warehouse issues: "Warehouse Issue" (Supply_Chain_KPI_Single_Sheet)
- Route disruptions: "Delivery Impacted by Route Disruptions" (Supply_Chain_KPI_Tuned)
- Material types: Check schema for material/product columns in Warehouse_DIM
- Cities: "Source City", "Destination City" (Delivery_Dim)
- Regions: "Region of the Country" (Supply_Chain_KPI_Tuned)

CRITICAL — If Intent is Missing Metric/Period:
- Read the user's original question carefully
- Infer what they're asking for based on keywords
- Choose the most relevant table and columns from the schema
- Generate a sensible query that attempts to answer their question
- Do NOT return an error unless the question is truly impossible to answer with the available schema

Rules:
- Only SELECT queries (no INSERT, UPDATE, DELETE, etc.)
- CRITICAL — ALWAYS include LIMIT clause:
  * For aggregated queries (with GROUP BY, or single-row results): LIMIT 100
  * For raw data queries: LIMIT 50
  * NEVER exceed LIMIT 100 under any circumstances
- CRITICAL — Prefer aggregation to reduce data size:
  * Use AVG, SUM, COUNT, MIN, MAX whenever the question asks for totals, averages, or rates
  * Use GROUP BY for comparisons, breakdowns, or trends
  * Only return raw rows when explicitly asked to "show", "list", or "display" individual records
- CRITICAL: Column names are case-sensitive in PostgreSQL. Always wrap EVERY column name in double quotes exactly as shown in the schema. Example: "Delivery_Dim"."Delivery_Date", "Sales_Order_DIM"."Case ID"
- CRITICAL: Copy EVERY column name CHARACTER-FOR-CHARACTER from the schema provided. Never shorten, abbreviate, or paraphrase column names. For example, if the schema shows "Region of the Country", you MUST write "Region of the Country" — never "Region" or "region". If the schema shows "Daily Shift Capacity Utilization (%)", you MUST write "Daily Shift Capacity Utilization (%)" — never "Capacity_Utilization" or any other variant.
- CRITICAL: Before finalising the query, verify each column name you used exists EXACTLY in the schema. If you are unsure, do not guess — return {"action": "error", "reason": "column not found in schema"}.
- Do not hallucinate tables or columns. Only use tables and columns that appear in the schema below.
- CRITICAL: When filtering by date/period, use ONLY the designated date column for each table — never borrow a date column from a different table:
    * Supply_Chain_KPI_Tuned      → "Warehouse_Record_Date" (stored as text, e.g. '2025-01'). Filter with: "Warehouse_Record_Date" LIKE '2025-01%' or TO_DATE("Warehouse_Record_Date", 'YYYY-MM-DD')
    * Supply_Chain_KPI_Single_Sheet     → no date column; join to Delivery_Dim on "Case ID" and filter by "Delivery_Dim"."Delivery_Date"
    * Delivery_Dim   → "Delivery_Date" (timestamp)
    * Warehouse_DIM  → "POSTING_DATE" (date)
    * Sales_Order_DIM → "Sales Order Date" (timestamp)
    * Invoice_Dim    → "Invoice Creation Date" (timestamp)
    * PO_DIM         → "Purchase Order Date" (timestamp)
- When filtering by date, always double-quote the column: EXTRACT(YEAR FROM "Delivery_Date") not Delivery_Date
- CRITICAL: Use aggregation (SUM, AVG, COUNT, MAX, MIN) when the question asks for a total, average, count, rate, or percentage — do NOT return raw rows. Examples:
  - "total savings lost" → SELECT SUM("Savings Lost (2025 Projection)") AS total FROM "Supply_Chain_KPI_Tuned"
  - "average lead time" → SELECT AVG("Total Lead Time (Days)") AS avg_lead_time FROM "Supply_Chain_KPI_Tuned"
  - "how many deliveries" → SELECT COUNT(*) AS count FROM "Delivery_Dim"
  - "delivery rate/percentage" → SELECT COUNT(*) FILTER (WHERE ...) * 100.0 / COUNT(*) AS rate FROM ...
- Only return raw rows when the question asks to "show", "list", or "display" individual records
- When aggregating a column that may be stored as text, always cast it: AVG(CAST("Column Name" AS numeric))
- CRITICAL — "by date" / "trend" / "over time" / "show by" queries: always SELECT the date column AND the metric together, then GROUP BY the date column. Never collapse into a single aggregate row. Example: SELECT "Warehouse_Record_Date", AVG(CAST("Daily Shift Capacity Utilization (%)" AS numeric)) AS avg_utilisation FROM "Supply_Chain_KPI_Tuned" WHERE "Warehouse_Record_Date" LIKE '2024%' GROUP BY "Warehouse_Record_Date" ORDER BY "Warehouse_Record_Date" LIMIT 100
- CRITICAL — Warehouse utilisation metric: always use "Daily Shift Capacity Utilization (%)" from Supply_Chain_KPI_Tuned. Never substitute "Utilization Efficiency (%)" for warehouse utilisation queries.
- CRITICAL — City vs Region distinction:
    * "Supply_Chain_KPI_Tuned"."Region of the Country" contains broad regions: 'North', 'South', 'East', 'West', 'Central'. Use this column ONLY when the user mentions region names like North/South/East/West.
    * City names (Mumbai, Delhi, Bangalore, Chennai, Hyderabad, Kolkata, Pune) are stored in "Delivery_Dim"."Source City" and "Delivery_Dim"."Destination City". NEVER filter "Supply_Chain_KPI_Tuned"."Region of the Country" with city names — it will return 0 rows.
    * For warehouse utilisation by CITY: join "Supply_Chain_KPI_Tuned" with "Delivery_Dim" on "Case ID", then filter by "Delivery_Dim"."Source City" IN ('Mumbai', 'Delhi') and group by "Delivery_Dim"."Source City".
    * The intent dict will include a "cities" list and a "regions" list — use the correct column based on which list is populated.
- CRITICAL — Period format for "Supply_Chain_KPI_Tuned"."Warehouse_Record_Date":
    * The period in the intent will be in format "YYYY-MM" (e.g. "2024-12" for December 2024) or "YYYY" (e.g. "2024").
    * For "YYYY-MM": filter with "Warehouse_Record_Date" LIKE '2024-12%'
    * For "YYYY": filter with "Warehouse_Record_Date" LIKE '2024%'
    * Never use EXTRACT() or TO_DATE() on "Warehouse_Record_Date" — it is a text column.
"""


class SQLAgent:
    """
    Agent Layer — SQL/PQL Agent.
    Generates SQL from a structured intent. Does NOT execute queries;
    execution is handled by the MCP Layer (QueryRouter).
    """

    def __init__(self, db: QueryRunner, max_iterations: int = 3):
        self.db = db
        self.max_iterations = max_iterations

    def _build_schema_context(self) -> str:
        tables = self.db.list_tables()
        parts = []
        for table in tables[:10]:
            schema = self.db.get_schema(table)
            col_lines = []
            for col, dtype in schema.items():
                # Column names with spaces must be double-quoted in SQL
                safe = f'"{col}"' if " " in col else col
                col_lines.append(f"{safe} ({dtype})")
            parts.append(f"Table: {table}\n  Columns: {', '.join(col_lines)}\n  Note: use double-quoted names exactly as shown above")
        return "\n\n".join(parts)

    def generate(self, intent: dict, original_query: str) -> str:
        """
        Generate a SQL query string from structured intent.
        Raises RuntimeError if no valid SQL can be produced.
        """
        schema_context = self._build_schema_context()

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"User Question: {original_query}\n\n"
                    f"Intent:\n{json.dumps(intent, indent=2)}\n\n"
                    f"Database Schema:\n{schema_context}\n\n"
                    "Generate the SQL query."
                ),
            },
        ]

        for i in range(self.max_iterations):
            logger.info("SQL generation attempt %d/%d", i + 1, self.max_iterations)

            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=messages,
                max_tokens=1024,
            )
            content = response.choices[0].message.content.strip()
            logger.info("Agent response: %s", content[:200])

            content = re.sub(r"^```json\s*", "", content, flags=re.MULTILINE)
            content = re.sub(r"```\s*$", "", content, flags=re.MULTILINE).strip()

            try:
                data = json.loads(content)
            except Exception:
                messages.append({"role": "assistant", "content": content})
                messages.append({"role": "user", "content": "Return valid JSON only."})
                continue

            if data.get("action") == "generate_sql":
                sql = data["query"]
                logger.info("Generated SQL: %s", sql)
                return sql

            if data.get("action") == "error":
                raise ValueError(f"Agent could not generate SQL: {data.get('reason')}")

        raise RuntimeError("SQL Agent failed to produce a query after max iterations")

    def refine(self, failed_sql: str, error: str, intent: dict, original_query: str) -> str:
        """
        Refine a SQL query after an execution error returned by the MCP Layer.
        Raises RuntimeError if refinement fails.
        """
        logger.info("Refining SQL after error: %s", error[:120])

        schema_context = self._build_schema_context()

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"The following SQL failed execution:\n\n"
                    f"SQL: {failed_sql}\n"
                    f"Error: {error}\n\n"
                    f"Original question: {original_query}\n"
                    f"Intent: {json.dumps(intent, indent=2)}\n\n"
                    f"Database Schema (use ONLY these tables and columns):\n{schema_context}\n\n"
                    'Fix the SQL and return {"action": "generate_sql", "query": "..."}'
                ),
            },
        ]

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=messages,
            max_tokens=1024,
        )
        content = response.choices[0].message.content.strip()
        content = re.sub(r"^```json\s*", "", content, flags=re.MULTILINE)
        content = re.sub(r"```\s*$", "", content, flags=re.MULTILINE).strip()

        try:
            data = json.loads(content)
            sql = data["query"]
            logger.info("Refined SQL: %s", sql)
            return sql
        except Exception:
            raise RuntimeError(f"Agent failed to refine SQL: {content[:200]}")
