import json
import re
import os
import logging
from groq import Groq
from db.query_runner import QueryRunner

logger = logging.getLogger(__name__)
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """You are a SQL query generator for a supply chain PostgreSQL database.

Given a structured intent and database schema, generate a single valid SQL SELECT query.

Return ONLY JSON — no explanation, no markdown:

To produce a query:
{"action": "generate_sql", "query": "SELECT ..."}

If you cannot generate a valid query:
{"action": "error", "reason": "..."}

Rules:
- Only SELECT queries (no INSERT, UPDATE, DELETE, etc.)
- Always include LIMIT 500
- CRITICAL: Column names are case-sensitive in PostgreSQL. Always wrap EVERY column name in double quotes exactly as shown in the schema. Example: delivery_dim."Delivery_Date", sales_order_dim."Case ID"
- Do not hallucinate tables or columns
- When filtering by date, always double-quote the column: EXTRACT(YEAR FROM "Delivery_Date") not Delivery_Date
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
                model="llama-3.3-70b-versatile",
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
                logger.info("Generated SQL: %s", sql[:120])
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
            logger.info("Refined SQL: %s", sql[:120])
            return sql
        except Exception:
            raise RuntimeError(f"Agent failed to refine SQL: {content[:200]}")
