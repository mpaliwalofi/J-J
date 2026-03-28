import json
import re
import os
import logging
from groq import Groq
from db.query_runner import QueryRunner

logger = logging.getLogger(__name__)
client = Groq(api_key=os.getenv("GROQ_API_KEY"))


SYSTEM_PROMPT = """You are a SQL agent connected to a Supabase PostgreSQL database.

You MUST follow this loop manually:

1. Decide which table to use
2. Write SQL query
3. Return ONLY JSON in this format:

{
  "action": "run_sql",
  "query": "SELECT ..."
}

OR final answer:

{
  "final": true,
  "sql": "...",
  "result": [...],
  "summary": "...",
  "row_count": number
}

Rules:
- Only SELECT queries
- Always LIMIT 500
- Fix errors if SQL fails
- Never hallucinate columns
"""


class SQLAgent:

    def __init__(self, db: QueryRunner, max_iterations: int = 6):
        self.db = db
        self.max_iterations = max_iterations

    def run(self, intent: dict, original_query: str) -> dict:
        logger.info("Agent starting | intent=%s | query=%s", intent.get("intent"), original_query)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"""
User Question: {original_query}

Intent:
{json.dumps(intent, indent=2)}

Available tables:
{self.db.list_tables()}

Now start solving.
"""
            }
        ]

        last_result = None

        for i in range(self.max_iterations):
            logger.info("Iteration %d/%d", i + 1, self.max_iterations)

            response = client.chat.completions.create(
                model="llama3-70b-8192",
                messages=messages,
                max_tokens=2048
            )

            content = response.choices[0].message.content
            logger.info("LLM Response: %s", content[:300])

            # Clean markdown
            content = re.sub(r'^```json', '', content)
            content = re.sub(r'```$', '', content).strip()

            try:
                data = json.loads(content)
            except:
                messages.append({"role": "assistant", "content": content})
                continue

            # ✅ FINAL ANSWER
            if data.get("final"):
                return {
                    "sql": data.get("sql"),
                    "result": data.get("result", []),
                    "summary": data.get("summary", ""),
                    "row_count": data.get("row_count", 0)
                }

            # ✅ RUN SQL
            if data.get("action") == "run_sql":
                query = data.get("query")
                logger.info("Executing SQL: %s", query)

                try:
                    result = self.db.execute(query)
                    last_result = result

                    messages.append({
                        "role": "assistant",
                        "content": content
                    })

                    messages.append({
                        "role": "user",
                        "content": f"SQL RESULT:\n{json.dumps(result, default=str)}"
                    })

                except Exception as e:
                    messages.append({
                        "role": "user",
                        "content": f"SQL ERROR: {str(e)}"
                    })

                continue

        return {
            "error": "Max iterations reached",
            "sql": None,
            "result": last_result or [],
            "summary": "",
            "row_count": 0
        }