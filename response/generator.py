import logging
import os
from groq import Groq

logger = logging.getLogger(__name__)
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """You are a supply chain KPI analyst for Johnson & Johnson.

You are given:
- The user's original question
- Query results from the PostgreSQL database
- KPI definitions for business context ONLY

Rules:
- Cite specific numbers from the query results
- Use KPI definitions only to explain business meaning — ignore any PQL/ARIS column references in them (e.g. "_ARIS.Case.*" or "Delivery DIM_csv.*"), those are tool-specific and not SQL column names
- Flag anomalies or notable trends if visible in the data
- Stay under 3 sentences unless detail is explicitly required
"""


class ResponseGenerator:
    """
    LLM Service — Response Generation.
    Takes Data + Context from the MCP Layer and produces a natural language answer.
    """

    def generate(self, original_query: str, data: list[dict], context: list[dict]) -> str:
        data_section = f"Query Results ({len(data)} rows):\n"
        if data:
            data_section += "\n".join(str(row) for row in data[:20])
        else:
            data_section += "No rows returned."

        context_section = ""
        if context:
            lines = []
            for c in context:
                name = c.get("KPI Name") or c.get("name", "")
                defn = c.get("Definition") or c.get("definition", "")
                lines.append(f"- {name}: {defn}")
            context_section = "\nKPI Context:\n" + "\n".join(lines)

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Question: {original_query}\n\n"
                        f"{data_section}"
                        f"{context_section}"
                    ),
                },
            ],
            max_tokens=512,
        )
        return response.choices[0].message.content.strip()
