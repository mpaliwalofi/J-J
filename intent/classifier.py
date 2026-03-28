import json
import logging
import os
import re
from groq import Groq

logger = logging.getLogger(__name__)
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """You are an intent classifier for a supply chain KPI assistant.

Extract the intent from the user's query and return ONLY a JSON object with no extra text:

{
  "intent": "<one of: kpi_lookup | trend_analysis | comparison | root_cause>",
  "metric": "<snake_case metric name or null>",
  "period": "<one of: this_week | last_week | this_month | last_month | last_30_days | this_quarter | null>",
  "filters": {},
  "confidence": <float 0.0-1.0>
}

Intent definitions:
- kpi_lookup     : asking for a current/latest value of a single KPI
- trend_analysis : asking about changes or trends over time
- comparison     : comparing values across regions, warehouses, or time periods
- root_cause     : asking why something happened or what caused a spike/drop

Common metrics: otif, on_time_delivery, failed_deliveries, warehouse_utilisation,
load_rejection_rate, order_fill_rate, inventory_turnover, transit_time, avg_delay_duration

Filters can include keys like: region, warehouse, product, carrier."""


class IntentClassifier:
    """
    LLM Service — Intent Classifier.
    Converts a raw user query into a structured intent dict for the Agent Layer.
    """

    def classify(self, query: str) -> dict:
        logger.info("Classifying: %s", query)

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": query},
            ],
            max_tokens=256,
        )

        raw = response.choices[0].message.content.strip()

        # Strip markdown fences if present
        raw = re.sub(r"^```json\s*", "", raw, flags=re.MULTILINE)
        raw = re.sub(r"```\s*$", "", raw, flags=re.MULTILINE).strip()

        try:
            intent = json.loads(raw)
        except Exception:
            logger.warning("Could not parse intent JSON: %s", raw[:200])
            intent = {
                "intent": "kpi_lookup",
                "metric": None,
                "period": None,
                "filters": {},
                "confidence": 0.5,
            }

        logger.info("Intent: %s", intent)
        return intent
