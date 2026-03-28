# app.py
#
# ENTRY POINT.
# Takes a structured intent dict (from your classifier)
# and returns data + SQL + summary from Supabase.
#
# HOW TO RUN:
#   python app.py
#   python app.py "What is OTIF this week?"

import os
import json
import logging
from dotenv import load_dotenv

load_dotenv()  # reads .env FIRST — must be before any other import

from db.query_runner import QueryRunner
from agent.sql_agent import SQLAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── Create once at startup ────────────────────────────────────────────────────
db    = QueryRunner()   # connects to Supabase using DATABASE_URL from .env
agent = SQLAgent(db)


# ── Main function ─────────────────────────────────────────────────────────────

def handle_intent(intent: dict, original_query: str) -> dict:
    """
    Call this from your classifier pipeline.

    Args:
        intent        : structured intent dict from BERT classifier
                        e.g. {
                          "intent":      "kpi_lookup",
                          "metric":      "otif",
                          "period":      "this_week",
                          "filters":     {"region": "North"},
                          "confidence":  0.94
                        }
        original_query: raw user question string

    Returns:
        {
          "answer":    "OTIF this week is 87.4%. North region leads at 91%.",
          "sql":       "SELECT ...",
          "result":    [ row dicts ],
          "row_count": 4,
          "success":   True
        }
    """
    logger.info("Handling intent: %s | query: %s", intent.get("intent"), original_query)

    result = agent.run(intent, original_query)

    return {
        "answer":    result.get("summary", "No answer generated."),
        "sql":       result.get("sql"),
        "result":    result.get("result", []),
        "row_count": result.get("row_count", 0),
        "success":   "error" not in result,
        "error":     result.get("error")
    }


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    # Test intents that simulate what your BERT classifier will output
    test_cases = [
        {
            "query": "What is the OTIF rate this week?",
            "intent": {
                "intent": "kpi_lookup",
                "metric": "otif",
                "period": "this_week",
                "filters": {},
                "confidence": 0.95
            }
        },
        {
            "query": "Show me failed deliveries trend over last 30 days",
            "intent": {
                "intent": "trend_analysis",
                "metric": "failed_deliveries",
                "period": "last_30_days",
                "filters": {},
                "confidence": 0.91
            }
        },
        {
            "query": "Compare warehouse utilisation between Mumbai and Delhi",
            "intent": {
                "intent": "comparison",
                "metric": "warehouse_utilisation",
                "period": None,
                "filters": {"warehouses": ["Mumbai", "Delhi"]},
                "confidence": 0.88
            }
        },
        {
            "query": "Why did load rejection rate spike this week?",
            "intent": {
                "intent": "root_cause",
                "metric": "rejection_rate",
                "period": "this_week",
                "filters": {},
                "confidence": 0.87
            }
        },
    ]

    # If query passed on command line, run just that one with a generic intent
    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
        test_cases = [{
            "query": q,
            "intent": {"intent": "kpi_lookup", "metric": None, "period": None, "filters": {}, "confidence": 0.8}
        }]

    for case in test_cases:
        print("\n" + "=" * 60)
        print(f"Q: {case['query']}")
        print(f"   Intent: {case['intent']['intent']} | metric: {case['intent'].get('metric')}")

        output = handle_intent(case["intent"], case["query"])

        print(f"A: {output['answer']}")
        if output.get("sql"):
            print(f"   SQL: {output['sql'][:120]}...")
        print(f"   Rows: {output['row_count']} | Success: {output['success']}")
        if output.get("error"):
            print(f"   Error: {output['error']}")