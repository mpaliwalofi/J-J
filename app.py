# app.py
#
# ENTRY POINT — AI KPI Assistant
#
# Full pipeline:
#   User query
#     → LLM Service: Intent Classifier       (intent/classifier.py)
#     → Agent Layer: SQL/PQL Agent            (agent/sql_agent.py)
#     → MCP Layer:   Query Validator & Router (mcp/router.py)
#         ├── PostgreSQL: Structured Data      (db/query_runner.py)
#         └── pgvector:   Embeddings           (db/vector_store.py)
#     → LLM Service: Response Generation      (response/generator.py)
#     → Final Answer
#
# HOW TO RUN:
#   python app.py
#   python app.py "What is OTIF this week?"

import sys
import logging
from dotenv import load_dotenv

load_dotenv()  # must be before any module that reads env vars

from db.query_runner import QueryRunner
from db.vector_store import VectorStore
from intent.classifier import IntentClassifier
from agent.sql_agent import SQLAgent
from mcp.router import QueryRouter
from response.generator import ResponseGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Initialise all layers once at startup ────────────────────────────────────
db           = QueryRunner()
vector_store = VectorStore(db)
classifier   = IntentClassifier()
agent        = SQLAgent(db)
router       = QueryRouter(db, vector_store)
responder    = ResponseGenerator()

MAX_RETRIES = 3


# ── Primary entry point ───────────────────────────────────────────────────────

def handle_query(query: str) -> dict:
    """
    End-to-end pipeline: raw natural language query → final answer.

    Returns:
        {
          "answer":    str,
          "sql":       str | None,
          "result":    list[dict],
          "row_count": int,
          "success":   bool,
          "error":     str | None
        }
    """
    logger.info("=== Pipeline start | query: %s", query)

    try:
        # 1. LLM Service — Intent Classifier
        intent = classifier.classify(query)

        # 2. Agent Layer + MCP Layer (with retry loop)
        sql = None
        mcp_result = None
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                # Agent Layer: generate SQL from intent
                if sql is None:
                    sql = agent.generate(intent, query)
                else:
                    sql = agent.refine(sql, last_error, intent, query)

                # MCP Layer: validate + route to PostgreSQL & pgvector
                mcp_result = router.route(sql, query)
                break  # success

            except (PermissionError, ValueError) as e:
                # Validation or agent errors — do not retry
                raise
            except RuntimeError as e:
                last_error = str(e)
                logger.warning("Attempt %d/%d failed: %s", attempt + 1, MAX_RETRIES, last_error)
                if attempt == MAX_RETRIES - 1:
                    return {
                        "answer":    "Could not retrieve data after multiple attempts.",
                        "sql":       sql,
                        "result":    [],
                        "row_count": 0,
                        "success":   False,
                        "error":     last_error,
                    }

        # 3. LLM Service — Response Generation
        answer = responder.generate(query, mcp_result["data"], mcp_result["context"])

        return {
            "answer":    answer,
            "sql":       mcp_result["sql"],
            "result":    mcp_result["data"],
            "row_count": mcp_result["row_count"],
            "success":   True,
            "error":     None,
        }

    except Exception as e:
        logger.error("Pipeline error: %s", e, exc_info=True)
        return {
            "answer":    "An unexpected error occurred.",
            "sql":       None,
            "result":    [],
            "row_count": 0,
            "success":   False,
            "error":     str(e),
        }


# ── CLI ───────────────────────────────────────────────────────────────────────

def _run_one(query: str) -> None:
    """Pre-check classifier, then run full pipeline and print result."""
    try:
        classifier.classify(query)
    except ValueError as e:
        print(f"\n{e}\n")
        return
    except Exception as e:
        print(f"\nClassifier error: {e}\n")
        return

    result = handle_query(query)
    print()
    if result["success"]:
        print(result["answer"])
    else:
        print(f"Error: {result['error']}")
    print()


if __name__ == "__main__":
    # One-shot mode: python app.py "What is OTIF this week?"
    if len(sys.argv) > 1:
        _run_one(" ".join(sys.argv[1:]))
        sys.exit(0)

    # Interactive mode
    print("\nJ&J KPI Assistant  (type 'quit' or 'exit' to stop)\n")
    try:
        while True:
            try:
                query = input("You: ").strip()
            except EOFError:
                print("\nGoodbye.")
                break

            if not query:
                print("Please type a question.\n")
                continue

            if query.lower() in ("quit", "exit"):
                print("Goodbye.")
                break

            _run_one(query)

    except KeyboardInterrupt:
        print("\nGoodbye.")




    

