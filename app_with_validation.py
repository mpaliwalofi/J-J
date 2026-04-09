# app_with_validation.py
#
# ENHANCED ENTRY POINT — AI KPI Assistant with Real-Time ARIS Validation
#
# This version shows validation scores alongside every answer
#
# HOW TO RUN:
#   python app_with_validation.py
#   python app_with_validation.py "What is OTIF this week?"

import sys
import logging
import csv
from pathlib import Path
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

# ── Load ARIS Ground Truth ───────────────────────────────────────────────────

ARIS_GROUND_TRUTH = {}

def load_aris_ground_truth():
    """Load ARIS reference data from CSV for validation."""
    global ARIS_GROUND_TRUTH

    csv_path = Path(__file__).parent / "tests" / "aris_reference_data" / "ground_truth.csv"

    if not csv_path.exists():
        logger.warning("ARIS ground truth CSV not found - validation disabled")
        return

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            query_lower = row['query'].lower().strip()
            ARIS_GROUND_TRUTH[query_lower] = {
                "test_case_id": row['test_case_id'],
                "expected_value": float(row['expected_value']) if row.get('expected_value') else None,
                "tolerance": float(row.get('tolerance', 0.5)) if row.get('tolerance') else 0.5,
                "unit": row.get('unit', ''),
                "source": row.get('source', ''),
            }

    logger.info(f"Loaded {len(ARIS_GROUND_TRUTH)} ARIS ground truth entries for validation")


# ── Validation Function ───────────────────────────────────────────────────────

def validate_against_aris(query: str, result: dict) -> dict:
    """
    Validate query result against ARIS ground truth if available.

    Returns:
        {
            "validated": bool,          # True if validation was performed
            "passed": bool,             # True if result matches ARIS
            "aris_value": float,        # Expected value from ARIS
            "ai_value": float,          # Actual value from AI
            "difference": float,        # Absolute difference
            "percentage_diff": float,   # Percentage difference
            "tolerance": float,         # Allowed tolerance
            "source": str,              # ARIS dashboard source
            "match_emoji": str          # ✅ or ❌
        }
    """
    query_lower = query.lower().strip()

    # Check if we have ground truth for this query
    if query_lower not in ARIS_GROUND_TRUTH:
        return {"validated": False}

    ground_truth = ARIS_GROUND_TRUTH[query_lower]

    # Can only validate numeric queries
    if ground_truth["expected_value"] is None:
        return {"validated": False}

    # Extract numeric value from result
    try:
        data = result.get("result", [])
        if not data:
            return {"validated": False}

        # Find first numeric value
        ai_value = None
        for row in data:
            for key, value in row.items():
                if isinstance(value, (int, float)):
                    ai_value = float(value)
                    break
                if isinstance(value, str):
                    try:
                        ai_value = float(value.replace('%', '').replace(',', '').strip())
                        break
                    except ValueError:
                        continue
            if ai_value is not None:
                break

        if ai_value is None:
            return {"validated": False}

    except Exception as e:
        logger.debug(f"Validation extraction error: {e}")
        return {"validated": False}

    # Calculate difference
    aris_value = ground_truth["expected_value"]
    difference = abs(ai_value - aris_value)
    percentage_diff = abs((ai_value - aris_value) / aris_value * 100) if aris_value != 0 else difference
    tolerance = ground_truth["tolerance"]
    passed = difference <= tolerance

    return {
        "validated": True,
        "passed": passed,
        "aris_value": aris_value,
        "ai_value": ai_value,
        "difference": difference,
        "percentage_diff": percentage_diff,
        "tolerance": tolerance,
        "source": ground_truth["source"],
        "match_emoji": "✅" if passed else "❌",
        "unit": ground_truth.get("unit", ""),
    }


# ── Primary entry point with validation ───────────────────────────────────────

def handle_query_with_validation(query: str) -> dict:
    """
    End-to-end pipeline: raw natural language query → final answer with ARIS validation.

    Returns:
        {
          "answer":     str,
          "sql":        str | None,
          "result":     list[dict],
          "row_count":  int,
          "success":    bool,
          "error":      str | None,
          "validation": dict  # ARIS validation results
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
                        "answer":     "Could not retrieve data after multiple attempts.",
                        "sql":        sql,
                        "result":     [],
                        "row_count":  0,
                        "success":    False,
                        "error":      last_error,
                        "validation": {"validated": False}
                    }

        # 3. LLM Service — Response Generation
        answer = responder.generate(query, mcp_result["data"], mcp_result["context"])

        result = {
            "answer":    answer,
            "sql":       mcp_result["sql"],
            "result":    mcp_result["data"],
            "row_count": mcp_result["row_count"],
            "success":   True,
            "error":     None,
        }

        # 4. Validate against ARIS ground truth
        validation = validate_against_aris(query, result)
        result["validation"] = validation

        return result

    except Exception as e:
        logger.error("Pipeline error: %s", e, exc_info=True)
        return {
            "answer":     "An unexpected error occurred.",
            "sql":        None,
            "result":     [],
            "row_count":  0,
            "success":    False,
            "error":      str(e),
            "validation": {"validated": False}
        }


# ── CLI with validation display ───────────────────────────────────────────────

def _run_one_with_validation(query: str) -> None:
    """Pre-check classifier, run pipeline, and display result with validation."""
    try:
        classifier.classify(query)
    except ValueError as e:
        print(f"\n{e}\n")
        return
    except Exception as e:
        print(f"\nClassifier error: {e}\n")
        return

    result = handle_query_with_validation(query)
    print()

    # Display answer
    if result["success"]:
        print(result["answer"])
    else:
        print(f"Error: {result['error']}")

    # Display validation if available
    validation = result.get("validation", {})
    if validation.get("validated"):
        print("\n" + "─" * 60)
        print(f"{validation['match_emoji']} ARIS VALIDATION")
        print("─" * 60)
        print(f"ARIS Dashboard:  {validation['aris_value']:.2f} {validation.get('unit', '')}")
        print(f"AI System:       {validation['ai_value']:.2f} {validation.get('unit', '')}")
        print(f"Difference:      {validation['difference']:.2f} (tolerance: ±{validation['tolerance']})")
        print(f"Match:           {'PASS' if validation['passed'] else 'FAIL'}")
        print(f"Source:          {validation['source']}")
        print("─" * 60)

    print()


if __name__ == "__main__":
    # Load ARIS ground truth on startup
    load_aris_ground_truth()

    # One-shot mode: python app_with_validation.py "What is OTIF this week?"
    if len(sys.argv) > 1:
        _run_one_with_validation(" ".join(sys.argv[1:]))
        sys.exit(0)

    # Interactive mode
    print("\n" + "="*60)
    print("J&J KPI Assistant with ARIS Validation")
    print("="*60)
    if ARIS_GROUND_TRUTH:
        print(f"✅ Loaded {len(ARIS_GROUND_TRUTH)} validation queries from ARIS")
    else:
        print("⚠️  No ARIS ground truth loaded - validation disabled")
    print("="*60)
    print("Type 'quit' or 'exit' to stop\n")

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

            _run_one_with_validation(query)

    except KeyboardInterrupt:
        print("\nGoodbye.")
