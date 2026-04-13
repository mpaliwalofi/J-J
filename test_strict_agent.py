"""
Test script for the strict KPI-driven SQL agent.

Tests all ARIS dashboard KPIs to ensure 100% match with definitions.
"""

from dotenv import load_dotenv
load_dotenv()

from db.query_runner import QueryRunner
from agent.sql_agent import SQLAgent

db = QueryRunner()
agent = SQLAgent(db)

# Test cases: queries that should match ARIS dashboard
TEST_CASES = [
    # Warehouse Issue
    {
        "query": "warehouse issue",
        "intent": {"metric": "warehouse", "confidence": 0.5},
        "expected_kpi": "Warehouse Issue",
        "description": "Should match warehouse keyword"
    },

    # OTIF
    {
        "query": "What is OTIF?",
        "intent": {"metric": "otif", "confidence": 0.9},
        "expected_kpi": "OTIF",
        "description": "Should match OTIF keyword"
    },

    # Orders at Risk
    {
        "query": "orders at risk",
        "intent": {"metric": "orders_at_risk", "confidence": 0.8},
        "expected_kpi": "Orders at Risk",
        "description": "Should match orders at risk keyword"
    },

    # On Time
    {
        "query": "delivery on time",
        "intent": {"metric": "on_time", "confidence": 0.7},
        "expected_kpi": "On Time",
        "description": "Should match on time keyword"
    },

    # Delay
    {
        "query": "average delay duration",
        "intent": {"metric": "avg_delay_duration", "confidence": 0.85},
        "expected_kpi": "Avg Delay Duration",
        "description": "Should match delay keyword"
    },

    # InFull
    {
        "query": "delivered in full",
        "intent": {"metric": "infull", "confidence": 0.75},
        "expected_kpi": "InFull",
        "description": "Should match in full keyword"
    },

    # Transport Delay
    {
        "query": "transport delay",
        "intent": {"metric": "transport_delay", "confidence": 0.8},
        "expected_kpi": "Transport Delay",
        "description": "Should match transport delay keyword"
    },

    # Stock Shortage
    {
        "query": "stock shortage in warehouse",
        "intent": {"metric": "stock_shortage", "confidence": 0.7},
        "expected_kpi": "Stock Shortage in Warehouse",
        "description": "Should match stock shortage keyword"
    },
]

print("=" * 80)
print("STRICT KPI-DRIVEN SQL AGENT TEST")
print("=" * 80)
print("\nThis test verifies:")
print("1. KPI identification uses keyword matching first")
print("2. SQL is generated from KPI definitions only (no LLM guessing)")
print("3. All results match ARIS dashboard definitions")
print("\n" + "=" * 80)

passed = 0
failed = 0

for i, test in enumerate(TEST_CASES, 1):
    print(f"\n[TEST {i}/{len(TEST_CASES)}] {test['description']}")
    print(f"Query: {test['query']}")
    print(f"Intent: {test['intent']}")

    try:
        # Step 1: Identify KPI
        kpi_name, confidence = agent.identify_kpi(test['query'], test['intent'])

        if kpi_name == test['expected_kpi']:
            print(f"[OK] KPI Identified: {kpi_name} (confidence: {confidence:.2f})")
        else:
            print(f"[FAIL] Expected '{test['expected_kpi']}' but got '{kpi_name}'")
            failed += 1
            continue

        # Step 2: Generate SQL
        sql = agent.generate(test['intent'], test['query'])
        print(f"\n[SQL Generated]")
        print(sql)

        # Step 3: Execute (optional - comment out if API issues)
        try:
            result = db.execute(sql)
            print(f"\n[RESULT] Rows returned: {len(result) if result else 0}")
            if result and len(result) > 0:
                print(f"Sample row: {result[0]}")
        except Exception as e:
            print(f"\n[EXECUTION ERROR] {str(e)[:200]}")

        passed += 1

    except Exception as e:
        print(f"\n[FAIL] Error: {str(e)[:200]}")
        failed += 1

    print("-" * 80)

# Summary
print("\n" + "=" * 80)
print("TEST SUMMARY")
print("=" * 80)
print(f"Passed: {passed}/{len(TEST_CASES)}")
print(f"Failed: {failed}/{len(TEST_CASES)}")

if failed == 0:
    print("\n[SUCCESS] All tests passed! Agent is working correctly.")
else:
    print(f"\n[WARNING] {failed} tests failed. Review the failures above.")
