"""
Test script to verify the cases table integration works
This bypasses the LLM to test the SQL agent's cases table logic directly
"""
from dotenv import load_dotenv
load_dotenv()

from db.query_runner import QueryRunner
from agent.sql_agent import SQLAgent

db = QueryRunner()
agent = SQLAgent(db)

# Test cases table KPI mapping
test_queries = [
    ("What is the average delay days?", {"metric": "avg_delay_duration"}),
    ("Show me delivery on time", {"metric": "delivery on time"}),
    ("What is OTIF?", {"metric": "otif"}),
    ("How many materials?", {"metric": "materials"}),
    ("Orders at risk", {"metric": "orders at risk"}),
]

print("=" * 70)
print("Testing Cases Table Integration")
print("=" * 70)

for query, intent in test_queries:
    print(f"\nQuery: {query}")
    print(f"   Intent: {intent}")

    try:
        # Generate SQL using the agent
        sql = agent.generate(intent, query)
        print(f"\n[OK] Generated SQL:")
        print(sql)

        # Execute the query
        result = db.execute(sql)
        print(f"\n[RESULT]")
        if result:
            for row in result:
                print(f"   KPI: {row.get('kpi_name', 'N/A')}")
                print(f"   Value: {row.get('kpi_value', 'N/A')}")
                print(f"   Date: {row.get('created_at', 'N/A')}")
        else:
            print("   No data found")

    except Exception as e:
        print(f"\n[ERROR] {e}")

    print("-" * 70)
