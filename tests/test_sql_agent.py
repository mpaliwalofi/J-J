# tests/test_sql_agent.py
#
# Unit tests for agent/sql_agent.py
# DB calls are mocked — no live Supabase connection needed.

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent.sql_agent import SQLAgent


SAMPLE_INTENT = {
    "intent": "comparison",
    "metric": "warehouse_utilisation",
    "period": "2024-12",
    "filters": {"cities": ["Mumbai", "Delhi"]},
    "cities": ["Mumbai", "Delhi"],
    "regions": [],
    "confidence": 0.81,
}

SAMPLE_SCHEMA = (
    'Table: Supply_Chain_KPI_Tuned\n'
    '  Columns: "Case ID" (text), "Daily Shift Capacity Utilization (%)" (double precision), '
    '"Region of the Country" (text), "Warehouse_Record_Date" (text)\n'
    '  Note: use double-quoted names exactly as shown above\n\n'
    'Table: Delivery_Dim\n'
    '  Columns: "Case ID" (text), "Source City" (text), "Delivery_Date" (timestamp with time zone)\n'
    '  Note: use double-quoted names exactly as shown above'
)


def _make_agent():
    """Return an SQLAgent with a mocked QueryRunner."""
    mock_db = MagicMock()
    mock_db.list_tables.return_value = ["Supply_Chain_KPI_Tuned", "Delivery_Dim"]
    mock_db.get_schema.side_effect = lambda table: {
        "Supply_Chain_KPI_Tuned": {
            "Case ID": "text",
            "Daily Shift Capacity Utilization (%)": "double precision",
            "Region of the Country": "text",
            "Warehouse_Record_Date": "text",
        },
        "Delivery_Dim": {
            "Case ID": "text",
            "Source City": "text",
            "Delivery_Date": "timestamp with time zone",
        },
    }.get(table, {})
    return SQLAgent(db=mock_db)


class TestSQLAgentGenerate:

    def test_generate_returns_select_string(self):
        agent = _make_agent()
        expected_sql = (
            'SELECT "Delivery_Dim"."Source City", AVG("Supply_Chain_KPI_Tuned"."Daily Shift Capacity Utilization (%)") '
            'AS avg_utilisation FROM "Supply_Chain_KPI_Tuned" '
            'JOIN "Delivery_Dim" ON "Supply_Chain_KPI_Tuned"."Case ID" = "Delivery_Dim"."Case ID" '
            'WHERE "Delivery_Dim"."Source City" IN (\'Mumbai\', \'Delhi\') '
            'AND "Supply_Chain_KPI_Tuned"."Warehouse_Record_Date" LIKE \'2024-12%\' '
            'GROUP BY "Delivery_Dim"."Source City" LIMIT 500'
        )
        mock_response = MagicMock()
        mock_response.choices[0].message.content = (
            '{"action": "generate_sql", "query": "' + expected_sql.replace('"', '\\"') + '"}'
        )

        with patch("agent.sql_agent.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            sql = agent.generate(SAMPLE_INTENT, "Compare warehouse utilisation between Mumbai and Delhi in December 2024")

        assert sql.strip().upper().startswith("SELECT")

    def test_generate_raises_on_agent_error(self):
        agent = _make_agent()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '{"action": "error", "reason": "column not found"}'

        with patch("agent.sql_agent.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            with pytest.raises(ValueError, match="column not found"):
                agent.generate(SAMPLE_INTENT, "some query")

    def test_generate_raises_runtime_after_max_iterations(self):
        agent = _make_agent()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "not json at all"

        with patch("agent.sql_agent.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            with pytest.raises(RuntimeError, match="max iterations"):
                agent.generate(SAMPLE_INTENT, "some query")

    def test_generate_strips_markdown_fences(self):
        agent = _make_agent()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = (
            "```json\n{\"action\": \"generate_sql\", \"query\": \"SELECT 1\"}\n```"
        )

        with patch("agent.sql_agent.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            sql = agent.generate(SAMPLE_INTENT, "some query")

        assert sql == "SELECT 1"


class TestSQLAgentRefine:

    def test_refine_includes_schema_in_prompt(self):
        agent = _make_agent()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '{"action": "generate_sql", "query": "SELECT 1"}'

        with patch("agent.sql_agent.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            agent.refine(
                failed_sql='SELECT "Region" FROM "Supply_Chain_KPI_Tuned"',
                error='column "Region" does not exist',
                intent=SAMPLE_INTENT,
                original_query="Compare warehouse utilisation in December 2024",
            )
            call_args = mock_client.chat.completions.create.call_args
            user_message = call_args[1]["messages"][1]["content"]

        assert "Database Schema" in user_message
        assert "Supply_Chain_KPI_Tuned" in user_message

    def test_refine_raises_on_bad_json(self):
        agent = _make_agent()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "I cannot fix this."

        with patch("agent.sql_agent.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            with pytest.raises(RuntimeError, match="failed to refine"):
                agent.refine(
                    failed_sql="SELECT bad FROM nowhere",
                    error="column does not exist",
                    intent=SAMPLE_INTENT,
                    original_query="test",
                )

    def test_refine_returns_fixed_sql(self):
        agent = _make_agent()
        fixed_sql = "SELECT region FROM \"Supply_Chain_KPI_Tuned\" LIMIT 500"
        mock_response = MagicMock()
        mock_response.choices[0].message.content = (
            '{"action": "generate_sql", "query": "' + fixed_sql + '"}'
        )

        with patch("agent.sql_agent.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            result = agent.refine(
                failed_sql='SELECT "Region" FROM "Supply_Chain_KPI_Tuned"',
                error='column "Region" does not exist',
                intent=SAMPLE_INTENT,
                original_query="test",
            )

        assert result == fixed_sql
