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
from agent.pipeline_logger import PipelineLogger, _extract_tables_from_sql


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


# ── Pipeline Logging Tests ────────────────────────────────────────────────────

class TestPipelineLogging:
    """
    Verifies that PipelineLogger emits the expected stage labels to stdout
    and that PQL validation correctly passes / warns.

    All tests use capsys (pytest's stdout capture) — no live DB needed.
    """

    plog = PipelineLogger()

    # ── Individual stage emitters ─────────────────────────────────────────

    def test_nl_input_label_and_content(self, capsys):
        self.plog.nl_input("What is the OTIF rate for Q1 2024?")
        out = capsys.readouterr().out
        assert "[NL INPUT]" in out
        assert "What is the OTIF rate for Q1 2024?" in out

    def test_pql_label_contains_kpi_name(self, capsys):
        self.plog.pql('CASE WHEN sk."Delivery Compliance (%)" > 0 THEN 1 ELSE 0 END', "OTIF")
        out = capsys.readouterr().out
        assert "[PQL]" in out
        assert "OTIF" in out

    def test_pql_truncates_long_expression(self, capsys):
        long_pql = "X" * 200
        self.plog.pql(long_pql, "TestKPI")
        out = capsys.readouterr().out
        # Output should be truncated with ellipsis
        assert "..." in out
        # Should not print all 200 Xs
        assert "X" * 121 not in out

    def test_sql_label_and_inline_sql(self, capsys):
        self.plog.sql('SELECT name, value FROM cases WHERE name = \'OTIF\' LIMIT 1')
        out = capsys.readouterr().out
        assert "[SQL]" in out
        assert "SELECT" in out
        assert "cases" in out

    def test_sql_collapses_newlines(self, capsys):
        multiline = "SELECT\n  name,\n  value\nFROM cases"
        self.plog.sql(multiline)
        out = capsys.readouterr().out
        # Newlines should be collapsed to a single line
        assert "\n  name," not in out
        assert "SELECT" in out

    def test_db_hit_shows_table_name(self, capsys):
        sql = 'SELECT name, value FROM "cases" WHERE name = \'OTIF\''
        self.plog.db_hit(sql)
        out = capsys.readouterr().out
        assert "[DB HIT]" in out
        assert "Supabase PostgreSQL" in out
        assert "cases" in out

    def test_db_hit_computed_kpi_shows_base_table(self, capsys):
        sql = 'SELECT SUM(...) FROM "Supply_Chain_KPI_Tuned" sk LEFT JOIN "Delivery_Dim" dd ON ...'
        self.plog.db_hit(sql)
        out = capsys.readouterr().out
        assert "[DB HIT]" in out
        assert "Supply_Chain_KPI_Tuned" in out

    def test_result_zero_rows(self, capsys):
        self.plog.result([])
        out = capsys.readouterr().out
        assert "[RESULT]" in out
        assert "0 rows" in out

    def test_result_single_row(self, capsys):
        self.plog.result([{"kpi_name": "OTIF", "kpi_value": "25.2%"}])
        out = capsys.readouterr().out
        assert "[RESULT]" in out
        assert "1 row" in out
        assert "OTIF" in out

    def test_result_multiple_rows_shows_count_and_sample(self, capsys):
        rows = [{"city": "Mumbai", "count": 120}, {"city": "Delhi", "count": 95}]
        self.plog.result(rows)
        out = capsys.readouterr().out
        assert "[RESULT]" in out
        assert "2 rows" in out
        assert "sample" in out

    def test_separator_printed(self, capsys):
        self.plog.separator()
        out = capsys.readouterr().out
        assert "─" in out

    # ── PQL Validation ────────────────────────────────────────────────────

    def test_pql_validation_pass_for_known_kpi(self, capsys):
        """A KPI that exists in kpi_definitions.csv with only known columns → PASS."""
        mock_defs = MagicMock()
        mock_defs.get.return_value = {"name": "OTIF", "pql_logic": "..."}
        mock_defs.definitions = {}

        self.plog.pql_validation(
            "OTIF",
            mock_defs,
            'CASE WHEN sk."Delivery Compliance (%)" > 0 THEN 1 ELSE 0 END',
        )
        out = capsys.readouterr().out
        assert "[PQL VALID]" in out
        assert "PASS" in out
        assert "WARN" not in out

    def test_pql_validation_warns_unknown_kpi_name(self, capsys):
        """A KPI name absent from kpi_definitions.csv → WARN."""
        mock_defs = MagicMock()
        mock_defs.get.return_value = None  # not found
        mock_defs.definitions = {}

        self.plog.pql_validation("NonExistentKPI", mock_defs, "")
        out = capsys.readouterr().out
        assert "[PQL VALID]" in out
        assert "WARN" in out
        assert "NonExistentKPI" in out

    def test_pql_validation_warns_unknown_column(self, capsys):
        """PQL referencing a column not in kpi_definitions or the known set → WARN."""
        mock_defs = MagicMock()
        mock_defs.get.return_value = {"name": "OTIF"}  # KPI exists
        mock_defs.definitions = {}

        self.plog.pql_validation(
            "OTIF",
            mock_defs,
            'CASE WHEN sk."NonExistentColumn_XYZ" > 0 THEN 1 ELSE 0 END',
        )
        out = capsys.readouterr().out
        assert "WARN" in out
        assert "NonExistentColumn_XYZ" in out

    def test_pql_validation_known_column_no_warn(self, capsys):
        """PQL referencing a well-known column → no column warning."""
        mock_defs = MagicMock()
        mock_defs.get.return_value = {"name": "Transport Delay"}
        mock_defs.definitions = {}

        self.plog.pql_validation(
            "Transport Delay",
            mock_defs,
            'CASE WHEN sk."Delivery Impacted by Route Disruptions" = \'Yes\' THEN 1 ELSE 0 END',
        )
        out = capsys.readouterr().out
        assert "[PQL VALID]" in out
        # No unknown-column warning
        assert "unknown field" not in out

    # ── Table extraction helper ───────────────────────────────────────────

    def test_extract_tables_cases_query(self):
        sql = "SELECT name, value FROM \"cases\" WHERE name = 'OTIF' LIMIT 1"
        tables = _extract_tables_from_sql(sql)
        assert "cases" in tables

    def test_extract_tables_computed_query(self):
        sql = (
            'SELECT SUM(...) AS result\n'
            'FROM "Supply_Chain_KPI_Tuned" sk\n'
            'LEFT JOIN "Delivery_Dim" dd ON sk."Case ID" = dd."Case ID"'
        )
        tables = _extract_tables_from_sql(sql)
        assert "Supply_Chain_KPI_Tuned" in tables
        assert "Delivery_Dim" in tables

    # ── Full generate() integration ───────────────────────────────────────

    def test_generate_emits_all_four_stages(self, capsys):
        """
        Call agent.generate() for a known KPI and verify all four log labels
        appear in stdout (NL INPUT, PQL, PQL VALID, SQL).
        DB HIT and RESULT are emitted by the router, not the agent — those
        are verified separately above.
        """
        agent = _make_agent()
        sql = agent.generate(
            {"metric": "otif", "period": None, "cities": [], "confidence": 0.9},
            "What is the OTIF rate?",
        )
        out = capsys.readouterr().out
        assert "[NL INPUT]" in out
        assert "[PQL]"      in out
        assert "[PQL VALID]" in out
        assert "[SQL]"      in out
        # The generated SQL must be a SELECT
        assert sql.strip().upper().startswith("SELECT")

    def test_generate_logs_correct_query_in_nl_input(self, capsys):
        agent = _make_agent()
        query = "How many shipments were affected last month?"
        agent.generate(
            {"metric": "shipment_affected", "period": "last_month", "cities": [], "confidence": 0.8},
            query,
        )
        out = capsys.readouterr().out
        assert query in out

    def test_generate_pql_valid_pass_for_cases_kpi(self, capsys):
        """Pre-aggregated KPIs (cases tier) should pass PQL validation."""
        agent = _make_agent()
        agent.generate(
            {"metric": "open_orders", "period": None, "cities": [], "confidence": 0.9},
            "How many open orders are there?",
        )
        out = capsys.readouterr().out
        assert "PASS" in out
