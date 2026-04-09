# tests/test_pipeline.py
#
# Integration-style tests for app.handle_query()
# All external services (DB, Groq) are mocked.

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Patch DB + vector store at import time so app.py module-level
# initialisations don't attempt a real Supabase connection.
_mock_db = MagicMock()
_mock_db.list_tables.return_value = ["Supply_Chain_KPI_Tuned", "Delivery_Dim"]
_mock_db.get_schema.return_value = {}

_mock_vs = MagicMock()
_mock_vs.search.return_value = []

with (
    patch("db.query_runner.QueryRunner.__init__", return_value=None),
    patch("db.vector_store.VectorStore.__init__", return_value=None),
    patch("psycopg2.pool.ThreadedConnectionPool", return_value=MagicMock()),
):
    import app  # noqa: E402  — must come after patches


def _make_mock_intent(metric="warehouse_utilisation", period="2024-12"):
    return {
        "intent": "comparison",
        "metric": metric,
        "period": period,
        "filters": {},
        "cities": ["Mumbai", "Delhi"],
        "regions": [],
        "confidence": 0.82,
    }


def _make_mock_router_result(rows=None):
    return {
        "data":      rows or [{"Source City": "Mumbai", "avg": 78.5}],
        "context":   [{"KPI Name": "Utilisation", "Definition": "..."}],
        "sql":       "SELECT ...",
        "row_count": len(rows or [{"x": 1}]),
    }


class TestHandleQuery:

    @pytest.fixture(autouse=True)
    def patch_all(self):
        """Patch all external dependencies before each test."""
        with (
            patch("app.classifier")  as self.mock_clf,
            patch("app.agent")       as self.mock_agent,
            patch("app.router")      as self.mock_router,
            patch("app.responder")   as self.mock_responder,
        ):
            self.mock_clf.classify.return_value    = _make_mock_intent()
            self.mock_agent.generate.return_value  = "SELECT ..."
            self.mock_router.route.return_value    = _make_mock_router_result()
            self.mock_responder.generate.return_value = "Mumbai: 78.5%, Delhi: 65.2%"
            yield

    def test_successful_query_returns_success_true(self):
        from app import handle_query
        result = handle_query("Compare warehouse utilisation between Mumbai and Delhi in December 2024")
        assert result["success"] is True

    def test_successful_query_returns_answer(self):
        from app import handle_query
        result = handle_query("Compare warehouse utilisation between Mumbai and Delhi in December 2024")
        assert result["answer"] == "Mumbai: 78.5%, Delhi: 65.2%"

    def test_successful_query_returns_sql(self):
        from app import handle_query
        result = handle_query("test query")
        assert result["sql"] == "SELECT ..."

    def test_successful_query_error_is_none(self):
        from app import handle_query
        result = handle_query("test query")
        assert result["error"] is None

    def test_classifier_value_error_returns_failure(self):
        from app import handle_query
        self.mock_clf.classify.side_effect = ValueError("Incomplete query — could not determine: time period")
        result = handle_query("What is OTIF in Mumbai?")
        assert result["success"] is False
        assert "time period" in result["error"]

    def test_permission_error_returns_failure(self):
        from app import handle_query
        self.mock_router.route.side_effect = PermissionError("MCP blocked query: Forbidden operation: DELETE")
        result = handle_query("test query")
        assert result["success"] is False
        assert "DELETE" in result["error"]

    def test_db_runtime_error_exhausts_retries(self):
        from app import handle_query
        self.mock_router.route.side_effect = RuntimeError("Database error: column does not exist")
        result = handle_query("test query")
        assert result["success"] is False
        assert result["answer"] == "Could not retrieve data after multiple attempts."

    def test_result_row_count_populated(self):
        from app import handle_query
        rows = [{"city": "Mumbai"}, {"city": "Delhi"}]
        self.mock_router.route.return_value = _make_mock_router_result(rows=rows)
        result = handle_query("test query")
        assert result["row_count"] == 2

    def test_agent_generate_called_with_intent(self):
        from app import handle_query
        intent = _make_mock_intent()
        self.mock_clf.classify.return_value = intent
        handle_query("test query")
        self.mock_agent.generate.assert_called_once_with(intent, "test query")

    def test_refine_called_on_retry(self):
        from app import handle_query
        # First call fails, second succeeds
        self.mock_router.route.side_effect = [
            RuntimeError("some db error"),
            _make_mock_router_result(),
        ]
        result = handle_query("test query")
        assert result["success"] is True
        assert self.mock_agent.refine.called
