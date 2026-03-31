# tests/test_router.py
#
# Unit tests for mcp/router.py
# DB and VectorStore are mocked — no live connections needed.

import pytest
import sys
import os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from mcp.router import QueryRouter


SAFE_SQL   = 'SELECT "Source City", COUNT(*) FROM delivery_dim GROUP BY "Source City" LIMIT 500'
UNSAFE_SQL = "DELETE FROM delivery_dim"


def _make_router(db_rows=None, context_items=None):
    mock_db = MagicMock()
    mock_db.execute.return_value = db_rows or [{"Source City": "Mumbai", "count": 42}]

    mock_vs = MagicMock()
    mock_vs.search.return_value = context_items or [{"KPI Name": "City", "Definition": "Source city of shipment."}]

    return QueryRouter(db=mock_db, vector_store=mock_vs), mock_db, mock_vs


class TestQueryRouter:

    def test_route_returns_expected_keys(self):
        router, _, _ = _make_router()
        result = router.route(SAFE_SQL, "What cities are there?")
        assert set(result.keys()) == {"data", "context", "sql", "row_count"}

    def test_route_passes_sql_through(self):
        router, _, _ = _make_router()
        result = router.route(SAFE_SQL, "test")
        assert result["sql"] == SAFE_SQL

    def test_route_row_count_matches_data(self):
        rows = [{"a": 1}, {"a": 2}, {"a": 3}]
        router, _, _ = _make_router(db_rows=rows)
        result = router.route(SAFE_SQL, "test")
        assert result["row_count"] == 3
        assert len(result["data"]) == 3

    def test_route_calls_vector_search(self):
        router, _, mock_vs = _make_router()
        router.route(SAFE_SQL, "Compare warehouse utilisation")
        mock_vs.search.assert_called_once_with("Compare warehouse utilisation", top_k=3)

    def test_route_blocks_unsafe_sql(self):
        router, _, _ = _make_router()
        with pytest.raises(PermissionError, match="blocked"):
            router.route(UNSAFE_SQL, "delete stuff")

    def test_route_blocks_update(self):
        router, _, _ = _make_router()
        with pytest.raises(PermissionError):
            router.route("UPDATE kpi_tuned SET \"Country\" = 'X'", "test")

    def test_route_propagates_db_runtime_error(self):
        router, mock_db, _ = _make_router()
        mock_db.execute.side_effect = RuntimeError("Database error: column does not exist")
        with pytest.raises(RuntimeError, match="Database error"):
            router.route(SAFE_SQL, "test")

    def test_route_empty_result(self):
        router, mock_db, _ = _make_router()
        mock_db.execute.return_value = []
        result = router.route(SAFE_SQL, "test")
        assert result["data"] == []
        assert result["row_count"] == 0

    def test_route_context_returned(self):
        ctx = [{"KPI Name": "OTIF", "Definition": "On-Time In-Full."}]
        router, _, _ = _make_router(context_items=ctx)
        result = router.route(SAFE_SQL, "test")
        assert result["context"] == ctx
