# tests/test_validator.py
#
# Unit tests for utils/validator.py
# No DB or API connections needed — fully offline.

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.validator import validate_sql


class TestValidateSql:

    # ── Safe queries ──────────────────────────────────────────────────────────

    def test_simple_select_passes(self):
        ok, reason = validate_sql('SELECT * FROM delivery_dim LIMIT 10')
        assert ok is True
        assert reason == ""

    def test_select_with_join_passes(self):
        sql = '''
            SELECT d."Source City", AVG(k."Daily Shift Capacity Utilization (%)")
            FROM kpi_tuned k
            JOIN delivery_dim d ON k."Case ID" = d."Case ID"
            GROUP BY d."Source City"
            LIMIT 500
        '''
        ok, reason = validate_sql(sql)
        assert ok is True

    def test_with_cte_passes(self):
        sql = 'WITH cte AS (SELECT 1) SELECT * FROM cte'
        ok, reason = validate_sql(sql)
        assert ok is True

    def test_select_with_where_passes(self):
        sql = 'SELECT "Case ID" FROM kpi_tuned WHERE "Warehouse_Record_Date" LIKE \'2024-12%\' LIMIT 500'
        ok, reason = validate_sql(sql)
        assert ok is True

    # ── Blocked write operations ──────────────────────────────────────────────

    def test_insert_blocked(self):
        ok, reason = validate_sql("INSERT INTO kpi_tuned VALUES ('x', 1)")
        assert ok is False
        assert "INSERT" in reason

    def test_update_blocked(self):
        ok, reason = validate_sql("UPDATE kpi_tuned SET \"Country\" = 'India'")
        assert ok is False
        assert "UPDATE" in reason

    def test_delete_blocked(self):
        ok, reason = validate_sql("DELETE FROM delivery_dim WHERE \"Case ID\" = '1'")
        assert ok is False
        assert "DELETE" in reason

    def test_drop_blocked(self):
        ok, reason = validate_sql("DROP TABLE kpi_tuned")
        assert ok is False
        assert "DROP" in reason

    def test_alter_blocked(self):
        ok, reason = validate_sql("ALTER TABLE kpi_tuned ADD COLUMN foo text")
        assert ok is False
        assert "ALTER" in reason

    def test_truncate_blocked(self):
        ok, reason = validate_sql("TRUNCATE TABLE kpi_tuned")
        assert ok is False
        assert "TRUNCATE" in reason

    def test_create_blocked(self):
        ok, reason = validate_sql("CREATE TABLE foo (id int)")
        assert ok is False
        assert "CREATE" in reason

    def test_grant_blocked(self):
        ok, reason = validate_sql("GRANT ALL ON kpi_tuned TO public")
        assert ok is False
        assert "GRANT" in reason

    # ── Must start with SELECT or WITH ────────────────────────────────────────

    def test_non_select_start_blocked(self):
        ok, reason = validate_sql("EXPLAIN SELECT * FROM kpi_tuned")
        assert ok is False
        assert "SELECT" in reason or "WITH" in reason

    def test_empty_query_blocked(self):
        ok, reason = validate_sql("")
        assert ok is False

    # ── Case insensitivity ────────────────────────────────────────────────────

    def test_lowercase_insert_blocked(self):
        ok, reason = validate_sql("insert into kpi_tuned values ('x')")
        assert ok is False

    def test_mixed_case_delete_blocked(self):
        ok, reason = validate_sql("Delete From kpi_tuned")
        assert ok is False

    # ── Keyword in column name should NOT be blocked ──────────────────────────

    def test_column_named_like_keyword_passes(self):
        # "Deletion_Date" contains DELETE but is not a standalone keyword
        ok, reason = validate_sql('SELECT "Deletion_Date" FROM delivery_dim LIMIT 10')
        assert ok is True
