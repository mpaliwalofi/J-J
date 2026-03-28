# db/query_runner.py
# Configured for Supabase PostgreSQL

import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
import re
import logging

logger = logging.getLogger(__name__)


class QueryRunner:

    FORBIDDEN_KEYWORDS = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
        "TRUNCATE", "CREATE", "GRANT", "REVOKE", "EXECUTE"
    ]

    def __init__(self, dsn: str = None, min_conn: int = 1, max_conn: int = 5):
        """
        For Supabase, your DSN looks like:
        postgresql://postgres:[YOUR-PASSWORD]@pfdkwwirzljwuurvqjfs.supabase.co:5432/postgres

        Get it from:
        Supabase Dashboard → your project → Connect button → Direct connection string
        Copy it, replace [YOUR-PASSWORD] with your actual DB password.
        Paste it in your .env as DATABASE_URL=...
        """
        self.dsn = dsn or os.getenv("DATABASE_URL")
        if not self.dsn:
            raise ValueError(
                "No DATABASE_URL found.\n"
                "Go to Supabase → Connect → Direct connection string\n"
                "Add it to your .env file:\n"
                "DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@pfdkwwirzljwuurvqjfs.supabase.co:5432/postgres"
            )

        # Supabase requires SSL — add sslmode=require if not already in the URL
        if "sslmode" not in self.dsn:
            self.dsn += "?sslmode=require"

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            dsn=self.dsn
        )
        logger.info("Connected to Supabase PostgreSQL (pool min=%d max=%d)", min_conn, max_conn)

    def _get_conn(self):
        return self._pool.getconn()

    def _release_conn(self, conn):
        self._pool.putconn(conn)

    def _is_safe_query(self, query: str) -> tuple[bool, str]:
        clean = " ".join(query.split()).upper()
        for keyword in self.FORBIDDEN_KEYWORDS:
            if re.search(rf'\b{keyword}\b', clean):
                return False, f"Forbidden operation: {keyword}"
        if not (clean.startswith("SELECT") or clean.startswith("WITH")):
            return False, "Query must start with SELECT or WITH"
        return True, ""

    def execute(self, query: str, params: tuple = None) -> list[dict]:
        is_safe, reason = self._is_safe_query(query)
        if not is_safe:
            raise PermissionError(f"Query blocked: {reason}")

        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                return [dict(row) for row in rows]
        except psycopg2.Error as e:
            raise RuntimeError(f"Database error: {e.pgerror or str(e)}")
        finally:
            self._release_conn(conn)

    def get_schema(self, table: str) -> dict:
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = %s
            ORDER BY ordinal_position
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, (table,))
                rows = cur.fetchall()
            if not rows:
                return {"error": f"Table '{table}' not found."}
            return {row[0]: row[1] for row in rows}
        except psycopg2.Error as e:
            raise RuntimeError(f"Schema lookup failed: {e.pgerror or str(e)}")
        finally:
            self._release_conn(conn)

    def list_tables(self) -> list[str]:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type   = 'BASE TABLE'
            ORDER BY table_name
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]
        except psycopg2.Error as e:
            raise RuntimeError(f"Table listing failed: {e.pgerror or str(e)}")
        finally:
            self._release_conn(conn)

    def get_sample_rows(self, table: str, limit: int = 3) -> list[dict]:
        query = f'SELECT * FROM "{table}" LIMIT %s'
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (limit,))
                return [dict(row) for row in cur.fetchall()]
        except psycopg2.Error as e:
            raise RuntimeError(f"Sample rows failed: {e.pgerror or str(e)}")
        finally:
            self._release_conn(conn)

    def close(self):
        self._pool.closeall()
        logger.info("Connection pool closed")