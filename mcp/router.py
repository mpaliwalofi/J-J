import logging
from db.query_runner import QueryRunner
from db.vector_store import VectorStore
from utils.validator import validate_sql

logger = logging.getLogger(__name__)


class QueryRouter:
    """
    MCP Layer — Query Validator & Router (Control Plane).

    Responsibilities:
      1. Validate the generated SQL (blocks any write operations)
      2. Route the SQL query to PostgreSQL (Structured Data)
      3. Run a parallel similarity search against pgvector (Embeddings) for KPI context
      4. Return combined {data, context} to the Response Generator
    """

    def __init__(self, db: QueryRunner, vector_store: VectorStore):
        self.db = db
        self.vector_store = vector_store

    def route(self, sql: str, original_query: str) -> dict:
        """
        Validate, execute, and enrich with context.

        Args:
            sql            : SQL string produced by the Agent Layer
            original_query : raw user question (used for similarity search)

        Returns:
            {
              "data":      list of row dicts from PostgreSQL,
              "context":   list of relevant KPI definition dicts from pgvector,
              "sql":       the validated SQL that was executed,
              "row_count": int
            }

        Raises:
            PermissionError : if SQL fails safety validation
            RuntimeError    : if the database query fails
        """
        # ── 1. Validate ───────────────────────────────────────────────────────
        is_safe, reason = validate_sql(sql)
        if not is_safe:
            raise PermissionError(f"MCP blocked query: {reason}")

        # ── 2. Route to PostgreSQL ────────────────────────────────────────────
        logger.info("MCP routing to PostgreSQL")
        data = self.db.execute(sql)
        logger.info("PostgreSQL returned %d rows", len(data))

        # ── 3. Similarity search → pgvector for KPI context ───────────────────
        logger.info("MCP running similarity search for context")
        context = self.vector_store.search(original_query, top_k=3)
        logger.info("VectorStore returned %d context items", len(context))

        return {
            "data":      data,
            "context":   context,
            "sql":       sql,
            "row_count": len(data),
        }
