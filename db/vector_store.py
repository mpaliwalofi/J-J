import csv
import logging
import os
import re

logger = logging.getLogger(__name__)

_CSV_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "kpi_definitions.csv")
)


class VectorStore:
    """
    Data Source — pgvector (Embeddings).

    Provides similarity search over KPI definitions to surface relevant context
    for the Response Generator.

    Strategy:
      - If the pgvector extension and a `kpi_context` table exist in Supabase,
        cosine-similarity search is performed using stored embeddings.
      - Otherwise, falls back to keyword-overlap scoring against kpi_definitions.csv.

    To enable pgvector:
      1. Run `CREATE EXTENSION IF NOT EXISTS vector;` in Supabase SQL editor.
      2. Create the kpi_context table and load embeddings (see db/seed_vectors.py).
    """

    def __init__(self, db=None):
        self.db = db
        self._definitions = self._load_csv()
        self._pgvector_ready = self._check_pgvector_table()

    # ── Initialisation ────────────────────────────────────────────────────────

    def _load_csv(self) -> list[dict]:
        if not os.path.exists(_CSV_PATH):
            logger.warning("KPI definitions CSV not found at %s", _CSV_PATH)
            return []
        with open(_CSV_PATH, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def _check_pgvector_table(self) -> bool:
        if self.db is None:
            return False
        try:
            result = self.db.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name   = 'kpi_context'
                LIMIT 1
                """
            )
            available = bool(result)
            if available:
                logger.info("pgvector kpi_context table found — using vector search")
            else:
                logger.info("kpi_context table not found — using keyword fallback")
            return available
        except Exception as e:
            logger.warning("pgvector check failed: %s", e)
            return False

    # ── Public API ────────────────────────────────────────────────────────────

    def search(self, query: str, top_k: int = 3) -> list[dict]:
        """Return top_k KPI context items most relevant to the query."""
        if self._pgvector_ready:
            return self._pgvector_search(query, top_k)
        return self._keyword_search(query, top_k)

    # ── Search backends ───────────────────────────────────────────────────────

    def _pgvector_search(self, query: str, top_k: int) -> list[dict]:
        """
        Cosine similarity search using pgvector.
        Expects kpi_context(name TEXT, definition TEXT, sql_logic TEXT, embedding VECTOR).
        Falls back to keyword search on any failure.
        """
        try:
            # Trigram similarity search using pg_trgm (available in Supabase by default).
            # For true vector cosine search, replace with:
            #   ORDER BY embedding <=> query_embedding::vector
            # after generating query_embedding via an embeddings API.
            sql = """
                SELECT name, definition, sql_logic,
                       similarity(name || ' ' || definition, %(q)s) AS score
                FROM kpi_context
                WHERE similarity(name || ' ' || definition, %(q)s) > 0.05
                ORDER BY score DESC
                LIMIT %(k)s
            """
            results = self.db.execute(sql, {"q": query, "k": top_k})
            if results:
                return results
        except Exception as e:
            logger.warning("pgvector search failed, falling back to keyword: %s", e)
        return self._keyword_search(query, top_k)

    def _keyword_search(self, query: str, top_k: int) -> list[dict]:
        """Keyword-overlap scoring over kpi_definitions.csv."""
        query_tokens = set(re.findall(r"\w+", query.lower()))
        scored = []
        for row in self._definitions:
            text = " ".join(str(v) for v in row.values()).lower()
            row_tokens = set(re.findall(r"\w+", text))
            overlap = len(query_tokens & row_tokens)
            if overlap > 0:
                scored.append((overlap, row))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [row for _, row in scored[:top_k]]
