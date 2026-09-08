from __future__ import annotations
from nova_graphrag.models import SearchHit
from nova_graphrag.retrieval.base import SemanticRetriever


class PgVectorRetriever(SemanticRetriever):
    """pgvector adapter. Embedder must return a vector matching the configured column dimension."""

    def __init__(self, conn, embedder, table: str = "nova_semantic_chunk"):
        self.conn = conn
        self.embedder = embedder
        self.table = table

    def search(self, query: str, top_k: int = 20, filters: dict | None = None) -> list[SearchHit]:
        embedding = self.embedder(query)
        sql = f"""
            SELECT chunk_id, content, metadata,
                   1 - (embedding <=> %s::vector) AS score
            FROM {self.table}
            ORDER BY embedding <=> %s::vector
            LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (embedding, embedding, top_k))
            rows = cur.fetchall()
        return [
            SearchHit(source="pgvector", item_id=row[0], text=row[1], metadata=row[2] or {}, score=float(row[3]))
            for row in rows
        ]
