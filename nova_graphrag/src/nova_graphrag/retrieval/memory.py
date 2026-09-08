from __future__ import annotations
from nova_graphrag.retrieval.base import SemanticRetriever
from nova_graphrag.models import SearchHit


class InMemoryRetriever(SemanticRetriever):
    def __init__(self, documents: dict[str, str] | None = None, source: str = "memory"):
        self.documents = documents or {}
        self.source = source

    def search(self, query: str, top_k: int = 20, filters: dict | None = None) -> list[SearchHit]:
        terms = {t.lower() for t in query.split() if len(t) > 2}
        hits = []
        for item_id, text in self.documents.items():
            tokens = set(text.lower().split())
            overlap = len(terms & tokens)
            if overlap:
                hits.append(SearchHit(source=self.source, item_id=item_id, score=float(overlap), text=text))
        return sorted(hits, key=lambda x: x.score, reverse=True)[:top_k]
