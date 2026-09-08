from __future__ import annotations
from nova_graphrag.models import SearchHit
from nova_graphrag.retrieval.base import SemanticRetriever


class ElasticRetriever(SemanticRetriever):
    """Elastic hybrid retriever using lexical query plus optional vector kNN."""

    def __init__(self, client, index: str, embedder=None, vector_field: str = "embedding"):
        self.client = client
        self.index = index
        self.embedder = embedder
        self.vector_field = vector_field

    def search(self, query: str, top_k: int = 20, filters: dict | None = None) -> list[SearchHit]:
        must = [{"multi_match": {"query": query, "fields": ["content^2", "title", "metadata.*"]}}]
        body = {"size": top_k, "query": {"bool": {"must": must}}}
        if self.embedder:
            body["knn"] = {
                "field": self.vector_field,
                "query_vector": self.embedder(query),
                "k": top_k,
                "num_candidates": max(top_k * 5, 100),
            }
        resp = self.client.search(index=self.index, body=body)
        hits = resp.get("hits", {}).get("hits", [])
        return [
            SearchHit(
                source="elastic",
                item_id=h["_id"],
                score=float(h.get("_score") or 0.0),
                text=h.get("_source", {}).get("content", ""),
                metadata=h.get("_source", {}).get("metadata", {}),
            )
            for h in hits
        ]
