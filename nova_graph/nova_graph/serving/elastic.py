"""Elasticsearch hybrid retrieval (ELSER sparse + BM25, RRF fused) returning chunks with graph ids."""
from __future__ import annotations
from typing import Optional, Sequence
from elasticsearch import Elasticsearch


class DocStore:
    def __init__(self, es: Elasticsearch, chunk_index: str, summary_index: str, elser_model: str):
        self.es, self.chunk_index, self.summary_index, self.elser_model = es, chunk_index, summary_index, elser_model

    def _filters(self, max_tier: int, as_of: Optional[str], entity_ids: Optional[Sequence[str]], doc_types: Optional[Sequence[str]]):
        f = [{"range": {"masking_tier": {"lte": max_tier}}}]
        if as_of:
            f.append({"bool": {"should": [{"bool": {"must_not": {"exists": {"field": "effective_date"}}}},
                                          {"range": {"effective_date": {"lte": as_of}}}], "minimum_should_match": 1}})
            f.append({"bool": {"should": [{"bool": {"must_not": {"exists": {"field": "valid_to"}}}},
                                          {"range": {"valid_to": {"gt": as_of}}}], "minimum_should_match": 1}})
        if entity_ids: f.append({"terms": {"entity_ids": list(entity_ids)}})
        if doc_types: f.append({"terms": {"doc_type": list(doc_types)}})
        return f

    def hybrid(self, query: str, k: int = 12, max_tier: int = 0, as_of: Optional[str] = None,
               entity_ids: Optional[Sequence[str]] = None, doc_types: Optional[Sequence[str]] = None):
        filters = self._filters(max_tier, as_of, entity_ids, doc_types)
        body = {
            "size": k,
            "retriever": {"rrf": {"retrievers": [
                {"standard": {"query": {"bool": {"must": {"multi_match": {"query": query, "fields": ["text^2", "doc_title", "entity_names^1.5"]}}, "filter": filters}}}},
                {"standard": {"query": {"bool": {"must": {"sparse_vector": {"field": "text_elser", "inference_id": self.elser_model, "query": query}}, "filter": filters}}}},
            ], "rank_window_size": 60, "rank_constant": 20}},
            "_source": ["chunk_id", "doc_id", "doc_title", "doc_type", "text", "entity_ids", "community_ids", "masking_tier", "effective_date"],
        }
        res = self.es.search(index=self.chunk_index, **body)
        return [{**h["_source"], "score": h.get("_score", 0.0)} for h in res["hits"]["hits"]]

    def summaries(self, query: str, k: int = 6, max_tier: int = 0, level: Optional[int] = None):
        f = [{"range": {"masking_tier": {"lte": max_tier}}}]
        if level is not None: f.append({"term": {"level": level}})
        body = {"size": k, "query": {"bool": {"must": {"sparse_vector": {"field": "summary_elser", "inference_id": self.elser_model, "query": query}}, "filter": f}}}
        res = self.es.search(index=self.summary_index, **body)
        return [{**h["_source"], "score": h.get("_score", 0.0)} for h in res["hits"]["hits"]]

    def chunks_by_id(self, ids: Sequence[str], max_tier: int = 0):
        if not ids: return []
        res = self.es.mget(index=self.chunk_index, ids=list(ids))
        return [d["_source"] for d in res["docs"] if d.get("found") and d["_source"].get("masking_tier", 0) <= max_tier]
