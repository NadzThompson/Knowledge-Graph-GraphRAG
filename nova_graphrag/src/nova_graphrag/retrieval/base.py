from __future__ import annotations
from abc import ABC, abstractmethod
from nova_graphrag.models import SearchHit


class SemanticRetriever(ABC):
    @abstractmethod
    def search(self, query: str, top_k: int = 20, filters: dict | None = None) -> list[SearchHit]: ...
