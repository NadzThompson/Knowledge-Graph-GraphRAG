"""Agent memory on PostgreSQL (replaces Redis): LangGraph checkpointer + store, semantic memory, cache."""
from __future__ import annotations
from typing import Callable, Optional
import numpy as np
from .pg import GraphStore


def make_checkpointer(dsn: str):
    """LangGraph's PostgresSaver stores every graph state transition (durable, transactional)."""
    from langgraph.checkpoint.postgres import PostgresSaver
    saver = PostgresSaver.from_conn_string(dsn).__enter__()
    saver.setup()
    return saver


def make_store(dsn: str, embed: Callable[[list[str]], list[list[float]]], dim: int = 1536):
    """LangGraph PostgresStore with vector index: cross-thread memory with semantic search."""
    from langgraph.store.postgres import PostgresStore
    store = PostgresStore.from_conn_string(dsn, index={"dims": dim, "embed": embed, "fields": ["text"]}).__enter__()
    store.setup()
    return store


class SemanticMemory:
    """NOVA-specific memory API used by agents (entity-linked, masked, scoped, TTL'd)."""
    def __init__(self, graph: GraphStore, embed: Callable[[str], np.ndarray]):
        self.g, self.embed = graph, embed

    def recall(self, query: str, user_id: str, agent_id: str, k: int = 8, max_tier: int = 0) -> list[dict]:
        return [{"memory_id": r[0], "content": r[1], "scope": r[2], "importance": r[3], "score": r[4]}
                for r in self.g.recall(self.embed(query), user_id, agent_id, k, max_tier)]

    def remember(self, user_id: str, agent_id: str, content: str, entity_ids=(), scope="user", importance=0.5, masking_tier=0, ttl_days: Optional[int] = None):
        self.g.remember(user_id, agent_id, content, self.embed(content), entity_ids, scope, importance, masking_tier, ttl_days)

    def cached(self, key: str, fn: Callable[[], dict], ttl_s: int = 300) -> dict:
        v = self.g.cache_get(key)
        if v is not None:
            return v
        v = fn(); self.g.cache_set(key, v, ttl_s); return v
