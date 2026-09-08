"""One-call bootstrap for the FastAPI app: build stores, retriever, memory and tools from config."""
from __future__ import annotations
import numpy as np
from elasticsearch import Elasticsearch
from ..config import GraphConfig, load_config
from ..serving.pg import GraphStore
from ..serving.elastic import DocStore
from ..serving.retriever import GraphRAG
from ..serving.memory import SemanticMemory, make_checkpointer, make_store
from .tools import build_tools


def bootstrap(embed_one, embed_many, cfg: GraphConfig | None = None, rerank=None):
    """embed_one: str -> np.ndarray ; embed_many: list[str] -> list[list[float]] (both via the LLM Gateway)."""
    cfg = cfg or load_config()
    graph = GraphStore(cfg.postgres.dsn, cfg.postgres.pool_size, cfg.postgres.ef_search)
    es = Elasticsearch(cfg.elastic.hosts, api_key=cfg.elastic.api_key)
    docs = DocStore(es, cfg.elastic.chunk_index, cfg.elastic.summary_index, cfg.elastic.elser_model)
    neo = None
    if cfg.optional.neo4j_enabled and cfg.optional.neo4j_uri:
        from ..serving.neo4j_optional import Neo4jExpander
        neo = Neo4jExpander(cfg.optional.neo4j_uri, None)
    rag = GraphRAG(graph, docs, lambda q: np.asarray(embed_one(q), dtype=np.float32), rerank=rerank, neo4j=neo,
                   neo4j_hop_threshold=cfg.optional.neo4j_hop_threshold)
    memory = SemanticMemory(graph, rag.embed)
    return {"cfg": cfg, "graph": graph, "docs": docs, "rag": rag, "memory": memory,
            "tools": build_tools(rag, memory),
            "checkpointer": make_checkpointer(cfg.postgres.dsn),
            "store": make_store(cfg.postgres.dsn, embed_many, cfg.postgres.embedding_dim)}
