from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Optional
import yaml


@dataclass
class DeltaConfig:
    catalog: str = "nova"
    schema: str = "graph_gold"
    chunk_table: str = "nova.silver.doc_chunks"
    structured_sources: dict = field(default_factory=dict)  # logical name -> fully qualified table


@dataclass
class PostgresConfig:
    dsn: str = os.getenv("NOVA_PG_DSN", "postgresql://nova@localhost:5432/nova")
    schema: str = "graph"
    memory_schema: str = "agent_memory"
    embedding_dim: int = 1536
    hnsw_m: int = 16
    hnsw_ef_construction: int = 200
    ef_search: int = 80
    max_hops_served: int = 3
    pool_size: int = 10


@dataclass
class ElasticConfig:
    hosts: list[str] = field(default_factory=lambda: [os.getenv("NOVA_ES_URL", "https://localhost:9200")])
    chunk_index: str = "nova-chunks"
    summary_index: str = "nova-community-summaries"
    elser_model: str = ".elser_model_2"
    api_key: Optional[str] = os.getenv("NOVA_ES_API_KEY")


@dataclass
class LLMConfig:
    gateway_url: str = os.getenv("NOVA_LLM_GATEWAY", "https://llm-gateway.example.internal/v1")
    extraction_model: str = "gpt-5"
    summary_model: str = "claude-sonnet-5"
    embedding_model: str = "text-embedding-3-large"
    max_concurrency: int = 32
    agent_id: str = "graph-builder"


@dataclass
class OptionalConfig:
    neo4j_enabled: bool = False
    neo4j_uri: Optional[str] = None
    redis_enabled: bool = False
    redis_url: Optional[str] = None
    neo4j_hop_threshold: int = 4  # route to Neo4j only for queries needing >= this many hops


@dataclass
class GraphConfig:
    delta: DeltaConfig = field(default_factory=DeltaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    elastic: ElasticConfig = field(default_factory=ElasticConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    optional: OptionalConfig = field(default_factory=OptionalConfig)
    hot_node_types: list[str] = field(default_factory=lambda: [
        "LegalEntity", "Counterparty", "Product", "GLAccount", "RegulatoryReport",
        "RegulatoryGuideline", "Policy", "Metric", "Portfolio", "Currency", "Jurisdiction"])
    hot_lookback_days: int = 730
    max_degree_cap: int = 5000  # supernode guard for closure precompute


def load_config(path: Optional[str] = None) -> GraphConfig:
    cfg = GraphConfig()
    path = path or os.getenv("NOVA_GRAPH_CONFIG")
    if path and os.path.exists(path):
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        for section, values in raw.items():
            target = getattr(cfg, section, None)
            if target is not None and isinstance(values, dict) and not isinstance(target, (list, dict)):
                for k, v in values.items():
                    setattr(target, k, v)
            elif hasattr(cfg, section):
                setattr(cfg, section, values)
    return cfg
