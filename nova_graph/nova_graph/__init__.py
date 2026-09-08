"""nova_graph: knowledge graph + GraphRAG layer for NOVA.

pipeline/  Databricks (Spark + Delta) graph construction: deterministic edges,
           LLM extraction, entity resolution, communities, embeddings, lineage,
           bitemporal upserts, sync to PostgreSQL and Elasticsearch.
serving/   Runtime retrieval against PostgreSQL/pgvector and Elasticsearch,
           agent memory, masking.
langgraph/ Tools and wiring for the NOVA LangGraph agents.
"""
from .config import GraphConfig, load_config  # noqa: F401
from .ontology import ONTOLOGY, NodeType, EdgeType  # noqa: F401
__version__ = "0.1.0"
