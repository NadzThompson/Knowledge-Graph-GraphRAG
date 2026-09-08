"""LangGraph tools exposing the graph to NOVA agents.

Wire once in the orchestration layer and pass to every specialist agent:

    from nova_graph.langgraph.tools import build_tools
    tools = build_tools(graphrag, memory)
    treasury_navigator = create_react_agent(model, tools=tools, checkpointer=make_checkpointer(dsn))

The retriever output feeds RETRIEVE -> CLASSIFY -> SYNTHESIZE -> EXPLAIN directly:
bundle.evidence_text() is the RETRIEVE payload, bundle.explain is the EXPLAIN payload.
"""
from __future__ import annotations
from datetime import date
from typing import Optional
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from ..serving.masking import Caller
from ..serving.retriever import GraphRAG
from ..serving.memory import SemanticMemory


def _caller(config: RunnableConfig) -> Caller:
    c = (config or {}).get("configurable", {})
    return Caller(user_id=c.get("user_id", "anonymous"), roles=c.get("roles", []), attributes=c.get("attributes", {}))


def build_tools(rag: GraphRAG, memory: SemanticMemory):

    @tool
    def graph_retrieve(query: str, mode: str = "auto", as_of: Optional[str] = None, hops: int = 2,
                       entity_names: list[str] = [], config: RunnableConfig = None) -> dict:
        """Retrieve evidence from the treasury knowledge graph and document corpus.
        mode: auto|local|global|drift|temporal|lineage|path. as_of: YYYY-MM-DD for point-in-time answers.
        Returns ranked evidence, the supporting subgraph, and an explanation of how it was retrieved."""
        caller = _caller(config)
        cfg = (config or {}).get("configurable", {})
        d = date.fromisoformat(as_of) if as_of else None
        b = rag.retrieve(query, caller, mode="local" if mode == "temporal" else mode, as_of=d, hops=hops,
                         entity_names=entity_names, agent_id=cfg.get("agent_id", "unknown"), thread_id=cfg.get("thread_id"))
        return {"evidence": b.evidence_text(), "subgraph": {"nodes": b.subgraph_nodes[:80], "edges": b.subgraph_edges[:160]},
                "communities": b.communities, "explain": b.explain, "mode": b.mode, "as_of": as_of}

    @tool
    def graph_lineage(entity: str, as_of: Optional[str] = None, config: RunnableConfig = None) -> dict:
        """Trace where a metric, report line or table comes from (upstream) and what it feeds (downstream)."""
        b = rag.retrieve(f"lineage of {entity}", _caller(config), mode="lineage", as_of=date.fromisoformat(as_of) if as_of else None, entity_names=[entity])
        return {"paths": [h.text for h in b.hits], "subgraph": {"nodes": b.subgraph_nodes, "edges": b.subgraph_edges}, "explain": b.explain}

    @tool
    def graph_path(entity_a: str, entity_b: str, as_of: Optional[str] = None, config: RunnableConfig = None) -> dict:
        """Explain how two entities are connected (shortest path with relationship types)."""
        b = rag.retrieve(f"{entity_a} to {entity_b}", _caller(config), mode="path", as_of=date.fromisoformat(as_of) if as_of else None, entity_names=[entity_a, entity_b])
        return {"path": b.hits[0].text if b.hits else None, "explain": b.explain}

    @tool
    def graph_risk_features(entity_names: list[str], as_of: Optional[str] = None, config: RunnableConfig = None) -> list[dict]:
        """Systemic-risk features (PageRank, betweenness, Fiedler coordinate, spectral gap, magnitude contribution) for entities."""
        caller = _caller(config)
        ids = [r[0] for r in rag.g.resolve_names(entity_names, max_tier=caller.max_tier, limit=1)]
        rows = rag.g.features(ids, date.fromisoformat(as_of) if as_of else None)
        return [dict(zip(["node_id", "as_of", "pagerank", "betweenness", "fiedler_coord", "spectral_gap", "gnn_risk_score", "magnitude_contrib"], r)) for r in rows]

    @tool
    def memory_recall(query: str, config: RunnableConfig = None) -> list[dict]:
        """Recall relevant long-term memories for this user and agent."""
        caller = _caller(config); cfg = (config or {}).get("configurable", {})
        return memory.recall(query, caller.user_id, cfg.get("agent_id", "*"), max_tier=caller.max_tier)

    @tool
    def memory_remember(content: str, importance: float = 0.5, entity_names: list[str] = [], config: RunnableConfig = None) -> str:
        """Store a durable fact or preference about this user's work (scoped to the user)."""
        caller = _caller(config); cfg = (config or {}).get("configurable", {})
        ids = [r[0] for r in rag.g.resolve_names(entity_names, max_tier=caller.max_tier, limit=1)]
        memory.remember(caller.user_id, cfg.get("agent_id", "*"), content, ids, importance=importance)
        return "stored"

    return [graph_retrieve, graph_lineage, graph_path, graph_risk_features, memory_recall, memory_remember]
