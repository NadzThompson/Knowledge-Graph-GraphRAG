from __future__ import annotations
from nova_graphrag.graph_engine import GraphEngine
from nova_graphrag.graph_store.base import GraphStore
from nova_graphrag.models import EvidencePackage, GraphQuery
from nova_graphrag.retrieval.base import SemanticRetriever
from nova_graphrag.retrieval.fusion import reciprocal_rank_fusion
from .planner import RuleBasedQueryPlanner


class NovaGraphRAG:
    def __init__(
        self,
        graph_store: GraphStore,
        retrievers: list[SemanticRetriever],
        planner: RuleBasedQueryPlanner | None = None,
    ):
        self.graph_store = graph_store
        self.graph = GraphEngine(graph_store)
        self.retrievers = retrievers
        self.planner = planner or RuleBasedQueryPlanner()

    def retrieve(self, text: str, **query_kwargs) -> EvidencePackage:
        plan = self.planner.plan(text, **query_kwargs)
        semantic_sets = [r.search(text, top_k=plan.graph_query.top_k) for r in self.retrievers] if plan.use_semantic else []
        semantic_hits = reciprocal_rank_fusion(semantic_sets, top_k=plan.graph_query.top_k) if semantic_sets else []

        seed_entities = []
        seen = set()
        for term in plan.seed_terms:
            for entity in self.graph_store.find_entities(term, limit=5):
                if entity.entity_id not in seen:
                    seen.add(entity.entity_id)
                    seed_entities.append(entity)

        graph_entities, graph_relationships = ([], [])
        if plan.use_graph and seed_entities:
            graph_entities, graph_relationships = self.graph.expand(
                [e.entity_id for e in seed_entities], plan.graph_query
            )

        evidence_ids = sorted({eid for rel in graph_relationships for eid in rel.evidence_ids})
        evidence = self.graph_store.get_evidence(evidence_ids)

        warnings = []
        if not seed_entities:
            warnings.append("No canonical graph entity was resolved from the query; response should rely more heavily on semantic retrieval.")
        if graph_relationships and not evidence:
            warnings.append("Graph relationships were retrieved without supporting evidence records.")

        return EvidencePackage(
            query=plan.graph_query,
            seed_entities=seed_entities,
            graph_entities=graph_entities,
            graph_relationships=graph_relationships,
            semantic_hits=semantic_hits,
            evidence=evidence,
            warnings=warnings,
        )
