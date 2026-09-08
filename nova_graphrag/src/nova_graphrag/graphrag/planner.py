from __future__ import annotations
from pydantic import BaseModel, Field
from nova_graphrag.models import GraphQuery


class QueryPlan(BaseModel):
    use_graph: bool = True
    use_semantic: bool = True
    use_structured_data: bool = False
    use_calculation: bool = False
    seed_terms: list[str] = Field(default_factory=list)
    graph_query: GraphQuery


class RuleBasedQueryPlanner:
    """Safe deterministic baseline. Replace or augment with a constrained LLM planner later."""

    CALC_TERMS = {"impact", "calculate", "scenario", "shock", "what if", "change", "recalculate"}
    DATA_TERMS = {"today", "yesterday", "balance", "position", "exposure", "amount", "actual"}

    def plan(self, text: str, **kwargs) -> QueryPlan:
        lower = text.lower()
        use_calc = any(term in lower for term in self.CALC_TERMS)
        use_data = use_calc or any(term in lower for term in self.DATA_TERMS)
        seed_terms = [w.strip("?,.") for w in text.split() if len(w.strip("?,.")) > 3][:12]
        return QueryPlan(
            use_graph=True,
            use_semantic=True,
            use_structured_data=use_data,
            use_calculation=use_calc,
            seed_terms=seed_terms,
            graph_query=GraphQuery(text=text, **kwargs),
        )
