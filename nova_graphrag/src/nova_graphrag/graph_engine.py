from __future__ import annotations
from collections import deque
from nova_graphrag.graph_store.base import GraphStore
from nova_graphrag.models import GraphQuery, Entity, Relationship


class GraphEngine:
    """Treasury-aware graph operations independent of the underlying graph store."""

    def __init__(self, store: GraphStore):
        self.store = store

    def expand(self, seed_ids: list[str], query: GraphQuery) -> tuple[list[Entity], list[Relationship]]:
        visited = set(seed_ids)
        frontier = list(seed_ids)
        all_entities: dict[str, Entity] = {}
        all_relationships: dict[str, Relationship] = {}

        for _depth in range(query.max_hops):
            if not frontier:
                break
            entities, rels = self.store.neighbors(frontier, query.relationship_types or None, query.as_of)
            next_frontier = []
            for entity in entities:
                if query.entity_types and entity.entity_type not in query.entity_types:
                    continue
                all_entities[entity.entity_id] = entity
                if entity.entity_id not in visited:
                    visited.add(entity.entity_id)
                    next_frontier.append(entity.entity_id)
            for rel in rels:
                if query.materiality_floor is not None and rel.materiality is not None and rel.materiality < query.materiality_floor:
                    continue
                all_relationships[rel.relationship_id] = rel
            frontier = next_frontier

        return list(all_entities.values()), list(all_relationships.values())

    def dependencies(self, entity_id: str, as_of=None, max_hops: int = 5):
        q = GraphQuery(text=f"dependencies of {entity_id}", as_of=as_of, max_hops=max_hops)
        return self.expand([entity_id], q)

    def shortest_path(self, source_id: str, target_id: str, max_hops: int = 8) -> list[str]:
        queue = deque([(source_id, [source_id])])
        visited = {source_id}
        for _ in range(max_hops + 1):
            if not queue:
                break
            node, path = queue.popleft()
            if node == target_id:
                return path
            entities, _rels = self.store.neighbors([node])
            for e in entities:
                if e.entity_id not in visited:
                    visited.add(e.entity_id)
                    queue.append((e.entity_id, path + [e.entity_id]))
        return []
