from __future__ import annotations
from datetime import datetime
from nova_graphrag.graph_store.base import GraphStore
from nova_graphrag.models import Entity, Relationship, Evidence


def _valid_at(valid_from, valid_to, as_of: datetime | None) -> bool:
    if as_of is None:
        return True
    if valid_from and as_of < valid_from:
        return False
    if valid_to and as_of >= valid_to:
        return False
    return True


class InMemoryGraphStore(GraphStore):
    def __init__(self) -> None:
        self.entities: dict[str, Entity] = {}
        self.relationships: dict[str, Relationship] = {}
        self.evidence: dict[str, Evidence] = {}

    def upsert_entities(self, entities: list[Entity]) -> None:
        for entity in entities:
            self.entities[entity.entity_id] = entity

    def upsert_relationships(self, relationships: list[Relationship]) -> None:
        for rel in relationships:
            self.relationships[rel.relationship_id] = rel

    def upsert_evidence(self, evidence: list[Evidence]) -> None:
        for item in evidence:
            self.evidence[item.evidence_id] = item

    def get_entity(self, entity_id: str, as_of: datetime | None = None) -> Entity | None:
        entity = self.entities.get(entity_id)
        if entity and _valid_at(entity.valid_from, entity.valid_to, as_of):
            return entity
        return None

    def find_entities(self, name: str, entity_types: list[str] | None = None, limit: int = 20) -> list[Entity]:
        n = name.lower()
        hits = []
        for entity in self.entities.values():
            if n in entity.canonical_name.lower() and (not entity_types or entity.entity_type in entity_types):
                hits.append(entity)
        return hits[:limit]

    def neighbors(self, entity_ids, relationship_types=None, as_of=None):
        ids = set(entity_ids)
        rels = []
        neighbor_ids = set()
        for rel in self.relationships.values():
            if relationship_types and rel.relationship_type not in relationship_types:
                continue
            if not _valid_at(rel.valid_from, rel.valid_to, as_of):
                continue
            if rel.source_entity_id in ids or rel.target_entity_id in ids:
                rels.append(rel)
                neighbor_ids.add(rel.source_entity_id)
                neighbor_ids.add(rel.target_entity_id)
        entities = [self.entities[i] for i in neighbor_ids if i in self.entities]
        return entities, rels

    def get_evidence(self, evidence_ids: list[str]) -> list[Evidence]:
        return [self.evidence[i] for i in evidence_ids if i in self.evidence]
