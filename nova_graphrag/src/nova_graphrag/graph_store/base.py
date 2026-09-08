from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from nova_graphrag.models import Entity, Relationship, Evidence


class GraphStore(ABC):
    @abstractmethod
    def upsert_entities(self, entities: list[Entity]) -> None: ...

    @abstractmethod
    def upsert_relationships(self, relationships: list[Relationship]) -> None: ...

    @abstractmethod
    def upsert_evidence(self, evidence: list[Evidence]) -> None: ...

    @abstractmethod
    def get_entity(self, entity_id: str, as_of: datetime | None = None) -> Entity | None: ...

    @abstractmethod
    def find_entities(self, name: str, entity_types: list[str] | None = None, limit: int = 20) -> list[Entity]: ...

    @abstractmethod
    def neighbors(
        self,
        entity_ids: list[str],
        relationship_types: list[str] | None = None,
        as_of: datetime | None = None,
    ) -> tuple[list[Entity], list[Relationship]]: ...

    @abstractmethod
    def get_evidence(self, evidence_ids: list[str]) -> list[Evidence]: ...
