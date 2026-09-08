from __future__ import annotations
from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, Field


class Entity(BaseModel):
    entity_id: str
    entity_type: str
    canonical_name: str
    properties: dict[str, Any] = Field(default_factory=dict)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    recorded_from: datetime | None = None
    recorded_to: datetime | None = None


class Relationship(BaseModel):
    relationship_id: str
    source_entity_id: str
    target_entity_id: str
    relationship_type: str
    properties: dict[str, Any] = Field(default_factory=dict)
    materiality: float | None = None
    confidence: float = 1.0
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    recorded_from: datetime | None = None
    recorded_to: datetime | None = None
    evidence_ids: list[str] = Field(default_factory=list)


class Evidence(BaseModel):
    evidence_id: str
    source_type: Literal["document", "data", "model", "control", "manual"]
    source_uri: str
    source_section: str | None = None
    source_version: str | None = None
    excerpt_hash: str | None = None
    confidence: float = 1.0
    approved: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class GraphQuery(BaseModel):
    text: str
    as_of: datetime | None = None
    max_hops: int = 3
    entity_types: list[str] = Field(default_factory=list)
    relationship_types: list[str] = Field(default_factory=list)
    materiality_floor: float | None = None
    top_k: int = 20


class SearchHit(BaseModel):
    source: str
    item_id: str
    score: float
    text: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class EvidencePackage(BaseModel):
    query: GraphQuery
    seed_entities: list[Entity] = Field(default_factory=list)
    graph_entities: list[Entity] = Field(default_factory=list)
    graph_relationships: list[Relationship] = Field(default_factory=list)
    semantic_hits: list[SearchHit] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    structured_results: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
