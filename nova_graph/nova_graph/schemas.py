"""Pydantic models shared by pipeline and serving layers."""
from __future__ import annotations
from datetime import datetime, date
from typing import Any, Optional
from pydantic import BaseModel, Field


class Node(BaseModel):
    node_id: str                      # deterministic: sha1(type|canonical_key)
    node_type: str
    name: str
    canonical_key: str                # e.g. LEI, GL code, OSFI guideline id
    properties: dict[str, Any] = Field(default_factory=dict)
    masking_tier: int = 0             # 0 public-internal .. 3 restricted
    source_system: str = "unknown"
    valid_from: date
    valid_to: Optional[date] = None
    recorded_at: datetime = Field(default_factory=datetime.utcnow)
    confidence: float = 1.0


class Edge(BaseModel):
    edge_id: str
    src: str
    dst: str
    edge_type: str
    weight: float = 1.0
    properties: dict[str, Any] = Field(default_factory=dict)
    evidence_chunk_ids: list[str] = Field(default_factory=list)
    masking_tier: int = 0
    source_system: str = "unknown"
    valid_from: date
    valid_to: Optional[date] = None
    recorded_at: datetime = Field(default_factory=datetime.utcnow)
    confidence: float = 1.0


class Chunk(BaseModel):
    chunk_id: str
    doc_id: str
    text: str
    ordinal: int
    doc_title: str = ""
    doc_type: str = ""
    entity_ids: list[str] = Field(default_factory=list)
    community_ids: list[str] = Field(default_factory=list)
    masking_tier: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class Community(BaseModel):
    community_id: str
    level: int
    parent_id: Optional[str] = None
    member_ids: list[str]
    title: str = ""
    summary: str = ""
    key_findings: list[str] = Field(default_factory=list)
    rating: float = 0.0               # importance 0-10 set by summariser
    masking_tier: int = 0


class ExtractionResult(BaseModel):
    chunk_id: str
    nodes: list[Node]
    edges: list[Edge]
    model: str
    prompt_version: str


class RetrievalHit(BaseModel):
    kind: str                         # chunk | node | edge | community | path | memory
    id: str
    score: float
    text: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class RetrievalBundle(BaseModel):
    query: str
    mode: str
    as_of: Optional[date] = None
    hits: list[RetrievalHit]
    subgraph_nodes: list[dict[str, Any]] = Field(default_factory=list)
    subgraph_edges: list[dict[str, Any]] = Field(default_factory=list)
    communities: list[dict[str, Any]] = Field(default_factory=list)
    explain: dict[str, Any] = Field(default_factory=dict)

    def evidence_text(self, max_chars: int = 12000) -> str:
        parts: list[str] = []
        for h in self.hits:
            parts.append(f"[{h.kind}:{h.id} score={h.score:.3f}] {h.text}")
        out = "\n".join(parts)
        return out[:max_chars]
