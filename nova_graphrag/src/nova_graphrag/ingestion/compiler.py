from __future__ import annotations
import hashlib
from dataclasses import dataclass
from nova_graphrag.models import Entity, Relationship, Evidence


@dataclass
class CompiledGraphBatch:
    entities: list[Entity]
    relationships: list[Relationship]
    evidence: list[Evidence]


def stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


class GraphCompiler:
    """Deterministic skeleton for converting validated extracted facts into graph objects.

    In production, extraction may come from LLMs, NLP models, parsers or deterministic rules,
    but this compiler should only persist normalized facts that pass schema and governance checks.
    """

    def compile_fact(
        self,
        source_name: str,
        source_type: str,
        source_uri: str,
        relationship_type: str,
        target_name: str,
        target_type: str,
        evidence_section: str | None = None,
        confidence: float = 1.0,
    ) -> CompiledGraphBatch:
        src_id = stable_id(source_type, source_name)
        tgt_id = stable_id(target_type, target_name)
        evidence_id = stable_id("evidence", f"{source_uri}|{evidence_section}|{source_name}|{target_name}|{relationship_type}")
        rel_id = stable_id("rel", f"{src_id}|{relationship_type}|{tgt_id}")
        return CompiledGraphBatch(
            entities=[
                Entity(entity_id=src_id, entity_type=source_type, canonical_name=source_name),
                Entity(entity_id=tgt_id, entity_type=target_type, canonical_name=target_name),
            ],
            relationships=[
                Relationship(
                    relationship_id=rel_id,
                    source_entity_id=src_id,
                    target_entity_id=tgt_id,
                    relationship_type=relationship_type,
                    confidence=confidence,
                    evidence_ids=[evidence_id],
                )
            ],
            evidence=[
                Evidence(
                    evidence_id=evidence_id,
                    source_type="document",
                    source_uri=source_uri,
                    source_section=evidence_section,
                    confidence=confidence,
                )
            ],
        )
