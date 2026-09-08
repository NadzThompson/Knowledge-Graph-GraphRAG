"""LLM entity/relation extraction constrained to the fixed ontology.

Calls go through the NOVA LLM Gateway. Output is validated against the
ontology; anything that does not validate is logged to extraction_log.rejected
and dropped. Nothing unvalidated reaches the graph.
"""
from __future__ import annotations
import json, time
from datetime import date, datetime
from typing import Callable, Optional
from ..ontology import ONTOLOGY
from ..schemas import Chunk, Node, Edge, ExtractionResult
from .ids import node_id, edge_id

PROMPT_VERSION = "extract-v3"

SYSTEM_PROMPT = """You extract a knowledge graph from bank treasury documents.
Return ONLY JSON: {"entities":[{"type":..,"name":..,"key":..,"props":{..}}],
"relations":[{"type":..,"from_key":..,"to_key":..,"weight":1.0,"props":{..},"quote":".."}]}
Rules:
- Use only the entity and relation types listed in the schema. If unsure, omit.
- "key" is the most canonical identifier available (regulation id like "OSFI LAR Ch.2 s.2.2.A.1",
  LEI, GL code, metric code like "LCR", report code like "NCCF#L12"). Otherwise a normalised name.
- Every relation must include a short verbatim "quote" from the text as evidence.
- Do not extract people's personal details; roles only (e.g. "CFO").
- Prefer fewer, precise items over many vague ones."""


def build_messages(chunk: Chunk) -> list[dict]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT + "\nSchema:\n" + json.dumps(ONTOLOGY.prompt_schema())},
        {"role": "user", "content": f"Document: {chunk.doc_title} ({chunk.doc_type})\nSection: {chunk.metadata.get('section','')}\n\n{chunk.text}"},
    ]


def parse_extraction(chunk: Chunk, raw: str, model: str, valid_from: Optional[date] = None,
                     source_system: str = "LLM_EXTRACT") -> tuple[ExtractionResult, int]:
    """Validate raw LLM JSON against the ontology. Returns (result, rejected_count)."""
    vf = valid_from or date.today()
    rejected = 0
    try:
        data = json.loads(raw.strip().removeprefix("```json").removesuffix("```"))
    except json.JSONDecodeError:
        return ExtractionResult(chunk_id=chunk.chunk_id, nodes=[], edges=[], model=model, prompt_version=PROMPT_VERSION), 1

    key_to_node: dict[str, Node] = {}
    for ent in data.get("entities", []):
        t, name, key = ent.get("type"), (ent.get("name") or "").strip(), (ent.get("key") or ent.get("name") or "").strip()
        if t not in {n.value for n in ONTOLOGY.node_types} or not name or not key:
            rejected += 1; continue
        n = Node(node_id=node_id(t, key), node_type=t, name=name, canonical_key=key,
                 properties={k: str(v) for k, v in (ent.get("props") or {}).items()},
                 masking_tier=max(chunk.masking_tier, ONTOLOGY.masking_tier_default.get(t, 0) if hasattr(ONTOLOGY, "masking_tier_default") else 0),
                 source_system=source_system, valid_from=vf, confidence=0.8)
        key_to_node[key] = n

    edges: list[Edge] = []
    for rel in data.get("relations", []):
        t = rel.get("type"); a = key_to_node.get(rel.get("from_key")); b = key_to_node.get(rel.get("to_key"))
        if not (t and a and b) or not ONTOLOGY.llm_allowed(t) or not ONTOLOGY.allowed(t, a.node_type, b.node_type):
            rejected += 1; continue
        props = {k: str(v) for k, v in (rel.get("props") or {}).items()}
        if rel.get("quote"):
            props["quote"] = str(rel["quote"])[:300]
        edges.append(Edge(edge_id=edge_id(a.node_id, b.node_id, t, str(vf)), src=a.node_id, dst=b.node_id, edge_type=t,
                          weight=float(rel.get("weight") or 1.0), properties=props, evidence_chunk_ids=[chunk.chunk_id],
                          masking_tier=max(a.masking_tier, b.masking_tier), source_system=source_system,
                          valid_from=vf, confidence=0.75))
    # MENTIONS edges from the document node give provenance for every extracted entity
    doc = Node(node_id=node_id("Document", chunk.doc_id), node_type="Document", name=chunk.doc_title or chunk.doc_id,
               canonical_key=chunk.doc_id, source_system=source_system, valid_from=vf, masking_tier=chunk.masking_tier)
    nodes = list(key_to_node.values())
    for n in nodes:
        edges.append(Edge(edge_id=edge_id(doc.node_id, n.node_id, "MENTIONS", str(vf)), src=doc.node_id, dst=n.node_id,
                          edge_type="MENTIONS", evidence_chunk_ids=[chunk.chunk_id], masking_tier=n.masking_tier,
                          source_system=source_system, valid_from=vf))
    return ExtractionResult(chunk_id=chunk.chunk_id, nodes=nodes + [doc], edges=edges, model=model, prompt_version=PROMPT_VERSION), rejected


def extract_chunk(chunk: Chunk, llm: Callable[[list[dict]], str], model: str) -> tuple[ExtractionResult, dict]:
    """llm: callable(messages)->text. Returns result plus an audit row for extraction_log."""
    t0 = time.time()
    raw = llm(build_messages(chunk))
    result, rejected = parse_extraction(chunk, raw, model)
    audit = {"chunk_id": chunk.chunk_id, "model": model, "prompt_version": PROMPT_VERSION, "raw_json": raw,
             "n_nodes": len(result.nodes), "n_edges": len(result.edges), "rejected": rejected,
             "latency_ms": int((time.time() - t0) * 1000), "recorded_at": datetime.utcnow()}
    return result, audit


def run_extraction_spark(spark, cfg, llm_factory: Callable[[], Callable], chunk_table: str, out_schema: str, batch_size: int = 64):
    """Distributed extraction: mapInPandas over Silver chunks, one gateway client per executor task."""
    import pandas as pd
    from pyspark.sql import functions as F
    def _partition(iterator):
        llm = llm_factory()
        for pdf in iterator:
            rows_n, rows_e, rows_a = [], [], []
            for r in pdf.itertuples():
                ch = Chunk(chunk_id=r.chunk_id, doc_id=r.doc_id, text=r.text, ordinal=r.ordinal, doc_title=r.doc_title,
                           doc_type=r.doc_type, masking_tier=int(r.masking_tier), metadata={"section": getattr(r, "section", "")})
                res, audit = extract_chunk(ch, llm, cfg.llm.extraction_model)
                rows_n += [n.model_dump() for n in res.nodes]; rows_e += [e.model_dump() for e in res.edges]; rows_a.append(audit)
            yield pd.DataFrame({"payload": [json.dumps({"nodes": rows_n, "edges": rows_e, "audit": rows_a}, default=str)]})
    chunks = spark.table(chunk_table).where(F.col("extracted_at").isNull())
    out = chunks.repartition(cfg.llm.max_concurrency).mapInPandas(_partition, schema="payload string")
    out.write.mode("append").saveAsTable(f"{out_schema}.stg_extraction_payload")
