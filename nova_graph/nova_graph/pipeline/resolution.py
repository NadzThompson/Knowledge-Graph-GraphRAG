"""Entity resolution: the step that decides whether a bank-scale graph is usable.

Three passes, cheapest first, each recorded in same_as with its method and score:
 1. exact canonical key within type (LEI, GL code, guideline id)   -> score 1.0
 2. rule-based normalised name match within blocking key            -> score 0.95
 3. embedding similarity within block, above threshold, same type   -> score = cosine
Survivor = highest confidence / most sources; all others get SAME_AS -> survivor
and their edges are re-pointed in the merge step. Nothing is deleted.
"""
from __future__ import annotations
import re, unicodedata
from dataclasses import dataclass
from typing import Iterable
import numpy as np

LEGAL_SUFFIX = re.compile(r"\b(inc|inc\.|ltd|ltd\.|limited|llc|plc|corp|corporation|co\.|company|sa|ag|nv|bv|gmbh|s\.a\.|bank|the)\b", re.I)

def normalise_name(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = LEGAL_SUFFIX.sub(" ", s.lower())
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def blocking_key(node_type: str, name: str, props: dict) -> str:
    """Coarse partition so we never compare across the whole bank."""
    n = normalise_name(name)
    tok = n.split()[0] if n else ""
    return f"{node_type}|{props.get('country', props.get('jurisdiction', '')).upper()}|{tok[:4]}"


@dataclass
class Candidate:
    node_id: str
    node_type: str
    name: str
    canonical_key: str
    props: dict
    confidence: float
    n_sources: int
    embedding: np.ndarray | None = None


def resolve_block(cands: list[Candidate], emb_threshold: float = 0.92) -> list[tuple[str, str, str, float]]:
    """Returns (node_id, canonical_id, method, score) rows for a single block."""
    out: list[tuple[str, str, str, float]] = []
    if len(cands) < 2:
        return out
    parent = {c.node_id: c.node_id for c in cands}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    def union(a, b, method, score):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra; out.append((b, a, method, score))
    by_key: dict[str, Candidate] = {}
    for c in cands:
        k = c.canonical_key.strip().upper()
        if k in by_key: union(by_key[k].node_id, c.node_id, "exact_key", 1.0)
        else: by_key[k] = c
    by_norm: dict[str, Candidate] = {}
    for c in cands:
        n = normalise_name(c.name)
        if len(n) < 4: continue
        if n in by_norm: union(by_norm[n].node_id, c.node_id, "rule", 0.95)
        else: by_norm[n] = c
    embs = [c for c in cands if c.embedding is not None]
    if len(embs) > 1:
        M = np.stack([c.embedding / (np.linalg.norm(c.embedding) + 1e-9) for c in embs])
        S = M @ M.T
        for i in range(len(embs)):
            for j in range(i + 1, len(embs)):
                if S[i, j] >= emb_threshold and embs[i].node_type == embs[j].node_type:
                    union(embs[i].node_id, embs[j].node_id, "embedding", float(S[i, j]))
    # choose survivor per cluster: most sources, then confidence
    clusters: dict[str, list[Candidate]] = {}
    for c in cands: clusters.setdefault(find(c.node_id), []).append(c)
    # method/score evidence per node (best link that pulled it into its cluster)
    evidence: dict[str, tuple[str, float]] = {}
    for a, b, method, score in out:
        for x in (a, b):
            if x not in evidence or score > evidence[x][1]:
                evidence[x] = (method, score)
    final = []
    for members in clusters.values():
        if len(members) < 2: continue
        survivor = max(members, key=lambda c: (c.n_sources, c.confidence, -len(c.name)))
        for m in members:
            if m.node_id != survivor.node_id:
                method, score = evidence.get(m.node_id, ("cluster", 0.9))
                final.append((m.node_id, survivor.node_id, method, score))
    return final


def resolve_spark(spark, catalog: str, schema: str, emb_threshold: float = 0.92):
    """Group candidates by blocking key and resolve each block on executors."""
    from pyspark.sql import functions as F, types as T
    nodes = spark.table(f"{catalog}.{schema}.stg_nodes_all")
    @F.udf(T.StringType())
    def bk(node_type, name, props): return blocking_key(node_type, name, props or {})
    blocked = nodes.withColumn("bk", bk("node_type", "name", "properties"))
    out_schema = "node_id string, canonical_id string, method string, score double"
    def _res(pdf):
        import pandas as pd
        cands = [Candidate(r.node_id, r.node_type, r.name, r.canonical_key, dict(r.properties or {}), float(r.confidence),
                           int(getattr(r, "n_sources", 1)), np.array(r.embedding) if getattr(r, "embedding", None) is not None else None)
                 for r in pdf.itertuples()]
        return pd.DataFrame(resolve_block(cands, emb_threshold), columns=["node_id", "canonical_id", "method", "score"])
    res = blocked.groupBy("bk").applyInPandas(_res, schema=out_schema)
    (res.withColumn("cluster_id", F.col("canonical_id")).withColumn("recorded_at", F.current_timestamp())
        .write.mode("append").saveAsTable(f"{catalog}.{schema}.same_as"))
