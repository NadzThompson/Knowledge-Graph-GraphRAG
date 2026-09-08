"""GraphRAG retriever: one call, five modes, always explainable.

modes
  local     entity neighbourhood: hybrid text hits + vector node hits -> k-hop expansion -> rerank
  global    community summaries (map) -> supporting chunks (reduce)      for 'what are the themes / across the bank' questions
  drift     global to seed communities, then local inside them            for broad questions needing specifics
  temporal  local, evaluated as of a business date (bitemporal filters)   for 'as at quarter end' questions
  lineage   DERIVED_FROM / MAPS_TO_LINE walks from a metric or report     for 'where does this number come from'
  path      shortest path between two named entities                     for 'how is X connected to Y'
auto        heuristics pick one of the above.
"""
from __future__ import annotations
import re, time
from datetime import date
from typing import Callable, Optional, Sequence
import numpy as np
from ..schemas import RetrievalBundle, RetrievalHit
from .pg import GraphStore
from .elastic import DocStore
from .masking import Caller

GLOBAL_CUES = re.compile(r"\b(overall|across|themes?|landscape|summar|trend|portfolio-wide|bank-wide|all (entities|subsidiaries|regions))\b", re.I)
LINEAGE_CUES = re.compile(r"\b(lineage|upstream|downstream|derived|source of|where does .* come from|feeds?|calculated from)\b", re.I)
PATH_CUES = re.compile(r"\b(connected|relationship between|link between|path from|how does .* relate)\b", re.I)
DATE_CUE = re.compile(r"\b(as (at|of)|on|at)\s+(\d{4}-\d{2}-\d{2}|\d{1,2} \w+ \d{4}|q[1-4] ?\d{4}|quarter end)\b", re.I)


class GraphRAG:
    def __init__(self, graph: GraphStore, docs: DocStore, embed: Callable[[str], np.ndarray],
                 rerank: Optional[Callable[[str, list[str]], list[float]]] = None, neo4j=None, neo4j_hop_threshold: int = 4):
        self.g, self.d, self.embed, self.rerank = graph, docs, embed, rerank
        self.neo4j, self.neo4j_hop_threshold = neo4j, neo4j_hop_threshold

    # ---------- public ----------
    def retrieve(self, query: str, caller: Caller, mode: str = "auto", as_of: Optional[date] = None, hops: int = 2,
                 k_chunks: int = 12, k_nodes: int = 10, entity_names: Sequence[str] = (), agent_id: str = "unknown",
                 thread_id: Optional[str] = None) -> RetrievalBundle:
        t0 = time.time()
        tier = caller.max_tier
        if mode == "auto":
            mode = self._pick_mode(query, as_of)
        if mode == "global":
            bundle = self._global(query, tier, as_of)
        elif mode == "drift":
            bundle = self._drift(query, tier, as_of, hops, k_chunks, k_nodes)
        elif mode == "lineage":
            bundle = self._lineage(query, tier, as_of, entity_names)
        elif mode == "path":
            bundle = self._path(query, tier, as_of, entity_names)
        else:
            bundle = self._local(query, tier, as_of, hops, k_chunks, k_nodes, entity_names)
        bundle.mode, bundle.as_of = mode, as_of
        bundle.explain["latency_ms"] = int((time.time() - t0) * 1000)
        bundle.explain["max_tier"] = tier
        try:
            self.g.log_retrieval(thread_id, caller.user_id, agent_id, query, mode, as_of, [h.id for h in bundle.hits], bundle.explain["latency_ms"])
        except Exception:
            pass
        return bundle

    # ---------- modes ----------
    def _local(self, query, tier, as_of, hops, k_chunks, k_nodes, entity_names) -> RetrievalBundle:
        qv = self.embed(query)
        as_of_s = as_of.isoformat() if as_of else None
        chunks = self.d.hybrid(query, k=k_chunks, max_tier=tier, as_of=as_of_s)
        seed_ids: dict[str, float] = {}
        for c in chunks:
            for eid in c.get("entity_ids", []):
                seed_ids[eid] = seed_ids.get(eid, 0) + c["score"]
        for r in self.g.search_nodes(qv, k=k_nodes, as_of=as_of, max_tier=tier):
            seed_ids[r[0]] = seed_ids.get(r[0], 0) + float(r[3]) * 2
        for r in self.g.resolve_names(entity_names, max_tier=tier):
            seed_ids[r[0]] = seed_ids.get(r[0], 0) + 3.0
        seeds = [i for i, _ in sorted(seed_ids.items(), key=lambda x: -x[1])[:k_nodes]]
        explain = {"seeds": seeds, "seed_scores": {s: round(seed_ids[s], 3) for s in seeds}, "chunk_hits": len(chunks)}
        if hops >= self.neo4j_hop_threshold and self.neo4j is not None:
            expansion = self.neo4j.expand(seeds, hops, as_of, tier); explain["expansion_engine"] = "neo4j"
        else:
            expansion = self.g.expand(seeds, hops=min(hops, 3), as_of=as_of, max_tier=tier); explain["expansion_engine"] = "postgres_closures"
        node_ids = set(seeds) | {r[1] for r in expansion}
        nodes = self.g.nodes(list(node_ids), max_tier=tier)
        edges = self.g.edges_between(list(node_ids), as_of=as_of, max_tier=tier)
        # evidence chunks referenced by edges that the text search missed
        ev_ids = {cid for e in edges for cid in (e[6] or [])} - {c["chunk_id"] for c in chunks}
        chunks += self.d.chunks_by_id(list(ev_ids)[:k_chunks], max_tier=tier)
        hits = self._rank(query, chunks, nodes, edges, seed_ids)
        return RetrievalBundle(query=query, mode="local", hits=hits,
                               subgraph_nodes=[self._node_dict(n) for n in nodes], subgraph_edges=[self._edge_dict(e) for e in edges], explain=explain)

    def _global(self, query, tier, as_of) -> RetrievalBundle:
        qv = self.embed(query)
        comms = self.g.search_communities(qv, k=8, max_tier=tier)
        es_comms = self.d.summaries(query, k=8, max_tier=tier)
        merged: dict[str, dict] = {}
        for c in comms:
            merged[c[0]] = {"community_id": c[0], "level": c[1], "title": c[2], "summary": c[3], "rating": c[4], "score": float(c[5])}
        for c in es_comms:
            m = merged.setdefault(c["community_id"], {**c, "score": 0.0}); m["score"] += 0.5
        ranked = sorted(merged.values(), key=lambda m: -(m["score"] + 0.05 * float(m.get("rating") or 0)))[:6]
        chunks = self.d.hybrid(query, k=8, max_tier=tier, as_of=as_of.isoformat() if as_of else None)
        hits = [RetrievalHit(kind="community", id=m["community_id"], score=m["score"], text=f"{m['title']}: {m['summary']}",
                             metadata={"level": m["level"], "rating": m.get("rating")}) for m in ranked]
        hits += [RetrievalHit(kind="chunk", id=c["chunk_id"], score=c["score"], text=c["text"],
                              metadata={"doc": c["doc_title"], "masking_tier": c.get("masking_tier", 0)}) for c in chunks]
        return RetrievalBundle(query=query, mode="global", hits=hits, communities=ranked, explain={"communities": [m["community_id"] for m in ranked]})

    def _drift(self, query, tier, as_of, hops, k_chunks, k_nodes) -> RetrievalBundle:
        g = self._global(query, tier, as_of)
        top_comm = [c["community_id"] for c in g.communities[:3]]
        local = self._local(query, tier, as_of, hops, k_chunks, k_nodes, entity_names=())
        local.hits = g.hits[:3] + local.hits
        local.communities = g.communities
        local.explain["seed_communities"] = top_comm
        return local

    def _lineage(self, query, tier, as_of, entity_names) -> RetrievalBundle:
        names = list(entity_names) or re.findall(r"\b([A-Z][A-Za-z0-9#\.\-]{2,}(?: [A-Z][A-Za-z0-9#\.\-]{1,})*)\b", query)
        seeds = [r[0] for r in self.g.resolve_names(names, max_tier=tier, limit=2)]
        if not seeds:
            seeds = [r[0] for r in self.g.search_nodes(self.embed(query), k=3, types=["Metric", "RegulatoryReport", "DataAsset"], max_tier=tier)]
        walk = self.g.traverse(seeds, max_hops=6, as_of=as_of, edge_types=["DERIVED_FROM", "MAPS_TO_LINE", "ROLLS_UP_TO", "REPORTED_IN"], max_tier=tier)
        node_ids = set(seeds) | {w[0] for w in walk}
        nodes = self.g.nodes(list(node_ids), max_tier=tier); edges = self.g.edges_between(list(node_ids), as_of=as_of, max_tier=tier)
        names_map = {n[0]: n[2] for n in nodes}
        hits = [RetrievalHit(kind="path", id="|".join(w[2]), score=1.0 / (1 + w[1]),
                             text=" -> ".join(f"{names_map.get(p, p)}" + (f" [{w[3][i]}]" if i < len(w[3]) else "") for i, p in enumerate(w[2])),
                             metadata={"hops": w[1]}) for w in walk[:40]]
        return RetrievalBundle(query=query, mode="lineage", hits=hits, subgraph_nodes=[self._node_dict(n) for n in nodes],
                               subgraph_edges=[self._edge_dict(e) for e in edges], explain={"seeds": seeds})

    def _path(self, query, tier, as_of, entity_names) -> RetrievalBundle:
        names = list(entity_names)
        if len(names) < 2:
            names = re.findall(r"\b([A-Z][A-Za-z0-9&\.\-]{2,}(?: [A-Z][A-Za-z0-9&\.\-]{1,})*)\b", query)[:2]
        res = self.g.resolve_names(names, max_tier=tier, limit=1)
        if len(res) < 2:
            return self._local(query, tier, as_of, 2, 8, 8, names)
        a, b = res[0][0], res[1][0]
        sp = self.g.shortest_path(a, b, 6, as_of)
        if not sp:
            return RetrievalBundle(query=query, mode="path", hits=[], explain={"a": a, "b": b, "found": False})
        nodes = self.g.nodes(sp[0], max_tier=tier); names_map = {n[0]: n[2] for n in nodes}
        edges = self.g.edges_between(sp[0], as_of=as_of, max_tier=tier)
        text = " -> ".join(f"{names_map.get(p, p)}" + (f" [{sp[1][i]}]" if i < len(sp[1]) else "") for i, p in enumerate(sp[0]))
        return RetrievalBundle(query=query, mode="path", hits=[RetrievalHit(kind="path", id="|".join(sp[0]), score=1.0, text=text, metadata={"hops": sp[2]})],
                               subgraph_nodes=[self._node_dict(n) for n in nodes], subgraph_edges=[self._edge_dict(e) for e in edges],
                               explain={"a": a, "b": b, "found": True})

    # ---------- helpers ----------
    def _pick_mode(self, query: str, as_of) -> str:
        if LINEAGE_CUES.search(query): return "lineage"
        if PATH_CUES.search(query): return "path"
        if GLOBAL_CUES.search(query): return "drift" if len(query.split()) > 12 else "global"
        return "local"

    def _rank(self, query, chunks, nodes, edges, seed_scores) -> list[RetrievalHit]:
        names = {n[0]: n[2] for n in nodes}
        hits: list[RetrievalHit] = []
        for c in chunks:
            boost = sum(seed_scores.get(e, 0) for e in c.get("entity_ids", [])) * 0.05
            hits.append(RetrievalHit(kind="chunk", id=c["chunk_id"], score=float(c.get("score", 0)) + boost, text=c["text"],
                                     metadata={"doc": c.get("doc_title"), "entity_ids": c.get("entity_ids", []), "masking_tier": c.get("masking_tier", 0)}))
        for e in edges:
            q = (e[5] or {}).get("quote", "")
            txt = f"{names.get(e[1], e[1])} {e[3].replace('_',' ').lower()} {names.get(e[2], e[2])} (w={e[4]:.2f})" + (f": \"{q}\"" if q else "")
            hits.append(RetrievalHit(kind="edge", id=e[0], score=0.4 + 0.1 * (seed_scores.get(e[1], 0) + seed_scores.get(e[2], 0)),
                                     text=txt, metadata={"edge_type": e[3], "evidence": e[6] or [], "valid_from": str(e[7]), "valid_to": str(e[8])}))
        for n in nodes:
            props = ", ".join(f"{k}={v}" for k, v in (n[4] or {}).items())
            hits.append(RetrievalHit(kind="node", id=n[0], score=0.3 + seed_scores.get(n[0], 0) * 0.1,
                                     text=f"{n[1]} {n[2]} [{n[3]}] {props}", metadata={"node_type": n[1], "community": n[9], "masking_tier": n[5]}))
        if self.rerank and hits:
            scores = self.rerank(query, [h.text for h in hits])
            for h, s in zip(hits, scores): h.score = 0.5 * h.score + 0.5 * float(s)
        return sorted(hits, key=lambda h: -h.score)[:60]

    @staticmethod
    def _node_dict(n): return {"id": n[0], "type": n[1], "name": n[2], "key": n[3], "props": n[4], "valid_from": str(n[6]), "valid_to": str(n[7]), "community": n[9]}
    @staticmethod
    def _edge_dict(e): return {"id": e[0], "src": e[1], "dst": e[2], "type": e[3], "weight": e[4], "props": e[5], "evidence": e[6], "valid_from": str(e[7]), "valid_to": str(e[8])}
