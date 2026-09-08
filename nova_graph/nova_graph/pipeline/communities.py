"""Hierarchical community detection and LLM summarisation (the 'global' half of GraphRAG)."""
from __future__ import annotations
import json
from typing import Callable
import networkx as nx

SUMMARY_PROMPT = """You are summarising a cluster of related treasury/regulatory entities for a bank.
Given the entities and relations below, write JSON:
{"title": "...", "summary": "<=200 words, factual, cite entity names>", "key_findings": ["...", "..."], "rating": 0-10}
rating = how important this cluster is for liquidity, capital, funding or regulatory compliance decisions."""


def leiden_hierarchy(edges: list[tuple[str, str, float]], levels: int = 3, resolution: float = 1.0):
    """Returns {level: {node_id: community_id}}. Uses leidenalg when available, else Louvain (networkx)."""
    G = nx.Graph()
    for s, d, w in edges:
        if s == d: continue
        G.add_edge(s, d, weight=float(w) + (G[s][d]["weight"] if G.has_edge(s, d) else 0.0))
    out: dict[int, dict[str, str]] = {}
    try:
        import igraph as ig, leidenalg as la
        g = ig.Graph.TupleList(((s, d, w) for s, d, w in G.edges(data="weight")), weights=True)
        names = g.vs["name"]
        part = la.find_partition(g, la.RBConfigurationVertexPartition, weights="weight", resolution_parameter=resolution)
        current = {names[i]: f"L0_{m}" for i, m in enumerate(part.membership)}
    except ImportError:
        comms = nx.community.louvain_communities(G, weight="weight", resolution=resolution, seed=42)
        current = {n: f"L0_{i}" for i, c in enumerate(comms) for n in c}
    out[0] = current
    for lvl in range(1, levels):
        H = nx.Graph()
        for s, d, w in G.edges(data="weight"):
            a, b = current[s], current[d]
            if a != b:
                H.add_edge(a, b, weight=w + (H[a][b]["weight"] if H.has_edge(a, b) else 0.0))
        if H.number_of_nodes() < 2: break
        comms = nx.community.louvain_communities(H, weight="weight", resolution=resolution / (lvl + 1), seed=42)
        parent = {c: f"L{lvl}_{i}" for i, cs in enumerate(comms) for c in cs}
        current = {n: parent.get(c, c) for n, c in current.items()}
        out[lvl] = current
    return out


def summarise_community(members: list[dict], relations: list[dict], llm: Callable[[list[dict]], str]) -> dict:
    body = {"entities": members[:80], "relations": relations[:150]}
    raw = llm([{"role": "system", "content": SUMMARY_PROMPT}, {"role": "user", "content": json.dumps(body)}])
    try:
        return json.loads(raw.strip().removeprefix("```json").removesuffix("```"))
    except json.JSONDecodeError:
        return {"title": "", "summary": raw[:1200], "key_findings": [], "rating": 0}


def run_communities_spark(spark, catalog: str, schema: str, llm_factory, run_id: str, levels: int = 3, min_size: int = 3):
    """Bank-scale path: GraphFrames connected components partition the graph; Leiden runs per component."""
    from pyspark.sql import functions as F
    edges = spark.table(f"{catalog}.{schema}.edges").where("is_current AND edge_type NOT IN ('MENTIONS','SAME_AS','BELONGS_TO_COMMUNITY')")
    try:
        from graphframes import GraphFrame
        v = spark.table(f"{catalog}.{schema}.nodes").where("is_current").select(F.col("node_id").alias("id"))
        g = GraphFrame(v, edges.select("src", "dst", "weight"))
        cc = g.connectedComponents()
        edges = edges.join(cc.select(F.col("id").alias("src"), "component"), "src")
    except Exception:
        edges = edges.withColumn("component", F.lit(0))
    def _part(pdf):
        import pandas as pd
        hier = leiden_hierarchy(list(zip(pdf.src, pdf.dst, pdf.weight)), levels=levels)
        rows = [(lvl, n, cid) for lvl, m in hier.items() for n, cid in m.items()]
        return pd.DataFrame(rows, columns=["level", "node_id", "community_id"])
    memb = edges.groupBy("component").applyInPandas(_part, schema="level int, node_id string, community_id string")
    memb = memb.withColumn("community_id", F.concat_ws("_", F.lit(run_id[:8]), "community_id"))
    memb.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.stg_community_members")
    # summaries: one LLM call per community >= min_size, parallelised by mapInPandas
    nodes = spark.table(f"{catalog}.{schema}.nodes").where("is_current").select("node_id", "node_type", "name", "masking_tier")
    agg = (memb.join(nodes, "node_id").groupBy("community_id", "level")
             .agg(F.collect_list(F.struct("node_id", "node_type", "name")).alias("members"), F.max("masking_tier").alias("masking_tier"))
             .where(F.size("members") >= min_size))
    def _summ(iterator):
        import pandas as pd
        llm = llm_factory()
        for pdf in iterator:
            out = []
            for r in pdf.itertuples():
                members = [dict(m.asDict()) if hasattr(m, "asDict") else dict(m) for m in r.members]
                s = summarise_community(members, [], llm)
                out.append({"community_id": r.community_id, "level": int(r.level), "member_ids": [m["node_id"] for m in members],
                            "member_count": len(members), "title": s.get("title", ""), "summary": s.get("summary", ""),
                            "key_findings": s.get("key_findings", []), "rating": float(s.get("rating", 0) or 0),
                            "masking_tier": int(r.masking_tier), "run_id": run_id})
            yield pd.DataFrame(out)
    schema_str = ("community_id string, level int, member_ids array<string>, member_count int, title string, summary string, "
                  "key_findings array<string>, rating double, masking_tier int, run_id string")
    summaries = agg.mapInPandas(_summ, schema=schema_str).withColumn("recorded_at", F.current_timestamp())
    summaries.write.mode("append").saveAsTable(f"{catalog}.{schema}.communities")
