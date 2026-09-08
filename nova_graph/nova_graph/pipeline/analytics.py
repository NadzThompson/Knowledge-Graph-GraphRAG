"""Graph analytics features written to node_features (GSRT hooks: spectral, magnitude, GNN scores).

Runs on the exposure subgraph (EXPOSED_TO, FUNDS, PARENT_OF) as of a business date.
Small/medium components use numpy; very large ones should be routed to cuGraph.
"""
from __future__ import annotations
import numpy as np
import networkx as nx


def spectral_features(edges: list[tuple[str, str, float]], k_magnitude: float = 1.0) -> dict[str, dict]:
    G = nx.Graph()
    for s, d, w in edges:
        G.add_edge(s, d, weight=float(w))
    if G.number_of_nodes() < 3:
        return {}
    nodes = list(G.nodes())
    L = nx.normalized_laplacian_matrix(G, nodelist=nodes, weight="weight").toarray()
    vals, vecs = np.linalg.eigh(L)
    fiedler = vecs[:, 1] if len(vals) > 1 else np.zeros(len(nodes))
    gap = float(vals[1]) if len(vals) > 1 else 0.0
    # magnitude functional: sum of entries of the inverse similarity matrix Z_ij = exp(-t d_ij)
    D = dict(nx.all_pairs_dijkstra_path_length(G, weight="weight")) if G.number_of_nodes() <= 2000 else None
    mag_contrib = {}
    if D is not None:
        Z = np.array([[np.exp(-k_magnitude * D[a].get(b, 1e6)) for b in nodes] for a in nodes])
        try:
            w = np.linalg.solve(Z, np.ones(len(nodes)))
            mag_contrib = {n: float(w[i]) for i, n in enumerate(nodes)}
        except np.linalg.LinAlgError:
            pass
    pr = nx.pagerank(G, weight="weight")
    bt = nx.betweenness_centrality(G, weight="weight", k=min(200, len(nodes)), seed=7)
    return {n: {"pagerank": pr[n], "betweenness": bt[n], "fiedler_coord": float(fiedler[i]),
                "spectral_gap": gap, "magnitude_contrib": mag_contrib.get(n, 0.0)} for i, n in enumerate(nodes)}


def run_analytics_spark(spark, catalog: str, schema: str, as_of: str, run_id: str):
    from pyspark.sql import functions as F
    edges = spark.sql(f"""
      SELECT src, dst, weight FROM {catalog}.{schema}.edges
      WHERE edge_type IN ('EXPOSED_TO','FUNDS','PARENT_OF') AND valid_from <= date('{as_of}')
        AND (valid_to IS NULL OR valid_to > date('{as_of}'))
    """)
    try:
        from graphframes import GraphFrame
        v = edges.select(F.col("src").alias("id")).union(edges.select(F.col("dst").alias("id"))).distinct()
        comp = GraphFrame(v, edges).connectedComponents()
        edges = edges.join(comp.withColumnRenamed("id", "src"), "src")
    except Exception:
        edges = edges.withColumn("component", F.lit(0))
    def _feat(pdf):
        import pandas as pd
        feats = spectral_features(list(zip(pdf.src, pdf.dst, pdf.weight)))
        return pd.DataFrame([{"node_id": n, **f} for n, f in feats.items()])
    out = edges.groupBy("component").applyInPandas(
        _feat, schema="node_id string, pagerank double, betweenness double, fiedler_coord double, spectral_gap double, magnitude_contrib double")
    (out.withColumn("as_of", F.lit(as_of).cast("date")).withColumn("gnn_risk_score", F.lit(None).cast("double"))
        .withColumn("run_id", F.lit(run_id)).write.mode("overwrite").option("replaceWhere", f"as_of = '{as_of}'")
        .saveAsTable(f"{catalog}.{schema}.node_features"))
