"""Incremental sync of the served subgraph from Delta to PostgreSQL using Change Data Feed.

Only hot node types within the lookback window are synced. Upserts are idempotent;
the Delta version watermark lives in graph.sync_watermark so re-runs are safe.
"""
from __future__ import annotations
import json
import psycopg
from pgvector.psycopg import register_vector


def _watermark(conn, table: str) -> int:
    row = conn.execute("SELECT delta_version FROM graph.sync_watermark WHERE table_name=%s", (table,)).fetchone()
    return int(row[0]) if row else -1


def _set_watermark(conn, table: str, version: int):
    conn.execute("INSERT INTO graph.sync_watermark(table_name, delta_version) VALUES (%s,%s) "
                 "ON CONFLICT (table_name) DO UPDATE SET delta_version=EXCLUDED.delta_version, synced_at=now()", (table, version))


def sync_nodes(spark, cfg, dsn: str):
    from pyspark.sql import functions as F
    tbl = f"{cfg.delta.catalog}.{cfg.delta.schema}.nodes"
    with psycopg.connect(dsn) as conn:
        start = _watermark(conn, "nodes") + 1
    latest = spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").collect()[0]["version"]
    if start > latest:
        return
    cdf = (spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", start).table(tbl)
             .where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
             .where(F.col("node_type").isin(cfg.hot_node_types))
             .where(F.col("valid_from") >= F.date_sub(F.current_date(), cfg.hot_lookback_days) | F.col("valid_to").isNull()))
    def _write(rows):
        with psycopg.connect(dsn) as conn:
            register_vector(conn)
            with conn.cursor() as cur:
                for r in rows:
                    if r["_change_type"] == "delete" or not r["is_current"]:
                        cur.execute("DELETE FROM graph.nodes WHERE node_id=%s", (r["node_id"],))
                        continue
                    cur.execute("""
                      INSERT INTO graph.nodes (node_id,node_type,name,canonical_key,properties,masking_tier,source_system,confidence,
                        valid_from,valid_to,degree,pagerank,community_l0,community_l1,community_l2,embedding,synced_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                      ON CONFLICT (node_id) DO UPDATE SET name=EXCLUDED.name, properties=EXCLUDED.properties,
                        masking_tier=EXCLUDED.masking_tier, confidence=EXCLUDED.confidence, valid_from=EXCLUDED.valid_from,
                        valid_to=EXCLUDED.valid_to, degree=EXCLUDED.degree, pagerank=EXCLUDED.pagerank,
                        community_l0=EXCLUDED.community_l0, community_l1=EXCLUDED.community_l1, community_l2=EXCLUDED.community_l2,
                        embedding=COALESCE(EXCLUDED.embedding, graph.nodes.embedding), synced_at=now()""",
                        (r["node_id"], r["node_type"], r["name"], r["canonical_key"], json.dumps(dict(r["properties"] or {})),
                         r["masking_tier"], r["source_system"], r["confidence"], r["valid_from"], r["valid_to"],
                         r["degree"], r["pagerank"], r["community_l0"], r["community_l1"], r["community_l2"],
                         list(r["embedding"]) if r["embedding"] else None))
            conn.commit()
    cdf.foreachPartition(_write)
    with psycopg.connect(dsn) as conn:
        _set_watermark(conn, "nodes", int(latest)); conn.commit()


def sync_edges(spark, cfg, dsn: str):
    from pyspark.sql import functions as F
    tbl = f"{cfg.delta.catalog}.{cfg.delta.schema}.edges"
    with psycopg.connect(dsn) as conn:
        start = _watermark(conn, "edges") + 1
    latest = spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").collect()[0]["version"]
    if start > latest:
        return
    cdf = (spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", start).table(tbl)
             .where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
             .where(~F.col("edge_type").isin("MENTIONS", "SAME_AS", "BELONGS_TO_COMMUNITY")))
    def _write(rows):
        with psycopg.connect(dsn) as conn:
            register_vector(conn)
            with conn.cursor() as cur:
                for r in rows:
                    if r["_change_type"] == "delete" or not r["is_current"]:
                        cur.execute("DELETE FROM graph.edges WHERE edge_id=%s", (r["edge_id"],)); continue
                    cur.execute("""
                      INSERT INTO graph.edges (edge_id,src,dst,edge_type,weight,properties,evidence_chunk_ids,masking_tier,source_system,
                        confidence,valid_from,valid_to,embedding,synced_at)
                      SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
                      WHERE EXISTS (SELECT 1 FROM graph.nodes WHERE node_id=%s) AND EXISTS (SELECT 1 FROM graph.nodes WHERE node_id=%s)
                      ON CONFLICT (edge_id) DO UPDATE SET weight=EXCLUDED.weight, properties=EXCLUDED.properties,
                        evidence_chunk_ids=EXCLUDED.evidence_chunk_ids, confidence=EXCLUDED.confidence,
                        valid_from=EXCLUDED.valid_from, valid_to=EXCLUDED.valid_to,
                        embedding=COALESCE(EXCLUDED.embedding, graph.edges.embedding), synced_at=now()""",
                        (r["edge_id"], r["src"], r["dst"], r["edge_type"], r["weight"], json.dumps(dict(r["properties"] or {})),
                         list(r["evidence_chunk_ids"] or []), r["masking_tier"], r["source_system"], r["confidence"],
                         r["valid_from"], r["valid_to"], list(r["embedding"]) if r["embedding"] else None, r["src"], r["dst"]))
            conn.commit()
    cdf.foreachPartition(_write)
    with psycopg.connect(dsn) as conn:
        _set_watermark(conn, "edges", int(latest)); conn.commit()


def sync_closures_and_communities(spark, cfg, dsn: str, as_of: str, run_id: str):
    """Full replace for the as_of snapshot (closures) and run_id (communities). Uses COPY for speed."""
    cat, sch = cfg.delta.catalog, cfg.delta.schema
    closures = spark.table(f"{cat}.{sch}.closures").where(f"as_of = date('{as_of}')").toLocalIterator()
    comms = spark.table(f"{cat}.{sch}.communities").where(f"run_id = '{run_id}'").collect()
    with psycopg.connect(dsn) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM graph.closures WHERE as_of = %s", (as_of,))
            with cur.copy("COPY graph.closures (src,dst,hops,path,edge_types,weight,as_of) FROM STDIN") as cp:
                for r in closures:
                    cp.write_row((r["src"], r["dst"], r["hops"], list(r["path"]), list(r["edge_types"]), r["weight"], as_of))
            cur.execute("DELETE FROM graph.communities WHERE run_id <> %s", (run_id,))
            for c in comms:
                cur.execute("""INSERT INTO graph.communities (community_id,level,parent_id,member_count,title,summary,key_findings,rating,masking_tier,embedding,run_id)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (community_id) DO UPDATE SET summary=EXCLUDED.summary,
                               title=EXCLUDED.title, key_findings=EXCLUDED.key_findings, rating=EXCLUDED.rating, embedding=EXCLUDED.embedding""",
                            (c["community_id"], c["level"], c["parent_id"], c["member_count"], c["title"], c["summary"],
                             list(c["key_findings"] or []), c["rating"], c["masking_tier"], list(c["embedding"]) if c["embedding"] else None, run_id))
                cur.execute("DELETE FROM graph.community_members WHERE community_id=%s", (c["community_id"],))
                cur.executemany("INSERT INTO graph.community_members VALUES (%s,%s) ON CONFLICT DO NOTHING",
                                [(c["community_id"], m) for m in (c["member_ids"] or [])])
        conn.commit()


def sync_same_as_and_features(spark, cfg, dsn: str, as_of: str):
    cat, sch = cfg.delta.catalog, cfg.delta.schema
    sa = spark.table(f"{cat}.{sch}.same_as").select("node_id", "canonical_id", "method", "score").toLocalIterator()
    ft = spark.table(f"{cat}.{sch}.node_features").where(f"as_of = date('{as_of}')").toLocalIterator()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.executemany("INSERT INTO graph.same_as VALUES (%s,%s,%s,%s) ON CONFLICT (node_id) DO UPDATE SET canonical_id=EXCLUDED.canonical_id, method=EXCLUDED.method, score=EXCLUDED.score",
                        [(r["node_id"], r["canonical_id"], r["method"], r["score"]) for r in sa])
        cur.execute("DELETE FROM graph.node_features WHERE as_of=%s", (as_of,))
        with cur.copy("COPY graph.node_features (node_id,as_of,pagerank,betweenness,fiedler_coord,spectral_gap,gnn_risk_score,magnitude_contrib) FROM STDIN") as cp:
            for r in ft:
                cp.write_row((r["node_id"], as_of, r["pagerank"], r["betweenness"], r["fiedler_coord"], r["spectral_gap"], r["gnn_risk_score"], r["magnitude_contrib"]))
        conn.commit()
