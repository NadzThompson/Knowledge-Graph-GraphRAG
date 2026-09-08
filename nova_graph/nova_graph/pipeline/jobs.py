"""Job entry points wired into the Databricks workflow (databricks/workflow.json).

Full build (weekly) and incremental build (hourly/daily) share the same steps;
incremental simply operates on CDF deltas and skips community re-detection unless
the changed-edge ratio exceeds a threshold.
"""
from __future__ import annotations
import uuid
from datetime import date
from ..config import GraphConfig
from . import structured_edges, resolution, temporal, communities, embeddings, lineage, closures, analytics, sync_postgres, sync_elastic


def _union_staging(spark, cat: str, sch: str):
    """Combine structured, extracted and lineage staging into stg_nodes_all / stg_edges_all with node_id/edge_id computed."""
    spark.sql(f"""
      CREATE OR REPLACE TABLE {cat}.{sch}.stg_nodes_all AS
      SELECT concat('n_', substr(sha1(concat_ws('|', node_type, upper(trim(canonical_key)))), 1, 20)) node_id, *
      FROM (
        SELECT node_type, name, canonical_key, properties, masking_tier, source_system, confidence, valid_from, valid_to FROM {cat}.{sch}.stg_nodes_structured
        UNION ALL SELECT node_type, name, canonical_key, properties, masking_tier, source_system, confidence, valid_from, valid_to FROM stg_nodes_lineage
        UNION ALL SELECT node_type, name, canonical_key, properties, masking_tier, source_system, confidence, valid_from, valid_to FROM {cat}.{sch}.stg_nodes_extracted)
    """)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {cat}.{sch}.stg_edges_all AS
      SELECT concat('e_', substr(sha1(concat_ws('|', src, edge_type, dst, cast(valid_from as string))), 1, 20)) edge_id, *
      FROM (
        SELECT concat('n_', substr(sha1(concat_ws('|', src_type, upper(trim(src_key)))),1,20)) src,
               concat('n_', substr(sha1(concat_ws('|', dst_type, upper(trim(dst_key)))),1,20)) dst,
               edge_type, weight, properties, evidence_chunk_ids, masking_tier, source_system, confidence, valid_from, valid_to, business_date
        FROM (SELECT * FROM {cat}.{sch}.stg_edges_structured UNION ALL SELECT * FROM stg_edges_lineage)
        UNION ALL
        SELECT src, dst, edge_type, weight, properties, evidence_chunk_ids, masking_tier, source_system, confidence, valid_from, valid_to, valid_from business_date
        FROM {cat}.{sch}.stg_edges_extracted)
    """)


def full_build(spark, cfg: GraphConfig, llm_factory, embed_factory, es_factory, as_of: str | None = None) -> str:
    run_id = uuid.uuid4().hex
    as_of = as_of or date.today().isoformat()
    cat, sch = cfg.delta.catalog, cfg.delta.schema
    run_ts = as_of
    # 1. deterministic graph from structured sources
    structured_edges.run_structured_builders(spark, cfg.delta.structured_sources, cat, sch, run_ts)
    # 2. lineage from Unity Catalog
    lineage.build_lineage(spark, cat, sch)
    # 3. LLM extraction over new chunks (payload staged, then exploded)
    from .extraction import run_extraction_spark
    run_extraction_spark(spark, cfg, llm_factory, cfg.delta.chunk_table, f"{cat}.{sch}")
    _explode_extraction(spark, cat, sch)
    # 4. union + entity resolution + bitemporal merge
    _union_staging(spark, cat, sch)
    resolution.resolve_spark(spark, cat, sch)
    temporal.merge_nodes(spark, cat, sch)
    temporal.merge_edges(spark, cat, sch)
    # 5. embeddings (nodes, edges)
    embeddings.embed_table_spark(spark, f"{cat}.{sch}.nodes", embeddings.node_text, embed_factory, cfg.llm.embedding_model)
    # 6. communities + summaries, then embed summaries
    communities.run_communities_spark(spark, cat, sch, llm_factory, run_id)
    _apply_community_labels(spark, cat, sch, run_id)
    embeddings.embed_table_spark(spark, f"{cat}.{sch}.communities", lambda r: f"{r['title']}. {r['summary']}", embed_factory,
                                 cfg.llm.embedding_model, where=f"run_id = '{run_id}' AND embedding IS NULL")
    # 7. closures + analytics
    closures.build_closures(spark, cat, sch, cfg.hot_node_types, cfg.postgres.max_hops_served, cfg.max_degree_cap, as_of)
    analytics.run_analytics_spark(spark, cat, sch, as_of, run_id)
    # 8. serve
    sync_postgres.sync_nodes(spark, cfg, cfg.postgres.dsn)
    sync_postgres.sync_edges(spark, cfg, cfg.postgres.dsn)
    sync_postgres.sync_closures_and_communities(spark, cfg, cfg.postgres.dsn, as_of, run_id)
    sync_postgres.sync_same_as_and_features(spark, cfg, cfg.postgres.dsn, as_of)
    sync_elastic.index_chunks_spark(spark, cfg, es_factory)
    sync_elastic.index_summaries(spark, cfg, es_factory(), run_id)
    return run_id


def incremental_build(spark, cfg: GraphConfig, llm_factory, embed_factory, es_factory) -> None:
    cat, sch = cfg.delta.catalog, cfg.delta.schema
    structured_edges.run_structured_builders(spark, cfg.delta.structured_sources, cat, sch, date.today().isoformat())
    lineage.build_lineage(spark, cat, sch, lookback_days=7)
    from .extraction import run_extraction_spark
    run_extraction_spark(spark, cfg, llm_factory, cfg.delta.chunk_table, f"{cat}.{sch}")
    _explode_extraction(spark, cat, sch)
    _union_staging(spark, cat, sch)
    resolution.resolve_spark(spark, cat, sch)
    temporal.merge_nodes(spark, cat, sch); temporal.merge_edges(spark, cat, sch)
    embeddings.embed_table_spark(spark, f"{cat}.{sch}.nodes", embeddings.node_text, embed_factory, cfg.llm.embedding_model)
    sync_postgres.sync_nodes(spark, cfg, cfg.postgres.dsn); sync_postgres.sync_edges(spark, cfg, cfg.postgres.dsn)
    sync_elastic.index_chunks_spark(spark, cfg, es_factory)


def _explode_extraction(spark, cat: str, sch: str):
    spark.sql(f"""
      CREATE OR REPLACE TABLE {cat}.{sch}.stg_nodes_extracted AS
      SELECT n.node_type, n.name, n.canonical_key, from_json(to_json(n.properties), 'map<string,string>') properties,
             cast(n.masking_tier as int) masking_tier, n.source_system, cast(n.confidence as double) confidence,
             to_date(n.valid_from) valid_from, to_date(n.valid_to) valid_to
      FROM {cat}.{sch}.stg_extraction_payload
      LATERAL VIEW explode(from_json(get_json_object(payload, '$.nodes'),
        'array<struct<node_type:string,name:string,canonical_key:string,properties:map<string,string>,masking_tier:int,source_system:string,confidence:double,valid_from:string,valid_to:string>>')) x AS n
    """)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {cat}.{sch}.stg_edges_extracted AS
      SELECT e.src, e.dst, e.edge_type, cast(e.weight as double) weight, e.properties, e.evidence_chunk_ids,
             cast(e.masking_tier as int) masking_tier, e.source_system, cast(e.confidence as double) confidence,
             to_date(e.valid_from) valid_from, to_date(e.valid_to) valid_to
      FROM {cat}.{sch}.stg_extraction_payload
      LATERAL VIEW explode(from_json(get_json_object(payload, '$.edges'),
        'array<struct<src:string,dst:string,edge_type:string,weight:double,properties:map<string,string>,evidence_chunk_ids:array<string>,masking_tier:int,source_system:string,confidence:double,valid_from:string,valid_to:string>>')) x AS e
    """)
    spark.sql(f"""
      INSERT INTO {cat}.{sch}.extraction_log
      SELECT a.chunk_id, a.model, a.prompt_version, a.raw_json, a.n_nodes, a.n_edges, a.rejected, a.latency_ms, current_timestamp()
      FROM {cat}.{sch}.stg_extraction_payload
      LATERAL VIEW explode(from_json(get_json_object(payload, '$.audit'),
        'array<struct<chunk_id:string,model:string,prompt_version:string,raw_json:string,n_nodes:int,n_edges:int,rejected:int,latency_ms:int>>')) x AS a
    """)
    spark.sql(f"UPDATE {cat}.{sch}.stg_extraction_payload SET payload = NULL WHERE payload IS NOT NULL")  # keep log, free space


def _apply_community_labels(spark, cat: str, sch: str, run_id: str):
    spark.sql(f"""
      MERGE INTO {cat}.{sch}.nodes t
      USING (SELECT node_id,
                    max(CASE WHEN level=0 THEN community_id END) l0,
                    max(CASE WHEN level=1 THEN community_id END) l1,
                    max(CASE WHEN level=2 THEN community_id END) l2
             FROM {cat}.{sch}.stg_community_members GROUP BY node_id) s
      ON t.node_id = s.node_id AND t.is_current
      WHEN MATCHED THEN UPDATE SET t.community_l0 = s.l0, t.community_l1 = s.l1, t.community_l2 = s.l2
    """)
