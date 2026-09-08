"""Index chunks (with entity_ids/community_ids) and community summaries into Elasticsearch."""
from __future__ import annotations
from elasticsearch import Elasticsearch, helpers


def ensure_indices(es: Elasticsearch, cfg, chunk_mapping: dict, summary_mapping: dict, elser_pipeline: str = "nova-elser"):
    if not es.indices.exists(index=cfg.elastic.chunk_index):
        es.indices.create(index=cfg.elastic.chunk_index, **chunk_mapping)
    if not es.indices.exists(index=cfg.elastic.summary_index):
        es.indices.create(index=cfg.elastic.summary_index, **summary_mapping)
    es.ingest.put_pipeline(id=elser_pipeline, processors=[
        {"inference": {"model_id": cfg.elastic.elser_model, "input_output": [{"input_field": "text", "output_field": "text_elser"}]}}])
    es.ingest.put_pipeline(id=elser_pipeline + "-summary", processors=[
        {"inference": {"model_id": cfg.elastic.elser_model, "input_output": [{"input_field": "summary", "output_field": "summary_elser"}]}}])


def index_chunks_spark(spark, cfg, es_factory, pipeline: str = "nova-elser"):
    """Join Silver chunks to MENTIONS edges and community membership, then bulk index per partition."""
    cat, sch = cfg.delta.catalog, cfg.delta.schema
    df = spark.sql(f"""
      WITH ents AS (
        SELECT chunk_id, collect_set(dst) AS entity_ids, collect_set(n.name) AS entity_names
        FROM {cat}.{sch}.edges e LATERAL VIEW explode(evidence_chunk_ids) x AS chunk_id
        JOIN {cat}.{sch}.nodes n ON n.node_id = e.dst AND n.is_current
        WHERE e.edge_type = 'MENTIONS' AND e.is_current GROUP BY chunk_id),
      comms AS (
        SELECT chunk_id, collect_set(coalesce(n.community_l1, n.community_l0)) AS community_ids
        FROM ents LATERAL VIEW explode(entity_ids) y AS nid JOIN {cat}.{sch}.nodes n ON n.node_id = nid AND n.is_current GROUP BY chunk_id)
      SELECT c.*, coalesce(ents.entity_ids, array()) entity_ids, coalesce(ents.entity_names, array()) entity_names,
             coalesce(comms.community_ids, array()) community_ids
      FROM {cfg.delta.chunk_table} c LEFT JOIN ents USING (chunk_id) LEFT JOIN comms USING (chunk_id)
      WHERE c.indexed_at IS NULL OR c.updated_at > c.indexed_at
    """)
    index = cfg.elastic.chunk_index
    def _idx(rows):
        es = es_factory()
        actions = ({"_index": index, "_id": r["chunk_id"], "pipeline": pipeline,
                    "_source": {k: r[k] for k in ("chunk_id", "doc_id", "doc_title", "doc_type", "ordinal", "text", "entity_ids",
                                                   "entity_names", "community_ids", "masking_tier", "source_system") if k in r}
                    | {"regulator": (r["metadata"] or {}).get("regulator"), "jurisdiction": (r["metadata"] or {}).get("jurisdiction"),
                       "effective_date": (r["metadata"] or {}).get("effective_date")}} for r in rows)
        helpers.bulk(es, actions, chunk_size=500, request_timeout=120)
    df.foreachPartition(_idx)


def index_summaries(spark, cfg, es: Elasticsearch, run_id: str, pipeline: str = "nova-elser-summary"):
    rows = spark.table(f"{cfg.delta.catalog}.{cfg.delta.schema}.communities").where(f"run_id='{run_id}'").collect()
    helpers.bulk(es, ({"_index": cfg.elastic.summary_index, "_id": r["community_id"], "pipeline": pipeline,
                       "_source": {k: r[k] for k in ("community_id", "level", "parent_id", "title", "summary", "key_findings",
                                                     "rating", "member_count", "masking_tier", "run_id")}} for r in rows))
