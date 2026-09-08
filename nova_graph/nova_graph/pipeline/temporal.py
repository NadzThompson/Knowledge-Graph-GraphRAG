"""Bitemporal merge into the graph of record.

A node or edge is never updated in place. When a new version arrives:
  - the current row's valid_to is closed at the new valid_from (business time)
  - a new row is inserted with recorded_at = now (system time)
So "the graph as of 31 March 2026 as we knew it on 15 April" is a filter, not a rebuild.
Delta MERGE handles this at scale; is_current speeds up the common 'latest' query.
"""
from __future__ import annotations


def merge_nodes(spark, catalog: str, schema: str, staging: str = "stg_nodes_all") -> None:
    tgt, src = f"{catalog}.{schema}.nodes", f"{catalog}.{schema}.{staging}"
    spark.sql(f"""
      MERGE INTO {tgt} t
      USING (SELECT s.*, sha2(concat_ws('|', s.name, to_json(s.properties), s.masking_tier), 256) AS content_hash
             FROM {src} s) s
      ON t.node_id = s.node_id AND t.is_current = true
      WHEN MATCHED AND sha2(concat_ws('|', t.name, to_json(t.properties), t.masking_tier), 256) <> s.content_hash
           AND s.valid_from > t.valid_from THEN
        UPDATE SET t.valid_to = s.valid_from, t.is_current = false
      WHEN NOT MATCHED THEN
        INSERT (node_id, node_type, name, canonical_key, properties, masking_tier, source_system, confidence,
                valid_from, valid_to, recorded_at, is_current)
        VALUES (s.node_id, s.node_type, s.name, s.canonical_key, s.properties, s.masking_tier, s.source_system,
                s.confidence, s.valid_from, s.valid_to, current_timestamp(), true)
    """)
    # second pass inserts the new versions for rows we just closed
    spark.sql(f"""
      INSERT INTO {tgt} (node_id, node_type, name, canonical_key, properties, masking_tier, source_system, confidence,
                         valid_from, valid_to, recorded_at, is_current)
      SELECT s.node_id, s.node_type, s.name, s.canonical_key, s.properties, s.masking_tier, s.source_system, s.confidence,
             s.valid_from, s.valid_to, current_timestamp(), true
      FROM {src} s
      LEFT ANTI JOIN {tgt} t ON t.node_id = s.node_id AND t.is_current = true
    """)


def merge_edges(spark, catalog: str, schema: str, staging: str = "stg_edges_all") -> None:
    tgt, src = f"{catalog}.{schema}.edges", f"{catalog}.{schema}.{staging}"
    # re-point src/dst through same_as so merged entities collapse onto survivors
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW stg_edges_resolved AS
      SELECT e.edge_id, coalesce(a.canonical_id, e.src) AS src, coalesce(b.canonical_id, e.dst) AS dst,
             e.edge_type, e.weight, e.properties, e.evidence_chunk_ids, e.masking_tier, e.source_system,
             e.confidence, e.valid_from, e.valid_to, e.business_date
      FROM {src} e
      LEFT JOIN {catalog}.{schema}.same_as a ON a.node_id = e.src
      LEFT JOIN {catalog}.{schema}.same_as b ON b.node_id = e.dst
    """)
    spark.sql(f"""
      MERGE INTO {tgt} t USING stg_edges_resolved s
      ON t.edge_id = s.edge_id AND t.is_current = true
      WHEN MATCHED AND (t.weight <> s.weight OR to_json(t.properties) <> to_json(s.properties)) AND s.valid_from > t.valid_from THEN
        UPDATE SET t.valid_to = s.valid_from, t.is_current = false
      WHEN MATCHED AND s.evidence_chunk_ids IS NOT NULL THEN
        UPDATE SET t.evidence_chunk_ids = array_distinct(concat(t.evidence_chunk_ids, s.evidence_chunk_ids)),
                   t.confidence = greatest(t.confidence, s.confidence)
      WHEN NOT MATCHED THEN INSERT (edge_id, src, dst, edge_type, weight, properties, evidence_chunk_ids, masking_tier,
                                    source_system, confidence, valid_from, valid_to, recorded_at, is_current, business_date)
        VALUES (s.edge_id, s.src, s.dst, s.edge_type, s.weight, s.properties, s.evidence_chunk_ids, s.masking_tier,
                s.source_system, s.confidence, s.valid_from, s.valid_to, current_timestamp(), true, s.business_date)
    """)


def as_of_view(spark, catalog: str, schema: str, business_date: str, known_at: str | None = None) -> tuple[str, str]:
    """Create temp views nodes_asof / edges_asof for a business date (and optionally a system time)."""
    known = f"AND recorded_at <= timestamp('{known_at}')" if known_at else ""
    for tbl in ("nodes", "edges"):
        spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW {tbl}_asof AS
          SELECT * FROM {catalog}.{schema}.{tbl}
          WHERE valid_from <= date('{business_date}') AND (valid_to IS NULL OR valid_to > date('{business_date}')) {known}
        """)
    return "nodes_asof", "edges_asof"
