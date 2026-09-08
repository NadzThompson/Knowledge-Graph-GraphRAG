"""Project Unity Catalog lineage into DataAsset nodes and DERIVED_FROM edges.

System tables: system.access.table_lineage / column_lineage. Every Delta table, Snowflake
mirror, Kyvos cube extract and regulatory report field becomes a DataAsset; every observed
read->write becomes DERIVED_FROM. Metrics link to the DataAssets they are computed from,
so an agent can answer 'which upstream tables feed LCR line 12 and who changed them'.
"""

def build_lineage(spark, catalog: str, schema: str, lookback_days: int = 90):
    spark.sql(f"""
      INSERT OVERWRITE {catalog}.{schema}.lineage_edges
      SELECT source_table_full_name AS src_asset, target_table_full_name AS dst_asset,
             map() AS column_lineage, entity_id AS job_name, max(event_time) AS last_seen
      FROM system.access.table_lineage
      WHERE source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL
        AND event_date >= date_sub(current_date(), {lookback_days})
      GROUP BY source_table_full_name, target_table_full_name, entity_id
    """)
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW stg_nodes_lineage AS
      SELECT DISTINCT 'DataAsset' node_type, asset name, asset canonical_key, map('kind','table') properties,
             0 masking_tier, 'UNITY_CATALOG' source_system, 1.0 confidence, current_date() valid_from, cast(NULL as date) valid_to
      FROM (SELECT src_asset asset FROM {catalog}.{schema}.lineage_edges UNION SELECT dst_asset FROM {catalog}.{schema}.lineage_edges)
    """)
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW stg_edges_lineage AS
      SELECT 'DataAsset' src_type, dst_asset src_key, 'DataAsset' dst_type, src_asset dst_key, 'DERIVED_FROM' edge_type,
             1.0 weight, map('job', job_name) properties, 0 masking_tier, 'UNITY_CATALOG' source_system, 1.0 confidence,
             current_date() valid_from, cast(NULL as date) valid_to, current_date() business_date, array() evidence_chunk_ids
      FROM {catalog}.{schema}.lineage_edges
    """)
