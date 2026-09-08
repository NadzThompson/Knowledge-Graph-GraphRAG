"""Deterministic graph construction from structured bank data.

This is the majority of the bank-wide graph and it costs nothing in LLM calls.
Each builder maps a source table (Snowflake mirror in Silver, Kyvos extract,
GL master, LE master, regulatory report line mapping) into Node/Edge rows.

Builders are expressed as Spark SQL so they run at any scale; the SQL is kept in
this module so ontology changes and source changes are reviewed together.
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class StructuredBuilder:
    name: str
    node_sql: str       # must yield: node_type, name, canonical_key, properties MAP, masking_tier, source_system, valid_from, valid_to
    edge_sql: str       # must yield: src_type, src_key, dst_type, dst_key, edge_type, weight, properties MAP, masking_tier, source_system, valid_from, valid_to, business_date


LEGAL_ENTITY_HIERARCHY = StructuredBuilder(
    name="legal_entity_hierarchy",
    node_sql="""
      SELECT 'LegalEntity' AS node_type, le_name AS name, lei AS canonical_key,
             map('country', country, 'entity_class', entity_class) AS properties,
             0 AS masking_tier, 'LE_MASTER' AS source_system,
             effective_from AS valid_from, effective_to AS valid_to
      FROM {le_master}
    """,
    edge_sql="""
      SELECT 'LegalEntity' src_type, parent_lei src_key, 'LegalEntity' dst_type, lei dst_key,
             'PARENT_OF' edge_type, 1.0 weight, map('ownership_pct', cast(ownership_pct as string)) properties,
             0 masking_tier, 'LE_MASTER' source_system, effective_from valid_from, effective_to valid_to, current_date() business_date
      FROM {le_master} WHERE parent_lei IS NOT NULL
      UNION ALL
      SELECT 'LegalEntity', lei, 'Jurisdiction', country, 'LOCATED_IN', 1.0, map(), 0, 'LE_MASTER', effective_from, effective_to, current_date()
      FROM {le_master}
    """,
)

GL_HIERARCHY = StructuredBuilder(
    name="gl_hierarchy",
    node_sql="""
      SELECT 'GLAccount' node_type, gl_description name, gl_code canonical_key,
             map('gl_level', cast(gl_level as string), 'balance_type', balance_type) properties,
             0 masking_tier, 'GL_MASTER' source_system, effective_from valid_from, effective_to valid_to
      FROM {gl_master}
    """,
    edge_sql="""
      SELECT 'GLAccount', gl_code, 'GLAccount', parent_gl_code, 'ROLLS_UP_TO', 1.0, map(), 0, 'GL_MASTER',
             effective_from, effective_to, current_date()
      FROM {gl_master} WHERE parent_gl_code IS NOT NULL
    """,
)

REPORT_LINE_MAPPING = StructuredBuilder(
    name="report_line_mapping",
    node_sql="""
      SELECT 'RegulatoryReport' node_type, concat(report_code, ' line ', line_id) name,
             concat(report_code, '#', line_id) canonical_key,
             map('regulator', regulator, 'frequency', frequency) properties,
             0 masking_tier, 'REG_MAPPING' source_system, effective_from valid_from, effective_to valid_to
      FROM {report_lines}
    """,
    edge_sql="""
      SELECT 'GLAccount', gl_code, 'RegulatoryReport', concat(report_code, '#', line_id), 'MAPS_TO_LINE',
             coalesce(weight, 1.0), map('sign', sign), 0, 'REG_MAPPING', effective_from, effective_to, current_date()
      FROM {report_line_gl_map}
    """,
)

COUNTERPARTY_EXPOSURE = StructuredBuilder(
    name="counterparty_exposure",
    node_sql="""
      SELECT 'Counterparty' node_type, cp_name name, coalesce(cp_lei, cp_id) canonical_key,
             map('sector', sector, 'rating', internal_rating, 'country', country) properties,
             2 masking_tier, 'CREDIT_DM' source_system, current_date() valid_from, cast(NULL as date) valid_to
      FROM {counterparties}
    """,
    edge_sql="""
      SELECT 'LegalEntity', booking_lei, 'Counterparty', coalesce(cp_lei, cp_id), 'EXPOSED_TO',
             cast(ead_cad as double), map('product', product_type, 'ccy', currency), 2, 'CREDIT_DM',
             business_date, date_add(business_date, 1), business_date
      FROM {exposures}
    """,
)

METRIC_TREE = StructuredBuilder(
    name="metric_tree",
    node_sql="""
      SELECT 'Metric' node_type, metric_name name, metric_code canonical_key,
             map('unit', unit, 'owner', owner_bu) properties, 0, 'TREASURY_DM',
             effective_from, effective_to FROM {metrics}
    """,
    edge_sql="""
      SELECT 'Metric', metric_code, 'Metric', parent_metric_code, 'ROLLS_UP_TO', coalesce(weight,1.0), map(), 0, 'TREASURY_DM',
             effective_from, effective_to, current_date() FROM {metrics} WHERE parent_metric_code IS NOT NULL
      UNION ALL
      SELECT 'Metric', metric_code, 'RegulatoryReport', report_line_key, 'REPORTED_IN', 1.0, map(), 0, 'TREASURY_DM',
             effective_from, effective_to, current_date() FROM {metric_report_map}
    """,
)

BUILDERS = [LEGAL_ENTITY_HIERARCHY, GL_HIERARCHY, REPORT_LINE_MAPPING, COUNTERPARTY_EXPOSURE, METRIC_TREE]


def run_structured_builders(spark, sources: dict[str, str], catalog: str, schema: str, run_ts: str) -> None:
    """Materialise all deterministic nodes/edges into staging tables for the bitemporal merge."""
    from pyspark.sql import functions as F  # noqa
    node_frames, edge_frames = [], []
    for b in BUILDERS:
        try:
            nodes = spark.sql(b.node_sql.format(**sources))
            edges = spark.sql(b.edge_sql.format(**sources))
        except KeyError as e:
            print(f"[structured] skipping {b.name}: missing source {e}")
            continue
        node_frames.append(nodes.withColumn("confidence", F.lit(1.0)))
        edge_frames.append(edges.withColumn("confidence", F.lit(1.0)).withColumn("evidence_chunk_ids", F.array()))
    if not node_frames:
        return
    from functools import reduce
    all_nodes = reduce(lambda a, b: a.unionByName(b), node_frames)
    all_edges = reduce(lambda a, b: a.unionByName(b), edge_frames)
    all_nodes.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.stg_nodes_structured")
    all_edges.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.stg_edges_structured")
