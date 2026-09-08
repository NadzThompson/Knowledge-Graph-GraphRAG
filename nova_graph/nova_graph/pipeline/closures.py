"""Precompute 1..k hop closures for the served subgraph (Spark), with a supernode guard."""

def build_closures(spark, catalog: str, schema: str, hot_types: list[str], max_hops: int = 3, degree_cap: int = 5000, as_of: str | None = None):
    types = ",".join(f"'{t}'" for t in hot_types)
    asof = f"date('{as_of}')" if as_of else "current_date()"
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW hot_edges AS
      WITH deg AS (SELECT src AS n, count(*) c FROM {catalog}.{schema}.edges WHERE is_current GROUP BY src)
      SELECT e.src, e.dst, e.edge_type, e.weight
      FROM {catalog}.{schema}.edges e
      JOIN {catalog}.{schema}.nodes a ON a.node_id = e.src AND a.is_current AND a.node_type IN ({types})
      JOIN {catalog}.{schema}.nodes b ON b.node_id = e.dst AND b.is_current AND b.node_type IN ({types})
      LEFT JOIN deg ON deg.n = e.src
      WHERE e.is_current AND e.edge_type NOT IN ('MENTIONS','SAME_AS','BELONGS_TO_COMMUNITY')
        AND coalesce(deg.c, 0) <= {degree_cap}
    """)
    spark.sql("CREATE OR REPLACE TEMP VIEW hop1 AS SELECT src, dst, 1 AS hops, array(src, dst) AS path, array(edge_type) AS edge_types, weight FROM hot_edges")
    prev = "hop1"
    unions = ["SELECT * FROM hop1"]
    for h in range(2, max_hops + 1):
        spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW hop{h} AS
          SELECT p.src, e.dst, {h} AS hops, concat(p.path, array(e.dst)) AS path, concat(p.edge_types, array(e.edge_type)) AS edge_types,
                 p.weight * e.weight AS weight
          FROM {prev} p JOIN hot_edges e ON e.src = p.dst
          WHERE NOT array_contains(p.path, e.dst)
        """)
        unions.append(f"SELECT * FROM hop{h}"); prev = f"hop{h}"
    spark.sql(f"""
      INSERT OVERWRITE {catalog}.{schema}.closures
      SELECT src, dst, hops, path, edge_types, weight, {asof} AS as_of
      FROM (SELECT *, row_number() OVER (PARTITION BY src, dst ORDER BY hops, weight DESC) rn FROM ({' UNION ALL '.join(unions)}))
      WHERE rn = 1
    """)
