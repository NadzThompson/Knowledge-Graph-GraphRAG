# Databricks notebook source
# MAGIC %md # Graph quality gates (fail the job if any breach)
# COMMAND ----------
from nova_graph import load_config
cfg = load_config("/Workspace/Repos/nova/nova_graph/config/prod.yaml"); cat, sch = cfg.delta.catalog, cfg.delta.schema
checks = {
  "dangling_edges": f"SELECT count(*) FROM {cat}.{sch}.edges e LEFT ANTI JOIN {cat}.{sch}.nodes n ON n.node_id=e.src WHERE e.is_current",
  "duplicate_current_nodes": f"SELECT count(*) FROM (SELECT node_id FROM {cat}.{sch}.nodes WHERE is_current GROUP BY node_id HAVING count(*)>1)",
  "unembedded_hot_nodes": f"SELECT count(*) FROM {cat}.{sch}.nodes WHERE is_current AND embedding IS NULL AND node_type IN ({','.join(repr(t) for t in cfg.hot_node_types)})",
  "extraction_reject_rate_pct": f"SELECT 100*sum(rejected)/greatest(sum(n_nodes+n_edges+rejected),1) FROM {cat}.{sch}.extraction_log WHERE recorded_at > current_timestamp() - INTERVAL 7 DAYS",
}
limits = {"dangling_edges": 0, "duplicate_current_nodes": 0, "unembedded_hot_nodes": 1000, "extraction_reject_rate_pct": 15}
failed = []
for name, sql in checks.items():
    v = float(spark.sql(sql).collect()[0][0] or 0); print(name, v)
    if v > limits[name]: failed.append((name, v))
assert not failed, f"quality gates failed: {failed}"
