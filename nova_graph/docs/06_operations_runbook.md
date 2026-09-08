# 06 Operations runbook

## Deploy
1. Unity Catalog: run `sql/delta/001_graph_tables.sql`. Grant the job service principal MODIFY on `nova.graph_gold`, SELECT on Silver, SELECT on `system.access`.
2. PostgreSQL 16 Flexible Server, pgvector >= 0.7: run `001_serving_schema.sql`, `002_functions.sql`. Enable Entra ID auth; the AKS workload identity gets `graph_reader` + `agent_memory_writer`; the Databricks SP gets `graph_writer`.
3. Elastic: create indices from `sql/elastic`, deploy ELSER model, run `sync_elastic.ensure_indices` once.
4. Secrets (Databricks scope `nova`): `pg_dsn`, `llm_gateway_token`, `es_api_key`.
5. Install the wheel on the job cluster with `[databricks]` extras and GraphFrames from Maven.
6. Import `databricks/workflow.json` and `workflow_incremental.json`.

## Schedules
- Full build: weekly Sunday 02:00 (communities, closures, analytics, full extraction backlog).
- Incremental: hourly weekdays (structured refresh, new chunks, resolution, merge, CDF sync, Elastic).
- `agent_memory.cache_gc()` every 5 minutes (pg_cron).
- `VACUUM ANALYZE graph.*` nightly; `REINDEX INDEX CONCURRENTLY` HNSW indices monthly or after > 30% churn.

## Quality gates (02_validate_graph.py)
dangling edges = 0; duplicate current nodes = 0; unembedded hot nodes <= 1000; extraction reject rate <= 15% over 7 days. Failing gates fail the job before sync.

## Monitoring
- Langfuse: tag gateway calls with `agent_id=graph-builder` for batch spend; runtime tools tagged with the calling agent.
- Postgres: `pg_stat_statements` on `graph.expand/search_nodes`; alert p95 > 500 ms; HNSW recall check weekly (brute-force sample of 200 queries).
- Elastic: index lag = chunks with `indexed_at IS NULL` older than 2 h.
- Sync: `graph.sync_watermark` age > 3 h alerts.

## Backfill / replay
- New ontology or prompt version: set `extracted_at = NULL` for affected doc types, run full build. Old extractions remain in `extraction_log`.
- Wrong merge in entity resolution: delete the `same_as` row, re-run `temporal.merge_edges` (edges re-point), sync. Nothing was destroyed.
- Restore a point in time: `RESTORE TABLE nova.graph_gold.nodes TO VERSION AS OF <v>`, then reset `graph.sync_watermark` to force a full resync.

## Failure modes
| Symptom | Likely cause | Action |
|---|---|---|
| Closure table explodes | supernode included | lower `max_degree_cap` or exclude type |
| Reject rate spikes | prompt drift / new doc format | inspect `extraction_log.raw_json`, adjust chunking or prompt |
| Slow expand | as_of snapshot missing | check closures for that date; fall back is `traverse` |
| Memory recall returns stale | expires_at not set | set `ttl_days` on remember for transient facts |

## Capacity guide
Delta: linear in edges; 1B edges ~ 150 GB compressed. Postgres: 20M nodes x 1536-dim float ~ 130 GB with HNSW; use `halfvec` to halve it. Elastic: ELSER sparse vectors roughly 2x raw text.
