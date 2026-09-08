# 03 Build pipeline

Entry points: `pipeline/jobs.py::full_build` (weekly, Sunday 02:00 Toronto) and `incremental_build` (hourly weekdays). Notebook `databricks/notebooks/01_build_graph.py` wires the LLM Gateway, embeddings and Elastic clients.

| Step | Module | Input | Output | Scale notes |
|---|---|---|---|---|
| 1 Structured edges | structured_edges.py | Silver mirrors (LE master, GL master, report line map, exposures, metrics) | stg_nodes_structured, stg_edges_structured | Pure Spark SQL. Add a `StructuredBuilder` per new source. Exposures are daily-dated edges with weight = EAD. |
| 2 Lineage | lineage.py | system.access.table_lineage | lineage_edges, stg views | 90-day lookback full, 7-day incremental |
| 3 Extraction | extraction.py | Silver chunks with `extracted_at IS NULL` | stg_extraction_payload -> stg_nodes_extracted, stg_edges_extracted, extraction_log | mapInPandas, one gateway client per task, `max_concurrency` partitions. JSON mode, temperature 0. Rejects logged, never written. |
| 4 Union + ids | jobs._union_staging | all staging | stg_nodes_all, stg_edges_all | ids computed in SQL identically to `ids.py` |
| 5 Resolution | resolution.py | stg_nodes_all | same_as | Blocked by type, country, first 4 chars. Exact key > rule > embedding (>= 0.92). Survivor = most sources, then confidence. |
| 6 Bitemporal merge | temporal.py | staging + same_as | nodes, edges | Two-pass MERGE: close old version, insert new. Edges re-pointed to survivors. |
| 7 Embeddings | embeddings.py | nodes/edges/communities missing embedding | embedding column | Batched 128, MERGE back |
| 8 Communities | communities.py | current edges (excl. MENTIONS/SAME_AS) | stg_community_members, communities, node community_l0..l2 | GraphFrames connected components, then Leiden per component (leidenalg; Louvain fallback). Summaries only for size >= 3. |
| 9 Closures | closures.py | hot-type edges, degree <= cap | closures (as_of) | Self-join per hop, best path per (src,dst). Supernodes above `max_degree_cap` are excluded from expansion and reached via 1-hop only. |
| 10 Analytics | analytics.py | exposure edges as of date | node_features | Laplacian eigen-decomposition, Fiedler vector, magnitude functional (<= 2000 nodes per component; route larger to cuGraph) |
| 11 Sync Postgres | sync_postgres.py | CDF since watermark | graph.* | idempotent upserts; closures/communities full replace per snapshot |
| 12 Sync Elastic | sync_elastic.py | chunks + MENTIONS + community labels | nova-chunks, nova-community-summaries | bulk 500, ELSER ingest pipeline |

## Tuning knobs (config/prod.yaml)
- `hot_node_types`, `hot_lookback_days`: what Postgres serves.
- `postgres.max_hops_served`: closure depth (2 is enough for most treasury questions; 3 is the default).
- `max_degree_cap`: supernode guard (currencies, regulators are naturally high degree; keep them out of closures).
- `llm.max_concurrency`: extraction parallelism against the gateway quota.
- resolution threshold in `resolve_spark(emb_threshold=0.92)`; lower it only with a review queue in place.

## Adding a new structured source
1. Land it in Silver with `effective_from/effective_to`.
2. Add a `StructuredBuilder` with node and edge SQL that emits the standard columns.
3. Add its logical name to `structured_sources`.
4. Add an ontology rule if it introduces a new edge type; mark `from_llm=False` if deterministic.

## Adding a new document corpus
1. Ingest to Silver `doc_chunks` via `chunking.chunk_document` (keeps section headings for citations).
2. Set `masking_tier` and metadata (`regulator`, `jurisdiction`, `effective_date`).
3. The next incremental run extracts, resolves, merges and indexes it.
