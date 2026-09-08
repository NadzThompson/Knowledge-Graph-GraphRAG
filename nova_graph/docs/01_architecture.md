# 01 Architecture

## Principle
Separate the **graph of record** from the **served graph**. The graph of record is a set of Delta tables: bank-wide, bitemporal, cheap to store, governed by Unity Catalog. The served graph is the hot subgraph that agents query in milliseconds, held in PostgreSQL with pgvector next to the AKS cluster. Elasticsearch remains the entry point for text. No component is a single-vendor graph engine.

## Components and responsibilities

| Component | Owns | Never does |
|---|---|---|
| Databricks / Delta (Unity Catalog + ADLS) | full graph, extraction, entity resolution, communities, embeddings, closures, analytics, lineage | serve agent queries |
| PostgreSQL + pgvector (Azure Flexible Server) | hot subgraph, closures, community summaries, node/edge/summary vectors, LangGraph checkpoints and store, semantic memory, cache | bank-wide storage |
| Elasticsearch (ELSER + BM25) | chunk and community-summary retrieval with graph ids attached | traversal |
| LLM Gateway | all LLM and embedding calls, batch and runtime | direct provider access |
| Unity Catalog | lineage, row/column masking policies, table ACLs | application logic |
| Neo4j (optional) | traversals >= `neo4j_hop_threshold` when enabled | graph of record |
| Redis (optional) | mirrored cache / session state when enabled | memory of record |

## Flows
**Build time (Databricks, weekly full + hourly incremental)**
1. Structured sources -> deterministic nodes/edges (no LLM): LE hierarchy, GL tree, report line mapping, exposures, metric tree.
2. Unity Catalog lineage -> DataAsset nodes and DERIVED_FROM edges.
3. Silver chunks -> LLM extraction constrained to the ontology; rejects logged.
4. Union -> entity resolution (exact key, rule, embedding within blocks) -> `same_as`.
5. Bitemporal MERGE into `nodes` / `edges`; edges re-pointed through `same_as`.
6. Embeddings for nodes, edges, community summaries.
7. Leiden hierarchy per connected component -> community summaries (LLM) -> ratings.
8. 1..3 hop closures for hot types; spectral/centrality features on the exposure graph.
9. CDF-driven sync to Postgres; chunk and summary indexing in Elastic with `entity_ids` / `community_ids`.

**Query time (AKS)**
1. Agent calls `graph_retrieve` (mode auto or explicit).
2. Elastic hybrid search returns chunks with entity ids; pgvector returns nearest nodes; trigram lookup links names the user typed.
3. Seeds expand via closure tables (or recursive CTE / optional Neo4j for deep hops), filtered by validity date and masking tier in SQL.
4. Ranked evidence (chunks, edges with quotes, nodes, communities, paths) plus the subgraph and an explain block return to the agent.
5. Session state, semantic memory and cache reads/writes hit the same Postgres in the same transaction scope.

## Why this beats Neo4j + Redis for NOVA
- **Scale economics.** Billions of edges are Delta files, not a licensed causal cluster. Postgres holds only what agents touch.
- **Time is native.** Every fact is bitemporal; "LCR exposure graph as at Q1 close, as known on filing date" is a filter. Neo4j has no native bitemporal model.
- **One governance plane.** Masking tiers are computed once in Databricks and enforced in every SQL function; Unity Catalog policies cover the source tables. Nothing is re-implemented in a second product.
- **Deterministic first.** Most edges come from structured data with confidence 1.0; the LLM only fills the unstructured gaps. Extraction is audited row by row.
- **Retrieval is hybrid by construction.** Text, vectors and structure fuse in one call with an explain trail, rather than a text search glued to a separate graph query.
- **Research surface.** Spectral, magnitude and GNN features run on Spark/PyTorch over the same tables; Neo4j GDS is a closed, memory-bound library.
- **Memory is transactional.** LangGraph checkpoints, semantic memory and cache live with the graph; a session write and a memory write commit together. Redis cannot join, filter by masking tier, or search vectors with SQL predicates.

The trade-off is acknowledged: variable-length pattern matching beyond 3 to 4 hops is slower in SQL than in a native graph engine. That is exactly what the optional Neo4j path is for, and it is gated by a single config flag and hop threshold.
