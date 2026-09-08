# 07 Capability comparison: nova-graph vs Neo4j + Redis

| Capability | Neo4j + Redis | nova-graph | Notes |
|---|---|---|---|
| Bank-wide graph storage | Neo4j cluster, licensed per core, memory-bound | Delta Lake, storage-priced, unbounded | graph of record is not a hot store |
| Point-in-time (bitemporal) queries | not native; modelled by hand | native columns + functions in Delta and Postgres | regulatory as-at questions |
| Deterministic construction from structured data | via ETL to Cypher | Spark SQL builders, confidence 1.0, no LLM | most treasury edges |
| LLM extraction governance | external | ontology-validated, rejects logged, prompt versioned | model risk audit |
| Entity resolution | external or GDS similarity | blocked exact/rule/embedding with auditable `same_as`, reversible | |
| Text + vector + graph fusion | Neo4j vector index + separate text search | Elastic hybrid, pgvector, closures fused in one ranked bundle | single explain trail |
| Community summaries (global GraphRAG) | GDS Leiden (enterprise) + custom | Leiden per component on Spark, summaries indexed in both stores | |
| Deep traversals (>3 hops, patterns) | strong (Cypher) | recursive CTE to 6 hops; optional Neo4j path | the one area Neo4j wins |
| Shortest path | native | SQL function (bounded) | |
| Lineage | manual load | Unity Catalog lineage projected automatically | |
| Row-level masking | application side | in every SQL function + Unity Catalog policies | |
| Session memory | Redis keys, no query language | LangGraph Postgres checkpointer, transactional with graph writes | |
| Semantic memory | Redis vector (separate module) | pgvector with scope, tier, importance, TTL, entity links | |
| Cache | Redis, sub-ms | Postgres UNLOGGED table, low-ms; optional Redis mirror | |
| Graph ML / spectral | GDS algorithms only | Spark/PyTorch/cuGraph on the same tables; magnitude and Fiedler features shipped | GSRT research |
| Auth | separate credentials | Entra ID across Databricks, Postgres, Elastic | |
| Operational surface | two extra products, on-prem OCP round trips | existing platforms, in-region | |

## When to switch the optional paths on
- **Neo4j**: an agent workload routinely needs >= 4 hops with type patterns (contagion across counterparties, funding chains). Set `optional.neo4j_enabled=true`, `neo4j_hop_threshold=4`, add a sync task that loads `nodes/edges` via the Spark Connector. Retrieval routes automatically; everything else is unchanged.
- **Redis**: p95 for cache or checkpoint reads must be < 2 ms under peak. Set `optional.redis_enabled=true` and mirror `agent_memory.cache` write-through. Postgres remains the record.
