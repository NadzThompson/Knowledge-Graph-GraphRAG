# Infrastructure Adapters

## Databricks / Delta

`DeltaGraphStore` makes Databricks the canonical graph persistence layer. It is intentionally isolated behind the `GraphStore` interface so NOVA is not coupled to a graph vendor.

Production improvements required:
- parameterized queries rather than direct SQL string construction
- Unity Catalog permissions
- service principals / workload identity
- optimized MERGE strategy
- evidence-join hydration
- partitioning/clustering based on measured access patterns
- batched graph reads and precomputed projections

## Elastic

`ElasticRetriever` is intended for:
- lexical retrieval
- hybrid retrieval
- rich metadata filtering
- policy and document retrieval

Elastic remains an index. Its content should be reproducible from the canonical knowledge fabric.

## pgvector

`PgVectorRetriever` is intended for:
- dense semantic search
- curated entity/chunk embeddings
- agent-facing semantic indexes

The embedding dimension in the SQL example is illustrative and must match the approved embedding model.

## Optional Neo4j

If added, implement a `GraphStore` or `GraphAccelerator` adapter that consumes graph projections from Delta. It should never become the only copy of canonical graph state.

## Optional Redis IRIS

If added, implement it behind a cache/session-memory interface. It should never hold authoritative enterprise knowledge.
