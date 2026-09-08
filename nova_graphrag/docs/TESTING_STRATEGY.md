# Testing Strategy

## Unit tests

Test deterministic behavior for:
- stable entity IDs
- alias resolution
- typed relationship validation
- temporal filtering
- materiality filtering
- traversal depth
- shortest paths
- RRF fusion
- evidence binding

## Graph invariant tests

Examples:
- every relationship references existing entities
- every production relationship has evidence
- relationship type is allowed by ontology
- no overlapping active versions where uniqueness is required
- approved-only queries never return unapproved evidence
- user entitlement filters are monotonic: lower privilege can never return more restricted data

## Retrieval quality

Create a Treasury gold set with questions and expected:
- canonical entities
- relevant documents
- graph paths
- source tables
- calculations
- citations

Measure:
- entity resolution accuracy
- Recall@K
- Precision@K
- MRR/nDCG
- evidence coverage
- answer groundedness
- unsupported assertion rate

## Adversarial tests

Include:
- prompt injection inside PDFs
- conflicting policy versions
- stale embeddings
- intentionally wrong extracted relationships
- aliases that collide across legal entities
- restricted-user queries
- extremely broad graph queries
- missing source data

## Performance tests

Measure separately:
- entity resolution
- Elastic
- pgvector
- graph expansion
- Databricks structured retrieval
- calculation engine
- LLM synthesis

This separation makes it possible to justify or reject optional Redis/Neo4j acceleration based on measured bottlenecks rather than assumption.
