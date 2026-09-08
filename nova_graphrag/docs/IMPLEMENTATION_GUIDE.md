# Implementation Guide

## Phase 0 - Establish the contract

Before coding production ingestion, agree on:
- authoritative source systems
- graph ownership model
- data classification and entitlements
- approved ontology governance process
- evidence requirements
- retention and bitemporal policies
- model validation boundary

## Phase 1 - Canonical graph foundation

1. Create Delta tables from `sql/delta_schema.sql`.
2. Deploy `ontology.yaml` to a governed configuration location.
3. Implement entity ID policy.
4. Implement alias/entity-resolution service.
5. Load one bounded domain first, recommended: Policy -> Requirement -> Metric -> Calculation -> DataElement -> DataAsset.
6. Require evidence for each persisted relationship.
7. Add data-quality rules for dangling edges, invalid types, duplicate canonical entities and temporal overlap.

## Phase 2 - Semantic retrieval

1. Keep existing Elastic indexes.
2. Create pgvector schema from `sql/postgres_pgvector_schema.sql`.
3. Chunk documents by semantic structure rather than fixed token count where possible.
4. Attach canonical entity IDs and document version metadata to chunks.
5. Generate embeddings in Databricks or approved embedding service.
6. Publish indexes from Delta so Elastic/pgvector remain rebuildable derivatives.

## Phase 3 - Graph Engine

Implement:
- neighbors
- typed traversal
- dependency closure
- ancestor/descendant queries
- shortest path
- materiality-filtered expansion
- as-of traversal
- evidence retrieval
- graph projection generation

Start with Spark/Databricks jobs for heavy operations and a small serving API for low-latency reads.

## Phase 4 - GraphRAG Engine

1. Deploy deterministic query planner baseline.
2. Run Elastic + pgvector in parallel.
3. Fuse semantic results.
4. Resolve canonical graph seeds.
5. Expand relevant graph relationships.
6. Build evidence package.
7. Add structured Databricks tool calls.
8. Add deterministic risk/liquidity calculators.
9. Add constrained LLM planner only after deterministic baseline is measurable.

## Phase 5 - Production hardening

- RBAC/ABAC enforcement on graph entities and evidence
- immutable audit logging
- encrypted secrets and private networking
- row/column-level security where needed
- PII redaction/tokenization
- lineage and model version capture
- query and retrieval observability
- latency budgets
- resilience tests
- graph consistency tests
- prompt injection and malicious-document tests

## Phase 6 - Scale

At large scale:
- partition by domain and business date where appropriate
- use Delta optimization and clustering strategies based on actual access patterns
- materialize hot neighborhoods
- create graph projections by business domain and resolution
- precompute dependency closures for critical metrics
- use pgvector/Elastic only as serving indexes
- introduce Neo4j only when measured workloads justify it
- introduce Redis IRIS only when measured latency or session-memory requirements justify it

## Recommended first production slice

Build a policy-to-data lineage graph around one high-value Treasury metric. Example:

Regulation -> Policy -> Requirement -> NCF -> Calculation -> Data Element -> Databricks Table -> Source System -> Report -> Control -> Owner

This slice proves the complete architecture without needing bank-wide graph population on day one.
