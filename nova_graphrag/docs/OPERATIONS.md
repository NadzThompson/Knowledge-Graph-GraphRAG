# Operations and SRE Guide

## Service-level signals

Track:
- end-to-end response latency
- graph traversal latency
- Elastic latency
- pgvector latency
- Databricks SQL latency
- retrieval recall/precision samples
- entity-resolution confidence
- evidence coverage
- unsupported-answer rate
- stale-index lag
- graph-ingestion lag
- policy-version synchronization lag

## Data quality checks

Run continuously:
- orphan entity count
- dangling relationship count
- duplicate canonical identity rate
- invalid ontology edge count
- missing evidence rate
- temporal overlap/conflict rate
- stale semantic index count
- unapproved high-risk relationship count

## Failure behavior

NOVA should degrade explicitly:
- graph unavailable -> semantic-only response with warning
- vector layer unavailable -> graph/structured retrieval only
- structured data unavailable -> no numeric answer; state limitation
- evidence unavailable -> do not present relationship as verified

## Optional accelerators

Neo4j and Redis IRIS are deployable only behind interfaces. They must be removable without changing the canonical graph model or GraphRAG contract.
