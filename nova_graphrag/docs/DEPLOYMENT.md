# Deployment Guide

## Target placement in NOVA

- `nova-graphrag` service runs inside the NOVA App Layer on Azure AKS/FastAPI.
- Databricks + ADLS hosts the canonical graph tables.
- Elastic and pgvector are queried through private network paths.
- LLM calls continue through NOVA's existing LLM Gateway.
- MCP/tool access should remain the governed path for structured enterprise actions.

## Environment configuration

Recommended environment variables:

```text
NOVA_ENV=dev|test|prod
NOVA_DATABRICKS_CATALOG=<catalog>
NOVA_DATABRICKS_SCHEMA=<schema>
NOVA_ELASTIC_INDEX=<index>
NOVA_PGVECTOR_DSN=<secret-backed DSN>
NOVA_PGVECTOR_TABLE=nova_semantic_chunk
NOVA_GRAPH_MAX_HOPS=5
NOVA_GRAPH_MATERIALITY_FLOOR=0.0
NOVA_APPROVED_ONLY=true
```

Secrets must come from the bank-approved secret manager and should never be stored in this repository.

## AKS service endpoints

Suggested endpoints:

- `GET /health`
- `POST /v1/retrieve` - return governed evidence package
- `POST /v1/graph/neighbors`
- `POST /v1/graph/path`
- `POST /v1/graph/impact`
- `POST /v1/graph/as-of`
- `POST /v1/admin/reindex` - privileged only
- `POST /v1/admin/projection` - privileged only

The starter repository implements `/health` and `/v1/retrieve`; the others should be added after security and entitlement contracts are finalized.

## CI/CD gates

Before deployment require:
- unit tests
- ontology validation
- schema migration validation
- data classification checks
- static security scanning
- dependency scanning
- prompt-injection regression suite
- retrieval quality tests
- graph consistency tests
- load/performance tests

## Production rollout

1. DEV: in-memory graph + synthetic data.
2. DEV Databricks: one bounded Treasury domain.
3. TEST: real schemas, masked/synthetic bank data, full security path.
4. PILOT: read-only production sources, restricted user cohort.
5. PROD phase 1: policy/data lineage and navigator use cases.
6. PROD phase 2: calculations and scenario propagation.
7. PROD phase 3: cross-domain graph expansion.
