# NOVA GraphRAG & Treasury Knowledge Graph

Production-oriented reference implementation for a proprietary GraphRAG and knowledge graph capability inside the NOVA Platform.

## Design goal

NOVA should not try to re-implement Neo4j or Redis as generic infrastructure. Instead, it should provide a Treasury-native intelligence layer that combines:

- Canonical graph persistence in Databricks Delta Lake on ADLS
- Large-scale graph computation in Spark/Databricks
- Hybrid semantic retrieval using Elastic and PostgreSQL + pgvector
- Treasury ontology and entity resolution
- Bitemporal graph facts and evidence lineage
- GraphRAG query planning and evidence fusion
- Deterministic calculations and structured data retrieval
- Optional Neo4j and Redis IRIS accelerators behind adapters, never as foundational dependencies

The result can be more capable for NOVA's Corporate Treasury use case than a generic Neo4j + Redis deployment because it understands policy, data, calculations, controls, ownership, lineage, temporal validity, materiality and scenario propagation together.

## Repository layout

- `src/nova_graphrag/` core Python package
- `sql/` Delta and PostgreSQL/pgvector schemas
- `config/ontology.yaml` starter Treasury ontology
- `docs/` detailed architecture, data model, GraphRAG flow, deployment, security and operations guides
- `tests/` unit tests for core graph behavior
- `examples/` example usage

## Core architecture

1. Enterprise sources -> Databricks/ADLS ingestion
2. Graph Compiler -> entity extraction, resolution, relationship extraction, evidence binding
3. NOVA Native Graph Store -> Delta tables for entities, relationships, evidence, ontology and temporal facts
4. Semantic indexes -> Elastic + pgvector
5. NOVA Graph Engine -> traversal, dependency, path, impact and as-of queries
6. NOVA GraphRAG Engine -> planner + hybrid retrieval + graph expansion + structured data + evidence fusion
7. NOVA agents -> consume a governed evidence package, not raw unverified chunks

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest
```

Run the example:

```bash
python examples/basic_graph_query.py
```

## Important implementation note

This repository is a production-oriented foundation, not a drop-in deployment artifact. The Databricks, Elastic and pgvector adapters require environment-specific connection configuration, credentials, network controls and table/index names before production use.

See `docs/IMPLEMENTATION_GUIDE.md` for the recommended rollout sequence.
