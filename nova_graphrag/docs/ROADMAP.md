# NOVA Graph Intelligence Roadmap

## Release 0.1 - Foundation
- Canonical graph schema
- Treasury ontology seed
- Graph Compiler skeleton
- Graph Engine abstraction
- Elastic + pgvector retriever interfaces
- Evidence package contract
- deterministic query planner baseline

## Release 0.2 - Treasury Navigator graph
- policy -> requirement -> metric -> calculation -> data lineage
- document versioning
- bitemporal graph queries
- human approval workflow for extracted facts
- citation-quality evidence binding

## Release 0.3 - Analytical graph
- structured Databricks retrieval
- materiality-aware traversal
- calculation graph
- report/control/owner relationships
- impact analysis

## Release 0.4 - Scenario graph
- scenario propagation
- liquidity/funding dependency graph
- legal entity and currency projections
- reusable what-if execution graph

## Release 0.5 - Enterprise scale
- graph partitions and multi-resolution projections
- precomputed dependency closures
- learned hybrid ranker
- workload-aware query planner
- optional Neo4j projection adapter if deep graph workloads justify it
- optional Redis IRIS cache adapter if latency/session workloads justify it
