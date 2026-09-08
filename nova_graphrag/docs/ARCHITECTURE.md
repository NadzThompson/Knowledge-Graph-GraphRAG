# NOVA Knowledge Graph & GraphRAG Architecture

## 1. Architectural principle

NOVA owns the intelligence model; infrastructure products are replaceable adapters.

**Canonical system of knowledge**
- Databricks + Delta Lake + ADLS
- Entities, relationships, evidence, ontology mappings and bitemporal facts

**Retrieval indexes**
- Elastic for lexical, filtered and hybrid search
- PostgreSQL + pgvector for dense semantic retrieval and curated vector indexes

**Proprietary engines**
- Graph Compiler
- Treasury Ontology + Entity Resolution
- Graph Engine
- GraphRAG Engine
- Evidence + Temporal Engine
- Query Planner

**Optional accelerators**
- Neo4j for specialized graph projections, very deep traversals and algorithms
- Redis IRIS for session memory, semantic cache or low-latency hot context

Neither optional component should be a source of truth.

## 2. Request flow

1. User submits a question through NOVA UI.
2. API Gateway authenticates and routes the request to the NOVA App Layer.
3. Router/Query Planner classifies intent and decomposes work.
4. GraphRAG planner decides which of these to invoke:
   - Elastic retrieval
   - pgvector retrieval
   - canonical graph lookup/traversal
   - Databricks SQL/structured data
   - deterministic calculations
   - scenario engine
5. The Graph Engine resolves canonical entities and retrieves relationships from the NOVA Native Graph Store.
6. Semantic retrievers return relevant chunks and entity vectors.
7. Evidence Engine binds source evidence, effective dates, policy versions and provenance.
8. Structured data/calculation tools return numerical results.
9. Evidence fusion produces a governed evidence package.
10. LLM receives only the evidence package plus explicit task instructions.
11. NOVA returns answer, calculations, citations, lineage, assumptions and warnings.

## 3. Why this can exceed generic Neo4j + Redis for NOVA

The target is not generic graph/database superiority. NOVA can be more capable for Corporate Treasury because the graph is semantically aware of:

- regulation -> policy -> requirement
- policy -> metric -> calculation
- calculation -> data element -> table -> source system
- legal entity -> product -> account -> position -> exposure
- scenario -> affected object -> metric -> limit -> report
- metric -> owner -> control -> evidence
- valid time and recorded time
- materiality and confidence
- calculation execution and scenario propagation

This makes the graph operational, auditable and decision-oriented rather than merely connected.

## 4. Scale strategy

At bank scale, do not traverse one undifferentiated enterprise graph for every request. Use:

- typed graph domains
- materiality thresholds
- multi-resolution graph projections
- precomputed neighborhoods and dependency closures
- partitioned Delta tables
- selective hot indexes in pgvector/Elastic
- optional Neo4j projections for graph-heavy subproblems

## 5. Graph domains

Recommended initial domains:

- Regulatory / Policy
- Liquidity
- Funding
- Capital
- ALM
- FTP
- Collateral
- Counterparty
- Legal Entity
- Data Lineage
- Calculation
- Controls
- Scenario

All domains share canonical entity identifiers so traversal can cross boundaries when required.
