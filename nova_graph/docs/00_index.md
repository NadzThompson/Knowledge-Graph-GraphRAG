# NOVA Knowledge Graph and GraphRAG: Documentation

| Doc | What it covers |
|---|---|
| 01_architecture.md | Design, component responsibilities, data flow, why this beats a Neo4j + Redis stack for NOVA |
| 02_data_model.md | Ontology, bitemporal model, Delta / Postgres / Elastic schemas, ids, masking tiers |
| 03_build_pipeline.md | Every construction step in order, with inputs, outputs, scale notes and tuning knobs |
| 04_retrieval.md | GraphRAG modes, ranking, explainability, how it feeds RETRIEVE, CLASSIFY, SYNTHESIZE, EXPLAIN |
| 05_langgraph_integration.md | Tools, checkpointer and store wiring, memory, config contract per agent |
| 06_operations_runbook.md | Deployment, schedules, quality gates, monitoring, backfills, failure recovery, capacity |
| 07_capability_comparison.md | Feature by feature comparison against Neo4j + Redis, and when to switch the optional paths on |
| 08_security_governance.md | Auth, masking, audit trails, model risk and regulatory considerations |
| 09_roadmap.md | Phase plan and GSRT research hooks |

Conventions: business dates are `DATE`; system time is `TIMESTAMP` UTC; node ids are `n_` + sha1 prefix of `type|KEY`; all SQL is Spark SQL unless in `sql/postgres`.
