# 08 Security and governance

- **Identity**: Databricks job SP, AKS workload identity and analysts all authenticate with Entra ID. Postgres uses Entra auth (no passwords); Elastic uses API keys rotated by the gateway team.
- **Least privilege**: `graph_reader` (SELECT + EXECUTE on graph functions), `agent_memory_writer`, `graph_writer` (sync only). Extraction jobs never touch Postgres directly except through sync.
- **Masking**: four tiers computed at construction, enforced in SQL functions and Elastic filters, and by NOVA's masking middleware on evidence text. Persons are tier 3 and only stored as roles.
- **Audit**: `extraction_log` (every LLM extraction, prompt version, raw output), `same_as` (every merge, method, score, reviewer), `agent_memory.retrieval_log` (every retrieval with hit ids), Delta history on all tables, LangGraph checkpoints for every agent step.
- **Model risk**: extraction and summarisation prompts are versioned constants; changing them triggers a full re-extraction and quality gate run; reject rate and recall@k are tracked per version. Deterministic edges dominate, and LLM-derived edges carry `confidence <= 0.8` so downstream logic can weight them.
- **Data residency**: Delta and Postgres in Canada Central; Elastic on-prem; no data leaves the gateway boundary for LLM calls.
- **Retention**: closures keep 8 weekly snapshots; extraction_log 2 years; memory TTL per scope; retrieval_log 1 year.
- **Regulatory alignment**: bitemporal facts support OSFI and FRB point-in-time evidence; lineage edges support BCBS 239 traceability from report line to source table.
