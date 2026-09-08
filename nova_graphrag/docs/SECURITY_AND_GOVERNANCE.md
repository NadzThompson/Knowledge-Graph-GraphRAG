# Security and Governance

NOVA's graph must inherit bank-grade controls rather than bypassing them.

## Identity and authorization

- Authenticate through Entra ID/OIDC.
- Carry user identity and entitlements through every retrieval call.
- Apply RBAC/ABAC before graph expansion and before semantic evidence is returned.
- Never retrieve restricted graph nodes and filter them later; enforce authorization at source/query time.

## Evidence integrity

- Persist source IDs, versions, timestamps and hashes.
- Distinguish extracted, inferred and approved relationships.
- High-risk relationships should require explicit approval or deterministic generation.

## Data minimization

- Store identifiers and semantic facts needed for reasoning, not unnecessary raw sensitive data.
- Keep transaction-level data in authoritative stores when possible and reference it through lineage relationships.

## Model governance

- LLM-generated graph facts must be traceable to evidence.
- Do not allow the LLM to create approved production relationships directly.
- Version prompts, extractors, embeddings and ontology releases.
- Track model/provider used for every generated relationship or answer.

## Audit

Every GraphRAG answer should be reconstructable from:
- authenticated user
- query
- plan
- retrieved entities/relationships
- semantic hits
- structured data queries
- calculations
- evidence
- model/version
- final answer
