# NOVA GraphRAG

## Retrieval model

NOVA GraphRAG is a retrieval orchestration system, not simply vector search followed by an LLM.

For a query Q, the planner may invoke:

- V_E(Q): Elastic lexical/hybrid search
- V_P(Q): pgvector dense semantic retrieval
- G(Q): graph traversal
- S(Q): structured Databricks SQL/data retrieval
- C(Q): deterministic calculations
- T(Q): temporal filtering and as-of reconstruction
- P(Q): provenance/evidence validation

The output is a governed evidence package.

## Recommended stages

### Stage 1 - Intent and safety classification
Determine whether the question is factual, analytical, scenario-based, policy-sensitive or privileged.

### Stage 2 - Entity resolution
Map text to canonical entities and aliases.

### Stage 3 - Hybrid semantic retrieval
Run Elastic and pgvector independently and fuse results using reciprocal rank fusion or a learned ranker.

### Stage 4 - Graph expansion
Use semantic hits and resolved entities as graph seeds. Traverse only relationship types relevant to the intent.

### Stage 5 - Temporal filtering
Apply as-of dates and policy/model versions before evidence is admitted.

### Stage 6 - Structured retrieval
Fetch governed numerical data from Databricks using approved query templates/tools.

### Stage 7 - Deterministic calculation / scenario execution
Where the question asks for impact, use Treasury calculation functions, not LLM arithmetic.

### Stage 8 - Evidence fusion
Build a compact package containing:
- relevant canonical entities
- graph paths
- semantic chunks
- structured results
- source evidence
- confidence
- effective dates
- assumptions
- warnings

### Stage 9 - Answer synthesis
The LLM converts the evidence package into natural language and never becomes the source of truth.

## Retrieval controls

Production GraphRAG should support:
- max_hops
- entity type allowlists
- relationship type allowlists
- materiality floor
- business date
- data classification
- user entitlement
- maximum evidence budget
- confidence threshold
- approved-only mode

## Example

Question: "If corporate deposit runoff increases by 10%, which liquidity metrics, limits and reports are affected and why?"

Planner:
1. Resolve Corporate Deposits and Runoff Treatment.
2. Traverse DEFINES, CALCULATED_FROM, DEPENDS_ON, IMPACTS, REPORTED_IN.
3. Retrieve governing policy sections using Elastic + pgvector.
4. Pull balances and runoff inputs from Databricks.
5. Execute deterministic scenario calculation.
6. Return impact path, metric deltas, affected limits/reports and evidence.
