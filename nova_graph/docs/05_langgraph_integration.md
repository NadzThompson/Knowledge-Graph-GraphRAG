# 05 LangGraph integration

## Bootstrap (FastAPI startup)
```python
from nova_graph.langgraph.bootstrap import bootstrap
nova = bootstrap(embed_one=gw.embed_one, embed_many=gw.embed_many, rerank=gw.rerank)   # all via the LLM gateway
```
Returns `graph`, `docs`, `rag`, `memory`, `tools`, `checkpointer` (PostgresSaver), `store` (PostgresStore with vector index).

## Tools (`langgraph/tools.py`)
| Tool | Purpose |
|---|---|
| graph_retrieve | main GraphRAG call, all modes |
| graph_lineage | upstream/downstream of a metric, report line or table |
| graph_path | shortest path between two entities |
| graph_risk_features | PageRank, betweenness, Fiedler coordinate, spectral gap, magnitude contribution |
| memory_recall / memory_remember | entity-linked semantic memory scoped to the user |

## Config contract
Every agent invocation passes:
```python
config={"configurable": {"thread_id": ..., "user_id": ..., "roles": [...], "attributes": {...}, "agent_id": "RiskCalculator"}}
```
`roles` map to masking tier in `serving/masking.py::ROLE_TIERS`. Extend `attributes` for ABAC (legal entity scope, region).

## Per-agent defaults
| Agent | Default mode | hops |
|---|---|---|
| TreasuryNavigatorAgent | auto | 2 |
| RiskCalculator | local, temporal for as-at | 2 |
| ScenarioSimulator | drift + graph_risk_features | 3 (Neo4j path if enabled and > 3) |
| RegulatoryAffairs | local with doc_types=[guideline, policy] | 1 |
| TreasuryGovernance / lineage questions | lineage | n/a |
| AnalyticsExpert | global then local | 2 |

## Memory replaces Redis
- Session state: `PostgresSaver` (every state transition durable and replayable).
- Cross-thread memory: `PostgresStore` with vector index for LangGraph's built-in namespace API, plus `SemanticMemory` for entity-linked, tiered, TTL memories.
- Cache: `agent_memory.cache` with TTL functions; `SemanticMemory.cached(key, fn, ttl_s)`.
- The AgentResult / needs_revision bounce pattern works unchanged: revisions are new checkpoints in the same thread.

## Masking middleware
Apply NOVA's existing masking middleware to `bundle.evidence_text()` before it enters the prompt; the database already filtered by tier, the middleware handles residual PII inside chunk text.
