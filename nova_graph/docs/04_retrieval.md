# 04 Retrieval (GraphRAG)

`serving/retriever.py::GraphRAG.retrieve(query, caller, mode, as_of, hops, entity_names)` returns a `RetrievalBundle`.

## Modes
| Mode | When | How |
|---|---|---|
| local | specific questions | Elastic hybrid (RRF of BM25 + ELSER) -> entity_ids; pgvector nearest nodes; trigram links for named entities; seeds expanded via closures; edges between the subgraph fetched; missing evidence chunks pulled by id; ranked |
| global | themes, bank-wide, across entities | community summaries from pgvector and Elastic, fused and boosted by rating; supporting chunks |
| drift | broad question needing specifics | global to pick communities, then local |
| temporal | "as at" questions | local with `as_of` applied in Elastic filters, Postgres validity ranges and closure snapshot |
| lineage | where does a number come from | recursive traversal on DERIVED_FROM, MAPS_TO_LINE, ROLLS_UP_TO, REPORTED_IN up to 6 hops |
| path | how is X connected to Y | shortest path with edge types |
| auto | default | regex cues pick the mode |

## Ranking
Chunks: Elastic score + 0.05 x sum(seed scores of their entities). Edges: 0.4 + 0.1 x seed scores, text includes the verbatim quote captured at extraction. Nodes: 0.3 + seed score. Optional cross-encoder rerank blends 50/50. Top 60 hits.

## Masking and time are enforced in the database
Every function takes `p_max_tier` and `p_as_of`; the Python layer never post-filters as the only control. A tier-1 analyst cannot receive a counterparty node even if the LLM asks for it.

## Explainability
`bundle.explain` contains seeds and their scores, expansion engine used (postgres_closures | neo4j), seed communities (drift), latency, tier. `bundle.subgraph_nodes/edges` is what the Web UI renders as the entity graph. Every retrieval is logged to `agent_memory.retrieval_log` for evaluation and audit.

## Feeding the Treasury Navigator pipeline
RETRIEVE -> `bundle.evidence_text()` (chunks with doc titles, edges with quotes, nodes, communities, paths)
CLASSIFY -> use `hits[].kind` and `metadata` (regulator, doc_type, edge_type) for the Gap 1-7 protocol
SYNTHESIZE -> answer over the evidence; citations are chunk ids and edge ids
EXPLAIN -> `bundle.explain` + subgraph

## Evaluation
Keep a golden set in `agent_memory.retrieval_log` tagged questions; measure recall@k of expected chunk/edge ids, mode selection accuracy and p95 latency per mode. Targets: local p95 < 400 ms, global < 700 ms, lineage < 1.2 s.
