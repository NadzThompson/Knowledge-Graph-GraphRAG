# 02 Data model

## Ontology (`nova_graph/ontology.py`)
Fixed, FIBO-aligned, treasury-specific. 20 node types (LegalEntity, Counterparty, Product, GLAccount, Metric, RegulatoryReport, RegulatoryGuideline, Regulator, Policy, DataAsset, Scenario, Event, ...) and 22 edge types. Each `EdgeRule` states which source and target types are allowed and whether the LLM may create it. `EXPOSED_TO`, `PARENT_OF`, `ROLLS_UP_TO`, `MAPS_TO_LINE`, `DERIVED_FROM` are deterministic only: the LLM cannot invent an exposure.

Changing the ontology is a reviewed change: update the rules, bump `PROMPT_VERSION` in extraction.py, re-run extraction for affected doc types.

## Identity
`node_id = "n_" + sha1(f"{type}|{KEY}")[:20]` where KEY is the canonical key upper-cased and trimmed (LEI, GL code, `REPORT#LINE`, guideline id, metric code). Same entity from Snowflake, a PDF and Unity Catalog lands on the same id. `same_as` records merges of ids that turned out to be the same entity with different keys.

## Bitemporal model
| Column | Meaning |
|---|---|
| valid_from / valid_to | business validity (half-open [from, to)) |
| recorded_at | when the platform learned it (system time) |
| is_current | fast path for "latest" |

Node/edge versions are never updated in place. `temporal.as_of_view(spark, ..., business_date, known_at)` gives point-in-time views on Delta; `graph.expand/traverse(p_as_of)` do the same in Postgres. Delta time travel additionally lets you audit the tables themselves.

## Masking tiers
0 internal, 1 confidential, 2 restricted (counterparties, positions), 3 highly restricted (people). Set at construction from source classification and ontology defaults, propagated to edges as max(src, dst), copied to Elastic docs. Every serving function takes `p_max_tier`; the caller tier comes from RBAC roles in `serving/masking.py`.

## Delta tables (`sql/delta`)
nodes, edges (partitioned by edge_type, business_date), same_as, communities, lineage_edges, extraction_log, closures, node_features. CDF enabled on nodes/edges for incremental sync.

## Postgres (`sql/postgres`)
graph.nodes/edges (HNSW cosine on `embedding`, GiST on validity range, trigram on name, GIN on properties), graph.closures (PK src,dst,hops,as_of), graph.communities + community_members, graph.same_as, graph.node_features, graph.sync_watermark. agent_memory.semantic_memory (HNSW), agent_memory.cache (UNLOGGED, TTL), agent_memory.retrieval_log. LangGraph creates its own checkpoint and store tables on `setup()`.

Sizing rule: Postgres holds the served subgraph only (hot types, `hot_lookback_days`), target tens of millions of vectors. If it grows past that, partition `graph.nodes` by node_type and give each partition its own HNSW index.

## Elastic (`sql/elastic`)
`nova-chunks`: text (BM25, treasury analyzer) + `text_elser` (sparse) + keyword `entity_ids`, `community_ids`, `masking_tier`, dates. `nova-community-summaries`: summary + `summary_elser`, rating, level.
