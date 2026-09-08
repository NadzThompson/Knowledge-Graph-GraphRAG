# nova-graph

Proprietary knowledge graph and GraphRAG layer for the NOVA treasury intelligence platform.
Built on infrastructure NOVA already runs: **Azure Databricks / Delta Lake** (graph of record and construction),
**PostgreSQL + pgvector** (served subgraph, agent memory, cache) and **Elasticsearch** (document retrieval).
Neo4j and Redis are optional, configuration-enabled paths, not dependencies.

```
nova_graph/
  ontology.py          fixed FIBO-aligned treasury ontology; every node/edge validates against it
  schemas.py           pydantic models shared by pipeline and serving
  config.py            YAML + env configuration
  pipeline/            Databricks graph construction (structured edges, extraction, resolution,
                       communities, embeddings, lineage, closures, analytics, bitemporal merge, sync)
  serving/             runtime: GraphStore (Postgres), DocStore (Elastic), GraphRAG retriever,
                       SemanticMemory, masking, optional Neo4j adapter
  langgraph/           tools + bootstrap for the NOVA agents
sql/                   Delta DDL, Postgres DDL + serving functions, Elastic mappings
databricks/            workflow definitions and notebooks
config/prod.yaml       environment configuration
docs/                  full documentation set (start with docs/00_index.md)
tests/                 unit tests for the pure-Python core
```

## Quick start

```bash
pip install -e ".[dev]"            # local: serving + tests
pip install -e ".[databricks]"     # on Databricks: adds pyspark, graphframes, leidenalg
pytest
```

1. Run `sql/delta/001_graph_tables.sql` in Unity Catalog, `sql/postgres/001_serving_schema.sql` then `002_functions.sql` in PostgreSQL, and create the two Elastic indices from `sql/elastic/*.json`.
2. Point `config/prod.yaml` `structured_sources` at your Silver tables.
3. Deploy `databricks/workflow.json` (weekly full build) and `workflow_incremental.json` (hourly).
4. In the FastAPI app:

```python
from nova_graph.langgraph.bootstrap import bootstrap
nova = bootstrap(embed_one=gateway_embed_one, embed_many=gateway_embed_many)
agent = create_react_agent(model, tools=nova["tools"], checkpointer=nova["checkpointer"], store=nova["store"])
agent.invoke({"messages": [...]}, config={"configurable": {"thread_id": t, "user_id": u, "roles": ["treasury_analyst"], "agent_id": "TreasuryNavigatorAgent"}})
```

See `docs/00_index.md` for the full guide.
