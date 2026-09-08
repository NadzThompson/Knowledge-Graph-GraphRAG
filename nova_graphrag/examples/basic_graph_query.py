from nova_graphrag.graph_store.memory import InMemoryGraphStore
from nova_graphrag.ingestion.compiler import GraphCompiler
from nova_graphrag.retrieval.memory import InMemoryRetriever
from nova_graphrag.graphrag.engine import NovaGraphRAG

store = InMemoryGraphStore()
compiler = GraphCompiler()

facts = [
    ("Liquidity Policy", "Policy", "policy://liquidity", "DEFINES", "NCF", "Metric", "5.3"),
    ("NCF", "Metric", "policy://liquidity", "CALCULATED_FROM", "Deposit Runoff", "Calculation", "7.1"),
    ("Deposit Runoff", "Calculation", "policy://liquidity", "DEPENDS_ON", "Corporate Deposits", "Product", "7.2"),
]

for fact in facts:
    batch = compiler.compile_fact(*fact)
    store.upsert_entities(batch.entities)
    store.upsert_relationships(batch.relationships)
    store.upsert_evidence(batch.evidence)

retriever = InMemoryRetriever({
    "chunk-1": "The liquidity policy defines NCF and deposit runoff treatment.",
    "chunk-2": "Corporate deposits contribute to stressed liquidity outflows.",
}, source="elastic-demo")

engine = NovaGraphRAG(store, [retriever])
package = engine.retrieve("What does NCF depend on?", max_hops=3)
print(package.model_dump_json(indent=2))
