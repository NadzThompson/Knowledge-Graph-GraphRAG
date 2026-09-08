from fastapi import FastAPI
from pydantic import BaseModel
from nova_graphrag.graph_store.memory import InMemoryGraphStore
from nova_graphrag.retrieval.memory import InMemoryRetriever
from nova_graphrag.graphrag.engine import NovaGraphRAG

app = FastAPI(title="NOVA GraphRAG API", version="0.1.0")
_store = InMemoryGraphStore()
_engine = NovaGraphRAG(_store, [InMemoryRetriever()])


class QueryRequest(BaseModel):
    query: str
    max_hops: int = 3


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/v1/retrieve")
def retrieve(req: QueryRequest):
    return _engine.retrieve(req.query, max_hops=req.max_hops).model_dump(mode="json")
