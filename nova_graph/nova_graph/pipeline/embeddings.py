"""Embedding generation for nodes, edges, community summaries and chunks (via the LLM Gateway)."""
from __future__ import annotations
from typing import Callable

def node_text(n: dict) -> str:
    props = ", ".join(f"{k}={v}" for k, v in (n.get("properties") or {}).items() if v)
    return f"{n['node_type']}: {n['name']} [{n['canonical_key']}] {props}".strip()

def edge_text(e: dict, names: dict[str, str]) -> str:
    q = (e.get("properties") or {}).get("quote", "")
    return f"{names.get(e['src'], e['src'])} {e['edge_type'].replace('_', ' ').lower()} {names.get(e['dst'], e['dst'])}. {q}".strip()

def embed_table_spark(spark, table: str, text_fn: Callable[[dict], str], embed_factory: Callable[[], Callable[[list[str]], list[list[float]]]],
                      model: str, batch: int = 128, where: str = "is_current AND embedding IS NULL"):
    from pyspark.sql import functions as F
    df = spark.table(table).where(where)
    def _part(iterator):
        import pandas as pd
        embed = embed_factory()
        for pdf in iterator:
            texts = [text_fn(r._asdict()) for r in pdf.itertuples(index=False)]
            vecs = []
            for i in range(0, len(texts), batch):
                vecs += embed(texts[i:i + batch])
            yield pd.DataFrame({"pk": pdf.iloc[:, 0].tolist(), "embedding": vecs})
    pk = df.columns[0]
    out = df.mapInPandas(_part, schema="pk string, embedding array<float>")
    out.createOrReplaceTempView("emb_out")
    spark.sql(f"MERGE INTO {table} t USING emb_out s ON t.{pk} = s.pk AND t.is_current "
              f"WHEN MATCHED THEN UPDATE SET t.embedding = s.embedding, t.embedding_model = '{model}'")
