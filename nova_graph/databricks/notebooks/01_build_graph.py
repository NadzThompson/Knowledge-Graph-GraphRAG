# Databricks notebook source
# MAGIC %md # NOVA graph build
# MAGIC Parameters: mode=full|incremental, as_of=YYYY-MM-DD (optional)

# COMMAND ----------
dbutils.widgets.text("mode", "incremental"); dbutils.widgets.text("as_of", "")
mode, as_of = dbutils.widgets.get("mode"), dbutils.widgets.get("as_of") or None

# COMMAND ----------
import os, numpy as np, requests
from nova_graph import load_config
from nova_graph.pipeline import jobs

cfg = load_config("/Workspace/Repos/nova/nova_graph/config/prod.yaml")
cfg.postgres.dsn = dbutils.secrets.get("nova", "pg_dsn")
GATEWAY = cfg.llm.gateway_url
TOKEN = dbutils.secrets.get("nova", "llm_gateway_token")   # OAuth client credentials via the LLM gateway

def llm_factory():
    s = requests.Session(); s.headers["Authorization"] = f"Bearer {TOKEN}"; s.headers["X-Agent-Id"] = cfg.llm.agent_id
    def call(messages, model=cfg.llm.extraction_model):
        r = s.post(f"{GATEWAY}/chat/completions", json={"model": model, "messages": messages, "temperature": 0, "response_format": {"type": "json_object"}}, timeout=120)
        r.raise_for_status(); return r.json()["choices"][0]["message"]["content"]
    return call

def embed_factory():
    s = requests.Session(); s.headers["Authorization"] = f"Bearer {TOKEN}"
    def embed(texts):
        r = s.post(f"{GATEWAY}/embeddings", json={"model": cfg.llm.embedding_model, "input": texts}, timeout=120)
        r.raise_for_status(); return [d["embedding"] for d in r.json()["data"]]
    return embed

def es_factory():
    from elasticsearch import Elasticsearch
    return Elasticsearch(cfg.elastic.hosts, api_key=dbutils.secrets.get("nova", "es_api_key"))

# COMMAND ----------
if mode == "full":
    run_id = jobs.full_build(spark, cfg, llm_factory, embed_factory, es_factory, as_of=as_of)
    print("run_id", run_id)
else:
    jobs.incremental_build(spark, cfg, llm_factory, embed_factory, es_factory)
