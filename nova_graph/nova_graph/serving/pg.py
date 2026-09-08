"""Thin, typed access to the serving functions defined in sql/postgres/002_functions.sql."""
from __future__ import annotations
from datetime import date
from typing import Optional, Sequence
import numpy as np
import psycopg
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector


class GraphStore:
    def __init__(self, dsn: str, pool_size: int = 10, ef_search: int = 80):
        self.pool = ConnectionPool(dsn, min_size=1, max_size=pool_size, configure=self._configure, open=True)
        self.ef_search = ef_search

    def _configure(self, conn: psycopg.Connection):
        register_vector(conn)
        conn.execute(f"SET hnsw.ef_search = {self.ef_search}")
        conn.commit()

    # ---- graph ----
    def search_nodes(self, q: np.ndarray, k: int = 12, types: Optional[Sequence[str]] = None, as_of: Optional[date] = None, max_tier: int = 0):
        with self.pool.connection() as c:
            return c.execute("SELECT * FROM graph.search_nodes(%s,%s,%s,%s,%s)", (q, k, list(types) if types else None, as_of, max_tier)).fetchall()

    def search_communities(self, q: np.ndarray, k: int = 6, level: Optional[int] = None, max_tier: int = 0):
        with self.pool.connection() as c:
            return c.execute("SELECT * FROM graph.search_communities(%s,%s,%s,%s)", (q, k, level, max_tier)).fetchall()

    def expand(self, seeds: Sequence[str], hops: int = 2, as_of: Optional[date] = None, edge_types: Optional[Sequence[str]] = None,
               max_tier: int = 0, limit: int = 200):
        with self.pool.connection() as c:
            return c.execute("SELECT * FROM graph.expand(%s,%s,%s,%s,%s,%s)",
                             (list(seeds), hops, as_of, list(edge_types) if edge_types else None, max_tier, limit)).fetchall()

    def traverse(self, seeds: Sequence[str], max_hops: int = 4, as_of: Optional[date] = None, edge_types=None, max_tier: int = 0, limit: int = 500):
        with self.pool.connection() as c:
            return c.execute("SELECT * FROM graph.traverse(%s,%s,%s,%s,%s,%s)",
                             (list(seeds), max_hops, as_of, list(edge_types) if edge_types else None, max_tier, limit)).fetchall()

    def shortest_path(self, a: str, b: str, max_hops: int = 6, as_of: Optional[date] = None):
        with self.pool.connection() as c:
            return c.execute("SELECT * FROM graph.shortest_path(%s,%s,%s,%s)", (a, b, max_hops, as_of)).fetchone()

    def nodes(self, ids: Sequence[str], max_tier: int = 0):
        if not ids: return []
        with self.pool.connection() as c:
            return c.execute("SELECT node_id,node_type,name,canonical_key,properties,masking_tier,valid_from,valid_to,pagerank,community_l1 "
                             "FROM graph.nodes WHERE node_id = ANY(%s) AND masking_tier <= %s", (list(ids), max_tier)).fetchall()

    def edges_between(self, ids: Sequence[str], as_of: Optional[date] = None, max_tier: int = 0):
        if not ids: return []
        with self.pool.connection() as c:
            return c.execute("""SELECT edge_id,src,dst,edge_type,weight,properties,evidence_chunk_ids,valid_from,valid_to
                                FROM graph.edges WHERE src = ANY(%s) AND dst = ANY(%s) AND masking_tier <= %s
                                  AND (%s::date IS NULL OR daterange(valid_from, valid_to, '[)') @> %s::date)""",
                             (list(ids), list(ids), max_tier, as_of, as_of)).fetchall()

    def resolve_names(self, names: Sequence[str], max_tier: int = 0, limit: int = 5):
        """Fuzzy (trigram) name -> node lookup for entity linking of user queries."""
        out = []
        with self.pool.connection() as c:
            for n in names:
                out += c.execute("SELECT node_id,node_type,name, similarity(name,%s) s FROM graph.nodes WHERE name %% %s AND masking_tier <= %s "
                                 "ORDER BY s DESC LIMIT %s", (n, n, max_tier, limit)).fetchall()
        return out

    def features(self, ids: Sequence[str], as_of: Optional[date] = None):
        with self.pool.connection() as c:
            return c.execute("""SELECT DISTINCT ON (node_id) * FROM graph.node_features WHERE node_id = ANY(%s)
                                AND (%s::date IS NULL OR as_of <= %s::date) ORDER BY node_id, as_of DESC""", (list(ids), as_of, as_of)).fetchall()

    def log_retrieval(self, thread_id, user_id, agent_id, query, mode, as_of, hit_ids, latency_ms):
        with self.pool.connection() as c:
            c.execute("INSERT INTO agent_memory.retrieval_log(thread_id,user_id,agent_id,query,mode,as_of,hit_ids,latency_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                      (thread_id, user_id, agent_id, query, mode, as_of, list(hit_ids), latency_ms)); c.commit()

    # ---- cache & memory (Redis replacement) ----
    def cache_get(self, key: str):
        with self.pool.connection() as c:
            row = c.execute("SELECT agent_memory.cache_get(%s)", (key,)).fetchone()
            return row[0] if row else None

    def cache_set(self, key: str, value: dict, ttl_s: int = 300):
        import json
        with self.pool.connection() as c:
            c.execute("SELECT agent_memory.cache_set(%s,%s,%s)", (key, json.dumps(value), ttl_s)); c.commit()

    def recall(self, q: np.ndarray, user_id: str, agent_id: str, k: int = 8, max_tier: int = 0):
        with self.pool.connection() as c:
            rows = c.execute("SELECT * FROM agent_memory.recall(%s,%s,%s,%s,%s)", (q, user_id, agent_id, k, max_tier)).fetchall()
            if rows:
                c.execute("UPDATE agent_memory.semantic_memory SET last_used=now() WHERE memory_id = ANY(%s)", ([r[0] for r in rows],)); c.commit()
            return rows

    def remember(self, user_id: str, agent_id: str, content: str, embedding: np.ndarray, entity_ids: Sequence[str] = (),
                 scope: str = "user", importance: float = 0.5, masking_tier: int = 0, ttl_days: Optional[int] = None):
        with self.pool.connection() as c:
            c.execute("""INSERT INTO agent_memory.semantic_memory (user_id,agent_id,scope,content,entity_ids,masking_tier,embedding,importance,expires_at)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s, CASE WHEN %s IS NULL THEN NULL ELSE now() + make_interval(days => %s) END)""",
                      (user_id, agent_id, scope, content, list(entity_ids), masking_tier, embedding, importance, ttl_days, ttl_days)); c.commit()
