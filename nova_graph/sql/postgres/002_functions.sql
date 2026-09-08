-- Serving functions. All take masking_tier so row-level filtering happens in the database.

-- Vector entry points: nearest nodes to a query embedding, respecting validity and masking.
CREATE OR REPLACE FUNCTION graph.search_nodes(
  q vector(1536), k INT, p_types TEXT[] DEFAULT NULL, p_as_of DATE DEFAULT NULL, p_max_tier SMALLINT DEFAULT 0)
RETURNS TABLE (node_id TEXT, node_type TEXT, name TEXT, score REAL, properties JSONB)
LANGUAGE sql STABLE AS $$
  SELECT n.node_id, n.node_type, n.name, (1 - (n.embedding <=> q))::real AS score, n.properties
  FROM graph.nodes n
  WHERE n.embedding IS NOT NULL
    AND n.masking_tier <= p_max_tier
    AND (p_types IS NULL OR n.node_type = ANY(p_types))
    AND (p_as_of IS NULL OR daterange(n.valid_from, n.valid_to, '[)') @> p_as_of)
  ORDER BY n.embedding <=> q
  LIMIT k;
$$;

-- Fast neighbourhood expansion from precomputed closures (1..3 hops).
CREATE OR REPLACE FUNCTION graph.expand(
  p_seeds TEXT[], p_hops INT DEFAULT 2, p_as_of DATE DEFAULT NULL,
  p_edge_types TEXT[] DEFAULT NULL, p_max_tier SMALLINT DEFAULT 0, p_limit INT DEFAULT 200)
RETURNS TABLE (src TEXT, dst TEXT, hops SMALLINT, path TEXT[], edge_types TEXT[], weight REAL)
LANGUAGE sql STABLE AS $$
  WITH snap AS (
    SELECT COALESCE(p_as_of, (SELECT max(as_of) FROM graph.closures)) AS d)
  SELECT c.src, c.dst, c.hops, c.path, c.edge_types, c.weight
  FROM graph.closures c, snap
  JOIN graph.nodes n ON n.node_id = c.dst
  WHERE c.src = ANY(p_seeds)
    AND c.hops <= p_hops
    AND c.as_of = snap.d
    AND n.masking_tier <= p_max_tier
    AND (p_edge_types IS NULL OR c.edge_types && p_edge_types)
  ORDER BY c.hops, c.weight DESC
  LIMIT p_limit;
$$;

-- Ad hoc bounded traversal with recursive CTE (used when closures are not enough, up to 6 hops).
CREATE OR REPLACE FUNCTION graph.traverse(
  p_seeds TEXT[], p_max_hops INT DEFAULT 4, p_as_of DATE DEFAULT NULL,
  p_edge_types TEXT[] DEFAULT NULL, p_max_tier SMALLINT DEFAULT 0, p_limit INT DEFAULT 500)
RETURNS TABLE (node_id TEXT, hops INT, path TEXT[], edge_path TEXT[], total_weight REAL)
LANGUAGE sql STABLE AS $$
  WITH RECURSIVE walk AS (
    SELECT s AS node_id, 0 AS hops, ARRAY[s] AS path, ARRAY[]::text[] AS edge_path, 0::real AS total_weight
    FROM unnest(p_seeds) s
    UNION ALL
    SELECT e.dst, w.hops + 1, w.path || e.dst, w.edge_path || e.edge_type, w.total_weight + e.weight
    FROM walk w
    JOIN graph.edges e ON e.src = w.node_id
    JOIN graph.nodes n ON n.node_id = e.dst
    WHERE w.hops < p_max_hops
      AND NOT e.dst = ANY(w.path)
      AND n.masking_tier <= p_max_tier
      AND e.masking_tier <= p_max_tier
      AND (p_edge_types IS NULL OR e.edge_type = ANY(p_edge_types))
      AND (p_as_of IS NULL OR daterange(e.valid_from, e.valid_to, '[)') @> p_as_of)
  )
  SELECT node_id, hops, path, edge_path, total_weight FROM walk WHERE hops > 0
  ORDER BY hops, total_weight DESC LIMIT p_limit;
$$;

-- Shortest path between two nodes (bidirectional bounded search).
CREATE OR REPLACE FUNCTION graph.shortest_path(a TEXT, b TEXT, p_max_hops INT DEFAULT 6, p_as_of DATE DEFAULT NULL)
RETURNS TABLE (path TEXT[], edge_path TEXT[], hops INT)
LANGUAGE sql STABLE AS $$
  SELECT path, edge_path, hops FROM graph.traverse(ARRAY[a], p_max_hops, p_as_of, NULL, 3, 100000)
  WHERE node_id = b ORDER BY hops LIMIT 1;
$$;

-- Community summaries nearest a query embedding (global GraphRAG entry).
CREATE OR REPLACE FUNCTION graph.search_communities(q vector(1536), k INT, p_level INT DEFAULT NULL, p_max_tier SMALLINT DEFAULT 0)
RETURNS TABLE (community_id TEXT, level SMALLINT, title TEXT, summary TEXT, rating REAL, score REAL)
LANGUAGE sql STABLE AS $$
  SELECT c.community_id, c.level, c.title, c.summary, c.rating, (1 - (c.embedding <=> q))::real
  FROM graph.communities c
  WHERE c.embedding IS NOT NULL AND c.masking_tier <= p_max_tier AND (p_level IS NULL OR c.level = p_level)
  ORDER BY c.embedding <=> q LIMIT k;
$$;

-- Semantic memory recall.
CREATE OR REPLACE FUNCTION agent_memory.recall(
  q vector(1536), p_user TEXT, p_agent TEXT, k INT DEFAULT 8, p_max_tier SMALLINT DEFAULT 0)
RETURNS TABLE (memory_id BIGINT, content TEXT, scope TEXT, importance REAL, score REAL)
LANGUAGE sql STABLE AS $$
  SELECT m.memory_id, m.content, m.scope, m.importance, (1 - (m.embedding <=> q))::real
  FROM agent_memory.semantic_memory m
  WHERE (m.scope = 'global' OR (m.scope = 'user' AND m.user_id = p_user))
    AND (m.agent_id = p_agent OR m.agent_id = '*')
    AND m.masking_tier <= p_max_tier
    AND (m.expires_at IS NULL OR m.expires_at > now())
  ORDER BY (m.embedding <=> q) - (m.importance * 0.1) LIMIT k;
$$;

-- Cache helpers with TTL semantics (replaces Redis GET/SETEX).
CREATE OR REPLACE FUNCTION agent_memory.cache_get(p_key TEXT) RETURNS JSONB LANGUAGE sql STABLE AS $$
  SELECT value FROM agent_memory.cache WHERE cache_key = p_key AND expires_at > now();
$$;
CREATE OR REPLACE FUNCTION agent_memory.cache_set(p_key TEXT, p_val JSONB, p_ttl_s INT) RETURNS VOID LANGUAGE sql AS $$
  INSERT INTO agent_memory.cache (cache_key, value, expires_at) VALUES (p_key, p_val, now() + make_interval(secs => p_ttl_s))
  ON CONFLICT (cache_key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at;
$$;
CREATE OR REPLACE FUNCTION agent_memory.cache_gc() RETURNS BIGINT LANGUAGE sql AS $$
  WITH d AS (DELETE FROM agent_memory.cache WHERE expires_at <= now() RETURNING 1) SELECT count(*) FROM d;
$$;
