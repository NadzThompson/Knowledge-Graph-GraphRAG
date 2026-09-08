-- NOVA graph serving layer. PostgreSQL 16+, pgvector 0.7+.
-- Holds the hot subgraph (not the full bank-wide graph) and all agent memory.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA IF NOT EXISTS graph;
CREATE SCHEMA IF NOT EXISTS agent_memory;

CREATE TABLE IF NOT EXISTS graph.nodes (
  node_id        TEXT PRIMARY KEY,
  node_type      TEXT NOT NULL,
  name           TEXT NOT NULL,
  canonical_key  TEXT NOT NULL,
  properties     JSONB NOT NULL DEFAULT '{}'::jsonb,
  masking_tier   SMALLINT NOT NULL DEFAULT 0,
  source_system  TEXT NOT NULL,
  confidence     REAL NOT NULL DEFAULT 1,
  valid_from     DATE NOT NULL,
  valid_to       DATE,
  degree         INTEGER DEFAULT 0,
  pagerank       REAL DEFAULT 0,
  community_l0   TEXT, community_l1 TEXT, community_l2 TEXT,
  embedding      vector(1536),
  synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS nodes_type_idx      ON graph.nodes (node_type);
CREATE INDEX IF NOT EXISTS nodes_key_idx       ON graph.nodes (canonical_key);
CREATE INDEX IF NOT EXISTS nodes_name_trgm     ON graph.nodes USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS nodes_props_gin     ON graph.nodes USING gin (properties jsonb_path_ops);
CREATE INDEX IF NOT EXISTS nodes_validity_idx  ON graph.nodes USING gist (daterange(valid_from, valid_to, '[)'));
-- Partitioned HNSW: one partial index per hot node type keeps recall high and build time bounded.
CREATE INDEX IF NOT EXISTS nodes_emb_hnsw ON graph.nodes USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 200);

CREATE TABLE IF NOT EXISTS graph.edges (
  edge_id            TEXT PRIMARY KEY,
  src                TEXT NOT NULL REFERENCES graph.nodes(node_id) ON DELETE CASCADE,
  dst                TEXT NOT NULL REFERENCES graph.nodes(node_id) ON DELETE CASCADE,
  edge_type          TEXT NOT NULL,
  weight             REAL NOT NULL DEFAULT 1,
  properties         JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_chunk_ids TEXT[] NOT NULL DEFAULT '{}',
  masking_tier       SMALLINT NOT NULL DEFAULT 0,
  source_system      TEXT NOT NULL,
  confidence         REAL NOT NULL DEFAULT 1,
  valid_from         DATE NOT NULL,
  valid_to           DATE,
  embedding          vector(1536),
  synced_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS edges_src_idx  ON graph.edges (src, edge_type);
CREATE INDEX IF NOT EXISTS edges_dst_idx  ON graph.edges (dst, edge_type);
CREATE INDEX IF NOT EXISTS edges_validity ON graph.edges USING gist (daterange(valid_from, valid_to, '[)'));
CREATE INDEX IF NOT EXISTS edges_emb_hnsw ON graph.edges USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 200);

CREATE TABLE IF NOT EXISTS graph.closures (
  src        TEXT NOT NULL,
  dst        TEXT NOT NULL,
  hops       SMALLINT NOT NULL,
  path       TEXT[] NOT NULL,
  edge_types TEXT[] NOT NULL,
  weight     REAL NOT NULL,
  as_of      DATE NOT NULL,
  PRIMARY KEY (src, dst, hops, as_of)
);
CREATE INDEX IF NOT EXISTS closures_src_hops ON graph.closures (src, hops, as_of);

CREATE TABLE IF NOT EXISTS graph.communities (
  community_id TEXT PRIMARY KEY,
  level        SMALLINT NOT NULL,
  parent_id    TEXT,
  member_count INTEGER NOT NULL,
  title        TEXT,
  summary      TEXT,
  key_findings TEXT[],
  rating       REAL,
  masking_tier SMALLINT NOT NULL DEFAULT 0,
  embedding    vector(1536),
  run_id       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS comm_emb_hnsw ON graph.communities USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS graph.community_members (
  community_id TEXT NOT NULL REFERENCES graph.communities(community_id) ON DELETE CASCADE,
  node_id      TEXT NOT NULL,
  PRIMARY KEY (community_id, node_id)
);
CREATE INDEX IF NOT EXISTS cm_node_idx ON graph.community_members (node_id);

CREATE TABLE IF NOT EXISTS graph.same_as (
  node_id      TEXT PRIMARY KEY,
  canonical_id TEXT NOT NULL,
  method       TEXT NOT NULL,
  score        REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS graph.node_features (
  node_id TEXT NOT NULL, as_of DATE NOT NULL,
  pagerank REAL, betweenness REAL, fiedler_coord REAL, spectral_gap REAL,
  gnn_risk_score REAL, magnitude_contrib REAL,
  PRIMARY KEY (node_id, as_of)
);

CREATE TABLE IF NOT EXISTS graph.sync_watermark (
  table_name  TEXT PRIMARY KEY,
  delta_version BIGINT NOT NULL,
  synced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------- Agent memory (replaces Redis) ----------------
-- LangGraph's PostgresSaver / PostgresStore create their own tables on setup();
-- these are NOVA-specific additions that live in the same database and transaction scope.
CREATE TABLE IF NOT EXISTS agent_memory.semantic_memory (
  memory_id   BIGSERIAL PRIMARY KEY,
  user_id     TEXT NOT NULL,
  agent_id    TEXT NOT NULL,
  scope       TEXT NOT NULL DEFAULT 'user',      -- user | team | global
  content     TEXT NOT NULL,
  entity_ids  TEXT[] NOT NULL DEFAULT '{}',
  masking_tier SMALLINT NOT NULL DEFAULT 0,
  embedding   vector(1536),
  importance  REAL NOT NULL DEFAULT 0.5,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used   TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS sm_user_idx ON agent_memory.semantic_memory (user_id, agent_id, scope);
CREATE INDEX IF NOT EXISTS sm_emb_hnsw ON agent_memory.semantic_memory USING hnsw (embedding vector_cosine_ops);

CREATE UNLOGGED TABLE IF NOT EXISTS agent_memory.cache (
  cache_key  TEXT PRIMARY KEY,
  value      JSONB NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS cache_exp_idx ON agent_memory.cache (expires_at);

CREATE TABLE IF NOT EXISTS agent_memory.retrieval_log (
  log_id     BIGSERIAL PRIMARY KEY,
  thread_id  TEXT, user_id TEXT, agent_id TEXT,
  query      TEXT NOT NULL, mode TEXT NOT NULL, as_of DATE,
  hit_ids    TEXT[] NOT NULL, latency_ms INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
