-- NOVA graph of record on Delta Lake (Unity Catalog).  Run once per environment.
-- All graph tables are bitemporal: valid_from/valid_to = business validity,
-- recorded_at = when we learned it.  "As of" queries filter on both.

CREATE SCHEMA IF NOT EXISTS nova.graph_gold;

CREATE TABLE IF NOT EXISTS nova.graph_gold.nodes (
  node_id          STRING NOT NULL,
  node_type        STRING NOT NULL,
  name             STRING NOT NULL,
  canonical_key    STRING NOT NULL,
  properties       MAP<STRING, STRING>,
  masking_tier     INT NOT NULL DEFAULT 0,
  source_system    STRING NOT NULL,
  confidence       DOUBLE NOT NULL DEFAULT 1.0,
  valid_from       DATE NOT NULL,
  valid_to         DATE,
  recorded_at      TIMESTAMP NOT NULL,
  is_current       BOOLEAN NOT NULL DEFAULT true,
  embedding        ARRAY<FLOAT>,
  embedding_model  STRING,
  degree           BIGINT,
  pagerank         DOUBLE,
  community_l0     STRING,
  community_l1     STRING,
  community_l2     STRING
)
USING DELTA
PARTITIONED BY (node_type)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS nova.graph_gold.edges (
  edge_id            STRING NOT NULL,
  src                STRING NOT NULL,
  dst                STRING NOT NULL,
  edge_type          STRING NOT NULL,
  weight             DOUBLE NOT NULL DEFAULT 1.0,
  properties         MAP<STRING, STRING>,
  evidence_chunk_ids ARRAY<STRING>,
  masking_tier       INT NOT NULL DEFAULT 0,
  source_system      STRING NOT NULL,
  confidence         DOUBLE NOT NULL DEFAULT 1.0,
  valid_from         DATE NOT NULL,
  valid_to           DATE,
  recorded_at        TIMESTAMP NOT NULL,
  is_current         BOOLEAN NOT NULL DEFAULT true,
  business_date      DATE NOT NULL,
  embedding          ARRAY<FLOAT>
)
USING DELTA
PARTITIONED BY (edge_type, business_date)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Entity resolution output. Kept separately so every merge is auditable and reversible.
CREATE TABLE IF NOT EXISTS nova.graph_gold.same_as (
  cluster_id     STRING NOT NULL,
  node_id        STRING NOT NULL,
  canonical_id   STRING NOT NULL,      -- survivor node
  method         STRING NOT NULL,      -- exact_key | rule | embedding | manual
  score          DOUBLE NOT NULL,
  reviewed_by    STRING,
  recorded_at    TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS nova.graph_gold.communities (
  community_id   STRING NOT NULL,
  level          INT NOT NULL,
  parent_id      STRING,
  member_ids     ARRAY<STRING>,
  member_count   INT,
  title          STRING,
  summary        STRING,
  key_findings   ARRAY<STRING>,
  rating         DOUBLE,
  masking_tier   INT NOT NULL DEFAULT 0,
  embedding      ARRAY<FLOAT>,
  run_id         STRING NOT NULL,
  recorded_at    TIMESTAMP NOT NULL
) USING DELTA PARTITIONED BY (level);

-- Unity Catalog lineage projected into graph edges (DataAsset -> DataAsset).
CREATE TABLE IF NOT EXISTS nova.graph_gold.lineage_edges (
  src_asset      STRING NOT NULL,
  dst_asset      STRING NOT NULL,
  column_lineage MAP<STRING, STRING>,
  job_name       STRING,
  last_seen      TIMESTAMP NOT NULL
) USING DELTA;

-- Extraction audit: every LLM extraction call, its prompt version and raw output.
CREATE TABLE IF NOT EXISTS nova.graph_gold.extraction_log (
  chunk_id        STRING NOT NULL,
  model           STRING NOT NULL,
  prompt_version  STRING NOT NULL,
  raw_json        STRING,
  n_nodes         INT,
  n_edges         INT,
  rejected        INT,               -- items failing ontology validation
  latency_ms      INT,
  recorded_at     TIMESTAMP NOT NULL
) USING DELTA PARTITIONED BY (prompt_version);

-- Precomputed k-hop closures for the served subgraph (materialised on Databricks, synced to Postgres).
CREATE TABLE IF NOT EXISTS nova.graph_gold.closures (
  src        STRING NOT NULL,
  dst        STRING NOT NULL,
  hops       INT NOT NULL,
  path       ARRAY<STRING>,          -- node ids along the best path
  edge_types ARRAY<STRING>,
  weight     DOUBLE,
  as_of      DATE NOT NULL
) USING DELTA PARTITIONED BY (hops);

-- Graph analytics features (GSRT hooks): spectral and GNN outputs by node and date.
CREATE TABLE IF NOT EXISTS nova.graph_gold.node_features (
  node_id        STRING NOT NULL,
  as_of          DATE NOT NULL,
  pagerank       DOUBLE,
  betweenness    DOUBLE,
  fiedler_coord  DOUBLE,           -- position on the Fiedler vector of the exposure Laplacian
  spectral_gap   DOUBLE,           -- graph-level, repeated per node for convenience
  gnn_risk_score DOUBLE,
  magnitude_contrib DOUBLE,        -- contribution to the magnitude functional
  run_id         STRING NOT NULL
) USING DELTA PARTITIONED BY (as_of);
