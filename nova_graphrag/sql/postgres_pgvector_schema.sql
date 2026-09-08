CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS nova_semantic_chunk (
  chunk_id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL,
  entity_id TEXT,
  content TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedding VECTOR(1536),
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_nova_semantic_chunk_document ON nova_semantic_chunk(document_id);
CREATE INDEX IF NOT EXISTS ix_nova_semantic_chunk_entity ON nova_semantic_chunk(entity_id);
CREATE INDEX IF NOT EXISTS ix_nova_semantic_chunk_metadata ON nova_semantic_chunk USING GIN(metadata);
-- Choose HNSW or IVFFlat based on scale, latency, recall, build cost and operating profile.
-- Example:
-- CREATE INDEX ix_nova_semantic_chunk_embedding_hnsw
-- ON nova_semantic_chunk USING hnsw (embedding vector_cosine_ops);
