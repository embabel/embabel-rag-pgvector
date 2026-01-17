-- HNSW index for vector similarity search (cosine distance)
CREATE INDEX IF NOT EXISTS idx_{table}_embedding
ON {table}
USING hnsw (embedding vector_cosine_ops)
