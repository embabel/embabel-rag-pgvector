CREATE INDEX IF NOT EXISTS idx_{table}_labels ON {table} USING gin (labels);
CREATE INDEX IF NOT EXISTS idx_{table}_embedding ON {table} USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_{table}_properties ON {table} USING gin (properties);
CREATE INDEX IF NOT EXISTS idx_{table}_metadata ON {table} USING gin (metadata);
CREATE INDEX IF NOT EXISTS idx_{table}_context ON {table}(context_id);
CREATE INDEX IF NOT EXISTS idx_{relationshipTable}_source ON {relationshipTable}(source_id);
CREATE INDEX IF NOT EXISTS idx_{relationshipTable}_target ON {relationshipTable}(target_id)
