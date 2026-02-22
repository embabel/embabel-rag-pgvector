CREATE TABLE IF NOT EXISTS {table} (
    id VARCHAR(255) PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    uri TEXT,
    labels TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    properties JSONB NOT NULL DEFAULT '{}',
    metadata JSONB NOT NULL DEFAULT '{}',
    context_id VARCHAR(255),
    embedding vector({embeddingDimension})
)
