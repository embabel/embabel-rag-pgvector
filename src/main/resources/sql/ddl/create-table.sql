-- Create content element table for PgVectorStore
CREATE TABLE IF NOT EXISTS {table} (
    id VARCHAR(255) PRIMARY KEY,
    uri TEXT,
    text TEXT,
    urtext TEXT,
    clean_text TEXT,
    tokens TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    tsv TSVECTOR,
    embedding vector({embeddingDimension}),
    parent_id VARCHAR(255),
    labels TEXT[],
    metadata JSONB,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Raw content storage for documents (original bytes before chunking)
    raw_content BYTEA,
    content_type VARCHAR(255)
)
