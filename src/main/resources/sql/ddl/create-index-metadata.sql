-- GIN index for metadata JSONB
CREATE INDEX IF NOT EXISTS idx_{table}_metadata
ON {table}
USING GIN (metadata)
