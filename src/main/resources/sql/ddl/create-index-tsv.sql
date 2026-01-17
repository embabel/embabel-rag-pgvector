-- GIN index for full-text search
CREATE INDEX IF NOT EXISTS idx_{table}_tsv
ON {table}
USING GIN (tsv)
