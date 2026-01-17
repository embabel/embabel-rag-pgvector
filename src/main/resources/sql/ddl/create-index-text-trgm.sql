-- GIN index for trigram fuzzy search
CREATE INDEX IF NOT EXISTS idx_{table}_text_trgm
ON {table}
USING GIN (text gin_trgm_ops)
