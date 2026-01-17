-- GIN index on labels array
CREATE INDEX IF NOT EXISTS idx_{table}_labels
ON {table}
USING GIN (labels)
