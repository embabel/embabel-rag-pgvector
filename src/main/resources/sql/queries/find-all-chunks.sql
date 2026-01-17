-- Find all chunks
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
FROM {table}
WHERE 'Chunk' = ANY(labels)
