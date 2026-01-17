-- Find chunk by ID
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
FROM {table}
WHERE id = :id AND 'Chunk' = ANY(labels)
