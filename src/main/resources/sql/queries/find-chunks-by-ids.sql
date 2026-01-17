-- Find chunks by their IDs
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
FROM {table}
WHERE id = ANY(:ids) AND 'Chunk' = ANY(labels)
