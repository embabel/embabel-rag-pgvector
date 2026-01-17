-- Find content element by ID
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
FROM {table}
WHERE id = :id
