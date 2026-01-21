-- Find all content elements with a specific label
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
FROM {table}
WHERE :label = ANY(labels)
ORDER BY ingestion_timestamp DESC
