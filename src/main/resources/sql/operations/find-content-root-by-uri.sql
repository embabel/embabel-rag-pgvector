-- Find content root by URI (Document or ContentRoot)
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
FROM {table}
WHERE uri = :uri AND ('Document' = ANY(labels) OR 'ContentRoot' = ANY(labels))
