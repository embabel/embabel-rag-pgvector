-- Retrieve raw content for a document by ID
SELECT id, uri, raw_content, content_type, labels, metadata
FROM {table}
WHERE id = :id AND 'Document' = ANY(labels)
