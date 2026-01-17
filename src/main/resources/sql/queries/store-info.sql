-- Get store statistics: counts of documents, chunks, and content elements
SELECT
    COUNT(*) FILTER (WHERE 'Document' = ANY(labels)) AS document_count,
    COUNT(*) FILTER (WHERE 'Chunk' = ANY(labels)) AS chunk_count,
    COUNT(*) AS content_element_count
FROM {table}
