-- Vector similarity search using pgvector cosine distance
-- Returns chunks with embedding vectors, scored by similarity
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp,
       (1 - (embedding <=> CAST(:embedding AS vector))) AS score
FROM {table}
WHERE 'Chunk' = ANY(labels)
    AND embedding IS NOT NULL
ORDER BY embedding <=> CAST(:embedding AS vector)
LIMIT :topK
