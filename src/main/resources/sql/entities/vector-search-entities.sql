SELECT id, name, description, uri, labels, properties, metadata, context_id, embedding,
       (1 - (embedding <=> CAST(:embedding AS vector))) AS score
FROM {table}
WHERE embedding IS NOT NULL
ORDER BY embedding <=> CAST(:embedding AS vector)
LIMIT :topK
