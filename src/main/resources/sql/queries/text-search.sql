-- Full-text search using PostgreSQL tsvector
-- Uses plainto_tsquery for natural language queries
-- Normalizes ts_rank score to 0-1 range using score * 3 (capped at 1.0)
-- This maps typical ts_rank values (0.2-0.3) to user-expected ranges (0.6-0.9)
SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp,
       ts_rank(tsv, plainto_tsquery('english', :query)) as raw_score,
       LEAST(1.0, ts_rank(tsv, plainto_tsquery('english', :query)) * 3) as score
FROM {table}
WHERE 'Chunk' = ANY(labels)
    AND tsv @@ plainto_tsquery('english', :query)
ORDER BY raw_score DESC
LIMIT :topK
