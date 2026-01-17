-- Hybrid search combining vector similarity and full-text search
-- Phase 1: Uses FTS as cheap prefilter, then computes vector similarity on matches
-- Combines scores with configurable weights (default: 70% vector, 30% FTS)
WITH fts AS (
    SELECT id,
           ts_rank(tsv, websearch_to_tsquery('english', :query)) AS raw_fts_score,
           ts_rank(tsv, websearch_to_tsquery('english', :query)) /
               (1 + ts_rank(tsv, websearch_to_tsquery('english', :query))) AS fts_score
    FROM {table}
    WHERE 'Chunk' = ANY(labels)
        AND tsv @@ websearch_to_tsquery('english', :query)
)
SELECT dc.id,
       dc.uri,
       dc.text,
       dc.urtext,
       dc.parent_id,
       dc.labels,
       dc.metadata,
       dc.ingestion_timestamp,
       (1 - (dc.embedding <=> CAST(:embedding AS vector))) AS vec_score,
       f.fts_score,
       :vectorWeight * (1 - (dc.embedding <=> CAST(:embedding AS vector))) + :ftsWeight * f.fts_score AS score
FROM fts f
JOIN {table} dc ON f.id = dc.id
ORDER BY score DESC
LIMIT :topK
