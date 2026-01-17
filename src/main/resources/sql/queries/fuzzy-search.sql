-- Fuzzy search using pg_trgm trigram similarity
-- Used as fallback when hybrid search returns no results
-- Handles typos, misspellings, and partial matches
SELECT id,
       uri,
       text,
       urtext,
       parent_id,
       labels,
       metadata,
       ingestion_timestamp,
       (
           SELECT MAX(similarity(w, :query))
           FROM unnest(tokens) AS w
       ) AS score
FROM {table}
WHERE 'Chunk' = ANY(labels)
    AND (
        SELECT MAX(similarity(w, :query))
        FROM unnest(tokens) AS w
    ) > :threshold
ORDER BY score DESC
LIMIT :topK
