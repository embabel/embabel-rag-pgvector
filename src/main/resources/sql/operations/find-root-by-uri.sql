-- Find root document by URI
SELECT id FROM {table}
WHERE uri = :uri AND 'Document' = ANY(labels)
