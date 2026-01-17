-- Check if a root document or content root exists with the given URI
SELECT COUNT(*) FROM {table}
WHERE uri = :uri AND ('Document' = ANY(labels) OR 'ContentRoot' = ANY(labels))
