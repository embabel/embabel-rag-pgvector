-- Delete all descendants using recursive CTE (includes root and all children)
WITH RECURSIVE descendants AS (
    SELECT id FROM {table} WHERE id = :rootId
    UNION ALL
    SELECT ce.id FROM {table} ce
    INNER JOIN descendants d ON ce.parent_id = d.id
)
DELETE FROM {table}
WHERE id IN (SELECT id FROM descendants)
