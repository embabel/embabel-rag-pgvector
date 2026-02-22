SELECT e.id, e.name, e.description, e.uri, e.labels, e.properties, e.metadata, e.context_id, e.embedding
FROM {relationshipTable} r
JOIN {table} e ON (
    CASE
        WHEN :direction = 'OUTGOING' THEN r.target_id = e.id AND r.source_id = :sourceId
        WHEN :direction = 'INCOMING' THEN r.source_id = e.id AND r.target_id = :sourceId
        ELSE (r.target_id = e.id AND r.source_id = :sourceId) OR (r.source_id = e.id AND r.target_id = :sourceId)
    END
)
WHERE r.relationship_name = :relationshipName
