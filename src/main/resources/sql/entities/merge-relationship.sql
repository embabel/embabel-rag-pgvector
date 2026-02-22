INSERT INTO {relationshipTable} (source_id, target_id, relationship_name, properties)
VALUES (:sourceId, :targetId, :relationshipName, :properties::jsonb)
ON CONFLICT (source_id, target_id, relationship_name) DO UPDATE SET
    properties = EXCLUDED.properties
