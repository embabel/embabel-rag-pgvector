INSERT INTO {table} (id, name, description, uri, labels, properties, metadata, context_id, embedding)
VALUES (:id, :name, :description, :uri, :labels::text[], :properties::jsonb, :metadata::jsonb, :contextId, CAST(:embedding AS vector))
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    uri = EXCLUDED.uri,
    labels = EXCLUDED.labels,
    properties = EXCLUDED.properties,
    metadata = EXCLUDED.metadata,
    context_id = EXCLUDED.context_id,
    embedding = EXCLUDED.embedding
