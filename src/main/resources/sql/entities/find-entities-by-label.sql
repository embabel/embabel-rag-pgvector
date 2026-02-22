SELECT id, name, description, uri, labels, properties, metadata, context_id, embedding
FROM {table}
WHERE :label = ANY(labels)
