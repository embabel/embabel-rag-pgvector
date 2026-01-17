-- Insert or update a chunk with embedding vector
INSERT INTO {table} (id, uri, text, urtext, parent_id, labels, metadata, embedding)
VALUES (:id, :uri, :text, :urtext, :parentId, :labels::text[], :metadata::jsonb, CAST(:embedding AS vector))
ON CONFLICT (id) DO UPDATE SET
    uri = EXCLUDED.uri,
    text = EXCLUDED.text,
    urtext = EXCLUDED.urtext,
    parent_id = EXCLUDED.parent_id,
    labels = EXCLUDED.labels,
    metadata = EXCLUDED.metadata,
    embedding = EXCLUDED.embedding
