-- Insert or update a content element
INSERT INTO {table} (id, uri, text, urtext, parent_id, labels, metadata)
VALUES (:id, :uri, :text, :urtext, :parentId, :labels::text[], :metadata::jsonb)
ON CONFLICT (id) DO UPDATE SET
    uri = EXCLUDED.uri,
    text = EXCLUDED.text,
    urtext = EXCLUDED.urtext,
    parent_id = EXCLUDED.parent_id,
    labels = EXCLUDED.labels,
    metadata = EXCLUDED.metadata
