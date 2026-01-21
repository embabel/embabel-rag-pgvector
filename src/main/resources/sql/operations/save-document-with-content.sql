-- Insert or update a document with raw content
INSERT INTO {table} (id, uri, text, parent_id, labels, metadata, raw_content, content_type)
VALUES (:id, :uri, :text, :parentId, :labels::text[], :metadata::jsonb, :rawContent, :contentType)
ON CONFLICT (id) DO UPDATE SET
    uri = EXCLUDED.uri,
    text = EXCLUDED.text,
    parent_id = EXCLUDED.parent_id,
    labels = EXCLUDED.labels,
    metadata = EXCLUDED.metadata,
    raw_content = EXCLUDED.raw_content,
    content_type = EXCLUDED.content_type
