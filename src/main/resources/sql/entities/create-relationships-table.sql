CREATE TABLE IF NOT EXISTS {relationshipTable} (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(255) NOT NULL REFERENCES {table}(id) ON DELETE CASCADE,
    target_id VARCHAR(255) NOT NULL REFERENCES {table}(id) ON DELETE CASCADE,
    relationship_name VARCHAR(255) NOT NULL,
    properties JSONB NOT NULL DEFAULT '{}',
    UNIQUE(source_id, target_id, relationship_name)
)
