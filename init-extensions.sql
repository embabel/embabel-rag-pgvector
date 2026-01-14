-- Initialize PostgreSQL extensions for hybrid search
-- This script runs automatically when the container starts for the first time

-- pgvector: Vector similarity search
CREATE EXTENSION IF NOT EXISTS vector;

-- pg_trgm: Trigram-based fuzzy text matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Verify extensions are installed
DO $$
BEGIN
    RAISE NOTICE 'Extensions installed:';
    RAISE NOTICE '  - vector: %', (SELECT extversion FROM pg_extension WHERE extname = 'vector');
    RAISE NOTICE '  - pg_trgm: %', (SELECT extversion FROM pg_extension WHERE extname = 'pg_trgm');
END $$;
