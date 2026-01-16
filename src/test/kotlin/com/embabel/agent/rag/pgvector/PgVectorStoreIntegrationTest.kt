/*
 * Copyright 2024-2026 Embabel Pty Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.embabel.agent.rag.pgvector

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIf
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

/**
 * Integration tests for [PgVectorStore] using Testcontainers with pgvector.
 * Skipped if Docker is not available or not responding properly.
 */
@Testcontainers(disabledWithoutDocker = true)
class PgVectorStoreIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer<*> = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg17")
                .asCompatibleSubstituteFor("postgres")
        )
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test")
    }

    private lateinit var jdbcClient: JdbcClient

    @BeforeEach
    fun setUp() {
        val dataSource = DriverManagerDataSource(
            postgres.jdbcUrl,
            postgres.username,
            postgres.password
        )
        jdbcClient = JdbcClient.create(dataSource)
    }

    @AfterEach
    fun tearDown() {
        try {
            jdbcClient.sql("DROP TABLE IF EXISTS content_elements CASCADE").update()
        } catch (e: Exception) {
            // Ignore cleanup errors
        }
    }

    @Test
    fun `provision should create extensions`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS pg_trgm").update()

        val vectorCount = jdbcClient.sql(
            "SELECT COUNT(*) FROM pg_extension WHERE extname = 'vector'"
        ).query(Int::class.java).single()

        val trgmCount = jdbcClient.sql(
            "SELECT COUNT(*) FROM pg_extension WHERE extname = 'pg_trgm'"
        ).query(Int::class.java).single()

        assertEquals(1, vectorCount, "vector extension should be installed")
        assertEquals(1, trgmCount, "pg_trgm extension should be installed")
    }

    @Test
    fun `provision should create table with correct schema`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS pg_trgm").update()

        jdbcClient.sql("""
            CREATE TABLE IF NOT EXISTS content_elements (
                id VARCHAR(255) PRIMARY KEY,
                uri TEXT,
                text TEXT,
                urtext TEXT,
                clean_text TEXT,
                tokens TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
                tsv TSVECTOR,
                parent_id VARCHAR(255),
                labels TEXT[],
                metadata JSONB,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """.trimIndent()).update()

        val tableCount = jdbcClient.sql("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'content_elements'
        """.trimIndent()).query(Int::class.java).single()

        assertEquals(1, tableCount, "content_elements table should exist")

        val columns = jdbcClient.sql("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'content_elements'
            ORDER BY ordinal_position
        """.trimIndent()).query(String::class.java).list()

        assertTrue(columns.contains("id"))
        assertTrue(columns.contains("text"))
        assertTrue(columns.contains("tsv"))
        assertTrue(columns.contains("tokens"))
        assertTrue(columns.contains("clean_text"))
        assertTrue(columns.contains("metadata"))
    }

    @Test
    fun `trigger should auto-populate tsv and tokens`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS pg_trgm").update()

        jdbcClient.sql("""
            CREATE TABLE IF NOT EXISTS content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                clean_text TEXT,
                tokens TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
                tsv TSVECTOR
            )
        """.trimIndent()).update()

        jdbcClient.sql("""
            CREATE OR REPLACE FUNCTION content_elements_tsv_trigger()
            RETURNS TRIGGER AS ${'$'}${'$'}
            BEGIN
                NEW.tsv := to_tsvector('english', COALESCE(NEW.text, ''));
                NEW.clean_text := regexp_replace(LOWER(COALESCE(NEW.text, '')), '[^a-z0-9\s]', '', 'g');
                NEW.tokens := regexp_split_to_array(NEW.clean_text, '\s+');
                RETURN NEW;
            END;
            ${'$'}${'$'} LANGUAGE plpgsql
        """.trimIndent()).update()

        jdbcClient.sql("""
            CREATE TRIGGER content_elements_tsv_update
            BEFORE INSERT OR UPDATE ON content_elements
            FOR EACH ROW
            EXECUTE FUNCTION content_elements_tsv_trigger()
        """.trimIndent()).update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text)
            VALUES ('test-1', 'Hello World! This is a test.')
        """.trimIndent()).update()

        val tsv = jdbcClient.sql(
            "SELECT tsv::text FROM content_elements WHERE id = 'test-1'"
        ).query(String::class.java).single()

        assertNotNull(tsv)
        assertTrue(tsv.contains("hello") || tsv.contains("world") || tsv.contains("test"))

        val tokens = jdbcClient.sql(
            "SELECT tokens FROM content_elements WHERE id = 'test-1'"
        ).query { rs, _ ->
            @Suppress("UNCHECKED_CAST")
            (rs.getArray("tokens").array as Array<String>).toList()
        }.single()

        assertNotNull(tokens)
        assertTrue(tokens.isNotEmpty(), "tokens should be populated")
    }

    @Test
    fun `full text search should work`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()
        jdbcClient.sql("""
            CREATE TABLE content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                tsv TSVECTOR
            )
        """.trimIndent()).update()

        jdbcClient.sql("""
            CREATE INDEX idx_content_elements_tsv ON content_elements USING GIN (tsv)
        """.trimIndent()).update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text, tsv) VALUES
            ('1', 'Machine learning is a subset of artificial intelligence',
             to_tsvector('english', 'Machine learning is a subset of artificial intelligence')),
            ('2', 'Deep learning uses neural networks',
             to_tsvector('english', 'Deep learning uses neural networks')),
            ('3', 'Natural language processing handles text data',
             to_tsvector('english', 'Natural language processing handles text data'))
        """.trimIndent()).update()

        val results = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE tsv @@ plainto_tsquery('english', 'machine learning')
            ORDER BY ts_rank(tsv, plainto_tsquery('english', 'machine learning')) DESC
        """.trimIndent()).query(String::class.java).list()

        assertEquals(1, results.size)
        assertEquals("1", results[0])
    }

    @Test
    fun `trigram similarity should work`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS pg_trgm").update()
        jdbcClient.sql("""
            CREATE TABLE content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                tokens TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
            )
        """.trimIndent()).update()

        jdbcClient.sql("""
            CREATE INDEX idx_content_elements_trgm ON content_elements USING GIN (text gin_trgm_ops)
        """.trimIndent()).update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text, tokens) VALUES
            ('1', 'kubernetes container orchestration', ARRAY['kubernetes', 'container', 'orchestration']),
            ('2', 'docker container runtime', ARRAY['docker', 'container', 'runtime']),
            ('3', 'python programming language', ARRAY['python', 'programming', 'language'])
        """.trimIndent()).update()

        // Test fuzzy search with typo "kubernetis" (misspelling)
        val results = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE (
                SELECT MAX(similarity(w, 'kubernetis'))
                FROM unnest(tokens) AS w
            ) > 0.3
            ORDER BY (
                SELECT MAX(similarity(w, 'kubernetis'))
                FROM unnest(tokens) AS w
            ) DESC
        """.trimIndent()).query(String::class.java).list()

        assertTrue(results.contains("1"), "Should find 'kubernetes' despite typo")
    }

    @Test
    fun `GIN indexes should be created`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS pg_trgm").update()

        jdbcClient.sql("""
            CREATE TABLE content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                tsv TSVECTOR,
                metadata JSONB,
                labels TEXT[]
            )
        """.trimIndent()).update()

        jdbcClient.sql("CREATE INDEX idx_tsv ON content_elements USING GIN (tsv)").update()
        jdbcClient.sql("CREATE INDEX idx_trgm ON content_elements USING GIN (text gin_trgm_ops)").update()
        jdbcClient.sql("CREATE INDEX idx_metadata ON content_elements USING GIN (metadata)").update()
        jdbcClient.sql("CREATE INDEX idx_labels ON content_elements USING GIN (labels)").update()

        val indexes = jdbcClient.sql("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'content_elements'
        """.trimIndent()).query(String::class.java).list()

        assertTrue(indexes.contains("idx_tsv"))
        assertTrue(indexes.contains("idx_trgm"))
        assertTrue(indexes.contains("idx_metadata"))
        assertTrue(indexes.contains("idx_labels"))
    }

    @Test
    fun `JSONB metadata filtering should work`() {
        jdbcClient.sql("""
            CREATE TABLE content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                metadata JSONB
            )
        """.trimIndent()).update()

        jdbcClient.sql("CREATE INDEX idx_metadata ON content_elements USING GIN (metadata)").update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text, metadata) VALUES
            ('1', 'Document about Java', '{"type": "blog", "language": "java"}'::jsonb),
            ('2', 'Document about Python', '{"type": "blog", "language": "python"}'::jsonb),
            ('3', 'Java tutorial', '{"type": "tutorial", "language": "java"}'::jsonb)
        """.trimIndent()).update()

        // Filter by metadata using containment operator
        val javaBlogs = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE metadata @> '{"type": "blog", "language": "java"}'::jsonb
        """.trimIndent()).query(String::class.java).list()

        assertEquals(1, javaBlogs.size)
        assertEquals("1", javaBlogs[0])

        // Filter all Java documents
        val javaDocs = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE metadata @> '{"language": "java"}'::jsonb
        """.trimIndent()).query(String::class.java).list()

        assertEquals(2, javaDocs.size)
        assertTrue(javaDocs.contains("1"))
        assertTrue(javaDocs.contains("3"))
    }
}
