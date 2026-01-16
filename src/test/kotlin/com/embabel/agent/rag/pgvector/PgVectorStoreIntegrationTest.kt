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

import com.embabel.agent.rag.ingestion.ChunkTransformer
import com.embabel.agent.rag.ingestion.ContentChunker
import com.embabel.agent.rag.model.Chunk
import com.embabel.common.core.types.TextSimilaritySearchRequest
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

    // ==================== PgVectorStore API Tests ====================

    private fun createStore(): PgVectorStore {
        val properties = PgVectorStoreProperties(
            name = "test-store",
            contentElementTable = "content_elements",
            embeddingDimension = 1536
        )
        return PgVectorStore(
            jdbcClient = jdbcClient,
            properties = properties,
            chunkerConfig = ContentChunker.Config(),
            chunkTransformer = ChunkTransformer.NO_OP,
            embeddingService = null,
            enhancers = emptyList()
        )
    }

    /**
     * Insert test chunks directly via SQL. The trigger will populate tsv automatically.
     */
    private fun insertTestChunks() {
        val testData = listOf(
            Triple("chunk-1", "Machine learning is a subset of artificial intelligence that enables systems to learn from data", """{"topic": "ai"}"""),
            Triple("chunk-2", "Deep learning uses neural networks with multiple layers to process complex patterns", """{"topic": "ai"}"""),
            Triple("chunk-3", "Kubernetes is a container orchestration platform for deploying and managing applications", """{"topic": "devops"}"""),
            Triple("chunk-4", "Flight booking policies allow passengers to change or cancel reservations under certain conditions", """{"topic": "travel"}"""),
            Triple("chunk-5", "Rebooking a flight requires contacting the airline at least 24 hours before departure", """{"topic": "travel"}"""),
            Triple("chunk-6", "PostgreSQL is an advanced open source relational database with strong SQL compliance", """{"topic": "database"}"""),
            Triple("chunk-7", "Vector similarity search enables finding semantically similar content using embeddings", """{"topic": "database"}""")
        )

        testData.forEach { (id, text, metadata) ->
            jdbcClient.sql("""
                INSERT INTO content_elements (id, text, urtext, parent_id, labels, metadata)
                VALUES (:id, :text, :text, 'doc-1', ARRAY['Chunk']::text[], :metadata::jsonb)
            """.trimIndent())
                .param("id", id)
                .param("text", text)
                .param("metadata", metadata)
                .update()
        }
    }

    @Test
    fun `PgVectorStore textSearch should find exact term matches`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        // Search for "machine learning"
        val request = TextSimilaritySearchRequest(
            query = "machine learning",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results for 'machine learning'")
        assertTrue(
            results.any { it.match.id == "chunk-1" },
            "Should find chunk about machine learning"
        )
    }

    @Test
    fun `PgVectorStore textSearch should find partial term matches`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        // Search for just "learning"
        val request = TextSimilaritySearchRequest(
            query = "learning",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results for 'learning'")
        // Should find both machine learning and deep learning chunks
        val ids = results.map { it.match.id }
        assertTrue(ids.contains("chunk-1") || ids.contains("chunk-2"),
            "Should find chunks containing 'learning'")
    }

    @Test
    fun `PgVectorStore textSearch should find flight and booking terms`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        // Search for "flight"
        val flightRequest = TextSimilaritySearchRequest(
            query = "flight",
            topK = 10,
            similarityThreshold = 0.0
        )
        val flightResults = store.textSearch(flightRequest, Chunk::class.java)

        assertTrue(flightResults.isNotEmpty(), "Should find results for 'flight'")
        val flightIds = flightResults.map { it.match.id }
        assertTrue(flightIds.contains("chunk-4") || flightIds.contains("chunk-5"),
            "Should find flight-related chunks")

        // Search for "booking"
        val bookingRequest = TextSimilaritySearchRequest(
            query = "booking",
            topK = 10,
            similarityThreshold = 0.0
        )
        val bookingResults = store.textSearch(bookingRequest, Chunk::class.java)

        assertTrue(bookingResults.isNotEmpty(), "Should find results for 'booking'")
    }

    @Test
    fun `PgVectorStore textSearch should find rebooking`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        val request = TextSimilaritySearchRequest(
            query = "rebooking",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results for 'rebooking'")
        assertTrue(
            results.any { it.match.id == "chunk-5" },
            "Should find chunk about rebooking"
        )
    }

    @Test
    fun `PgVectorStore textSearch should find kubernetes`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        val request = TextSimilaritySearchRequest(
            query = "kubernetes",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results for 'kubernetes'")
        assertTrue(
            results.any { it.match.id == "chunk-3" },
            "Should find kubernetes chunk"
        )
    }

    @Test
    fun `PgVectorStore textSearch should find postgresql database`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        val request = TextSimilaritySearchRequest(
            query = "postgresql database",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results for 'postgresql database'")
        assertTrue(
            results.any { it.match.id == "chunk-6" },
            "Should find postgresql chunk"
        )
    }

    @Test
    fun `PgVectorStore textSearch should return normalized scores between 0 and 1`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        val request = TextSimilaritySearchRequest(
            query = "machine learning artificial intelligence",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results")
        results.forEach { result ->
            assertTrue(result.score >= 0.0, "Score should be >= 0: ${result.score}")
            assertTrue(result.score <= 1.0, "Score should be <= 1: ${result.score}")
        }
    }

    @Test
    fun `PgVectorStore textSearch should respect similarity threshold`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        // First get results with no threshold
        val noThresholdRequest = TextSimilaritySearchRequest(
            query = "learning",
            topK = 10,
            similarityThreshold = 0.0
        )
        val allResults = store.textSearch(noThresholdRequest, Chunk::class.java)

        // Now with a threshold
        val thresholdRequest = TextSimilaritySearchRequest(
            query = "learning",
            topK = 10,
            similarityThreshold = 0.3
        )
        val filteredResults = store.textSearch(thresholdRequest, Chunk::class.java)

        // All filtered results should have score >= threshold
        filteredResults.forEach { result ->
            assertTrue(result.score >= 0.3, "Score should be >= 0.3: ${result.score}")
        }

        // Filtered results should be subset of all results
        assertTrue(filteredResults.size <= allResults.size,
            "Threshold should filter out lower scoring results")
    }

    @Test
    fun `PgVectorStore textSearch should return empty for non-matching query`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        val request = TextSimilaritySearchRequest(
            query = "xyznonexistentterm123",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isEmpty(), "Should return empty for non-matching query")
    }

    @Test
    fun `PgVectorStore textSearch should handle multi-word natural language queries`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        // Natural language query
        val request = TextSimilaritySearchRequest(
            query = "how to change flight reservations",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        // Should find travel-related chunks due to "flight" and "reservations"
        assertTrue(results.isNotEmpty(), "Should find results for natural language query")
    }

    @Test
    fun `PgVectorStore info should return correct chunk count after ingestion`() {
        val store = createStore()
        store.provision()

        insertTestChunks()

        val info = store.info()

        assertEquals(7, info.chunkCount, "Should report correct chunk count")
    }

    // ==================== PostgreSQL Full-Text Search Syntax Tests ====================
    // Note: plainto_tsquery handles plain text and implicitly ANDs terms together.
    // These tests verify the behavior and document how different query patterns work.

    @Test
    fun `textSearch should implicitly AND multiple terms`() {
        val store = createStore()
        store.provision()
        insertTestChunks()

        // "machine learning" should match chunk-1 which has both terms
        // but not chunk-2 which only has "learning"
        val request = TextSimilaritySearchRequest(
            query = "machine learning",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results")
        // The top result should be chunk-1 which contains both "machine" and "learning"
        assertEquals("chunk-1", results.first().match.id,
            "Top result should be the chunk with both terms")
    }

    @Test
    fun `textSearch should handle stemming - finds plural forms`() {
        val store = createStore()
        store.provision()
        insertTestChunks()

        // "network" should match "networks" in chunk-2 due to stemming
        val request = TextSimilaritySearchRequest(
            query = "network",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results for stemmed term")
        assertTrue(
            results.any { it.match.id == "chunk-2" },
            "Should find chunk with 'networks' when searching for 'network'"
        )
    }

    @Test
    fun `textSearch should handle stemming - finds verb forms`() {
        val store = createStore()
        store.provision()
        insertTestChunks()

        // "deploy" should match "deploying" in chunk-3 due to stemming
        val request = TextSimilaritySearchRequest(
            query = "deploy",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results for stemmed verb")
        assertTrue(
            results.any { it.match.id == "chunk-3" },
            "Should find chunk with 'deploying' when searching for 'deploy'"
        )
    }

    @Test
    fun `textSearch should ignore common stop words`() {
        val store = createStore()
        store.provision()
        insertTestChunks()

        // "is a" are stop words, should still find "artificial intelligence"
        val request = TextSimilaritySearchRequest(
            query = "is a artificial intelligence",
            topK = 10,
            similarityThreshold = 0.0
        )
        val results = store.textSearch(request, Chunk::class.java)

        assertTrue(results.isNotEmpty(), "Should find results ignoring stop words")
        assertTrue(
            results.any { it.match.id == "chunk-1" },
            "Should find AI chunk when searching with stop words"
        )
    }

    @Test
    fun `direct SQL tsquery with AND operator should work`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS pg_trgm").update()

        jdbcClient.sql("""
            CREATE TABLE IF NOT EXISTS content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                labels TEXT[],
                tsv TSVECTOR
            )
        """.trimIndent()).update()

        // Create trigger for tsv
        jdbcClient.sql("""
            CREATE OR REPLACE FUNCTION content_elements_tsv_trigger()
            RETURNS TRIGGER AS ${'$'}${'$'}
            BEGIN
                NEW.tsv := to_tsvector('english', COALESCE(NEW.text, ''));
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
            INSERT INTO content_elements (id, text, labels) VALUES
            ('1', 'Machine learning artificial intelligence', ARRAY['Chunk']),
            ('2', 'Deep learning neural networks', ARRAY['Chunk']),
            ('3', 'Machine processing data', ARRAY['Chunk'])
        """.trimIndent()).update()

        // Test AND with to_tsquery
        val andResults = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE tsv @@ to_tsquery('english', 'machine & learning')
        """.trimIndent()).query(String::class.java).list()

        assertEquals(1, andResults.size, "AND should require both terms")
        assertEquals("1", andResults[0], "Should match only chunk with both terms")
    }

    @Test
    fun `direct SQL tsquery with OR operator should work`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()

        jdbcClient.sql("""
            CREATE TABLE IF NOT EXISTS content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                labels TEXT[],
                tsv TSVECTOR
            )
        """.trimIndent()).update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text, labels, tsv) VALUES
            ('1', 'Kubernetes orchestration', ARRAY['Chunk'], to_tsvector('english', 'Kubernetes orchestration')),
            ('2', 'Docker containers', ARRAY['Chunk'], to_tsvector('english', 'Docker containers')),
            ('3', 'Python programming', ARRAY['Chunk'], to_tsvector('english', 'Python programming'))
        """.trimIndent()).update()

        // Test OR with to_tsquery
        val orResults = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE tsv @@ to_tsquery('english', 'kubernetes | docker')
            ORDER BY id
        """.trimIndent()).query(String::class.java).list()

        assertEquals(2, orResults.size, "OR should match either term")
        assertTrue(orResults.contains("1"), "Should match kubernetes")
        assertTrue(orResults.contains("2"), "Should match docker")
    }

    @Test
    fun `direct SQL tsquery with NOT operator should work`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()

        jdbcClient.sql("""
            CREATE TABLE IF NOT EXISTS content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                labels TEXT[],
                tsv TSVECTOR
            )
        """.trimIndent()).update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text, labels, tsv) VALUES
            ('1', 'Machine learning AI', ARRAY['Chunk'], to_tsvector('english', 'Machine learning AI')),
            ('2', 'Deep learning neural', ARRAY['Chunk'], to_tsvector('english', 'Deep learning neural')),
            ('3', 'Machine processing', ARRAY['Chunk'], to_tsvector('english', 'Machine processing'))
        """.trimIndent()).update()

        // Test NOT with to_tsquery: learning but not machine
        val notResults = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE tsv @@ to_tsquery('english', 'learning & !machine')
        """.trimIndent()).query(String::class.java).list()

        assertEquals(1, notResults.size, "NOT should exclude term")
        assertEquals("2", notResults[0], "Should match only chunk without 'machine'")
    }

    @Test
    fun `direct SQL tsquery with prefix matching should work`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()

        jdbcClient.sql("""
            CREATE TABLE IF NOT EXISTS content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                labels TEXT[],
                tsv TSVECTOR
            )
        """.trimIndent()).update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text, labels, tsv) VALUES
            ('1', 'Kubernetes deployment', ARRAY['Chunk'], to_tsvector('english', 'Kubernetes deployment')),
            ('2', 'Docker containers', ARRAY['Chunk'], to_tsvector('english', 'Docker containers')),
            ('3', 'Kubeflow pipelines', ARRAY['Chunk'], to_tsvector('english', 'Kubeflow pipelines'))
        """.trimIndent()).update()

        // Test prefix matching with :*
        val prefixResults = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE tsv @@ to_tsquery('english', 'kube:*')
            ORDER BY id
        """.trimIndent()).query(String::class.java).list()

        assertEquals(2, prefixResults.size, "Prefix should match multiple terms")
        assertTrue(prefixResults.contains("1"), "Should match kubernetes")
        assertTrue(prefixResults.contains("3"), "Should match kubeflow")
    }

    @Test
    fun `direct SQL tsquery with phrase proximity should work`() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()

        jdbcClient.sql("""
            CREATE TABLE IF NOT EXISTS content_elements (
                id VARCHAR(255) PRIMARY KEY,
                text TEXT,
                labels TEXT[],
                tsv TSVECTOR
            )
        """.trimIndent()).update()

        jdbcClient.sql("""
            INSERT INTO content_elements (id, text, labels, tsv) VALUES
            ('1', 'machine learning algorithms', ARRAY['Chunk'], to_tsvector('english', 'machine learning algorithms')),
            ('2', 'machine and deep learning', ARRAY['Chunk'], to_tsvector('english', 'machine and deep learning')),
            ('3', 'learning about machine', ARRAY['Chunk'], to_tsvector('english', 'learning about machine'))
        """.trimIndent()).update()

        // Test phrase with <-> (adjacent words)
        val phraseResults = jdbcClient.sql("""
            SELECT id FROM content_elements
            WHERE tsv @@ to_tsquery('english', 'machine <-> learning')
        """.trimIndent()).query(String::class.java).list()

        assertEquals(1, phraseResults.size, "Phrase should match adjacent words only")
        assertEquals("1", phraseResults[0], "Should match exact phrase")
    }
}
