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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.jdbc.datasource.DriverManagerDataSource

/**
 * Local integration tests for [PgVectorStore] using an already running pgvector container.
 *
 * Run with: PGVECTOR_TEST=true mvn test -Dtest=PgVectorStoreLocalTest
 *
 * Requires:
 * - Docker container running: docker compose up -d
 * - Database: embabel_air with content_elements table populated
 */
@EnabledIfEnvironmentVariable(named = "PGVECTOR_TEST", matches = "true")
class PgVectorStoreLocalTest {

    companion object {
        private lateinit var jdbcClient: JdbcClient
        private lateinit var store: PgVectorStore

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val dataSource = DriverManagerDataSource(
                "jdbc:postgresql://localhost:5432/embabel_air",
                "embabel",
                "embabel"
            )
            jdbcClient = JdbcClient.create(dataSource)

            val properties = PgVectorStoreProperties(
                name = "test-store",
                contentElementTable = "content_elements",
                embeddingDimension = 1536
            )

            store = PgVectorStore(
                jdbcClient = jdbcClient,
                properties = properties,
                chunkerConfig = ContentChunker.Config(),
                chunkTransformer = ChunkTransformer.NO_OP,
                embeddingService = null,
                enhancers = emptyList()
            )
        }
    }

    @Test
    fun `textSearch should find chunks matching query`() {
        val request = TextSimilaritySearchRequest(
            query = "flight",
            topK = 5,
            similarityThreshold = 0.0
        )

        val results = store.textSearch(request, Chunk::class.java)

        println("Text search results for 'flight': ${results.size}")
        results.forEach { println("  - ${it.match.id}: score=${it.score}") }

        assertTrue(results.isNotEmpty(), "Should find chunks containing 'flight'")
        assertTrue(results.all { it.score > 0 }, "All results should have positive scores")
    }

    @Test
    fun `textSearch should find chunks with rebooking query`() {
        val request = TextSimilaritySearchRequest(
            query = "rebooking policy",
            topK = 5,
            similarityThreshold = 0.0
        )

        val results = store.textSearch(request, Chunk::class.java)

        println("Text search results for 'rebooking policy': ${results.size}")
        results.forEach { println("  - ${it.match.id}: score=${it.score}") }

        assertTrue(results.isNotEmpty(), "Should find chunks about rebooking")
    }

    @Test
    fun `info should return correct statistics`() {
        val info = store.info()

        println("Store info: chunks=${info.chunkCount}, documents=${info.documentCount}")

        assertTrue(info.chunkCount > 0, "Should have chunks")
        assertTrue(info.contentElementCount > 0, "Should have content elements")
    }

    @Test
    fun `findAllChunksById should retrieve chunks`() {
        // First get some chunk IDs via text search
        val request = TextSimilaritySearchRequest(
            query = "flight",
            topK = 3,
            similarityThreshold = 0.0
        )
        val searchResults = store.textSearch(request, Chunk::class.java)

        if (searchResults.isNotEmpty()) {
            val chunkIds = searchResults.map { it.match.id }
            val chunks = store.findAllChunksById(chunkIds).toList()

            assertEquals(chunkIds.size, chunks.size, "Should retrieve all requested chunks")
            assertTrue(chunks.all { it.text.isNotEmpty() }, "All chunks should have text")
        }
    }

    @Test
    fun `direct SQL text search should work`() {
        // Direct SQL test to verify the query works
        val results = jdbcClient.sql("""
            SELECT id, ts_rank(tsv, plainto_tsquery('english', :query)) as score
            FROM content_elements
            WHERE 'Chunk' = ANY(labels)
                AND tsv @@ plainto_tsquery('english', :query)
            ORDER BY score DESC
            LIMIT 5
        """.trimIndent())
            .param("query", "flight booking")
            .query { rs: java.sql.ResultSet, _: Int ->
                Pair(rs.getString("id"), rs.getDouble("score"))
            }
            .list()

        println("Direct SQL results for 'flight booking': ${results.size}")
        results.forEach { println("  - ${it.first}: score=${it.second}") }

        assertTrue(results.isNotEmpty(), "Direct SQL should find results")
    }
}
