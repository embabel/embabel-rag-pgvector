/*
 * Copyright 2024-2025 Embabel Software, Inc.
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
import com.embabel.agent.rag.ingestion.RetrievableEnhancer
import com.embabel.agent.rag.model.Chunk
import com.embabel.agent.rag.model.ContentElement
import com.embabel.agent.rag.model.ContentRoot
import com.embabel.agent.rag.model.NavigableDocument
import com.embabel.agent.rag.model.Retrievable
import com.embabel.agent.rag.service.CoreSearchOperations
import com.embabel.agent.rag.store.AbstractChunkingContentElementRepository
import com.embabel.agent.rag.store.ChunkingContentElementRepository
import com.embabel.agent.rag.store.ContentElementRepositoryInfo
import com.embabel.agent.rag.store.DocumentDeletionResult
import com.embabel.common.ai.model.EmbeddingService
import com.embabel.common.core.types.SimilarityResult
import com.embabel.common.core.types.TextSimilaritySearchRequest
import org.springframework.ai.document.Document
import org.springframework.ai.vectorstore.SearchRequest
import org.springframework.ai.vectorstore.VectorStore
import org.springframework.jdbc.core.JdbcTemplate

/**
 * PgVector-based implementation of [ChunkingContentElementRepository] using Spring AI's PgVectorStore.
 *
 * This implementation:
 * - Uses PostgreSQL with pgvector extension for vector similarity search
 * - Supports full-text search via PostgreSQL's tsvector/tsquery
 * - Persists content element metadata in a dedicated table
 * - Implements [CoreSearchOperations] for standard RAG operations
 *
 * @param vectorStore The Spring AI PgVectorStore instance
 * @param jdbcTemplate JDBC template for PostgreSQL operations
 * @param properties Configuration properties for this store
 * @param chunkerConfig Configuration for content chunking
 * @param chunkTransformer Transformer for processing chunks
 * @param embeddingService Service for generating embeddings
 * @param enhancers List of enhancers to apply to retrievables
 */
class PgVectorStore @JvmOverloads constructor(
    private val vectorStore: VectorStore,
    private val jdbcTemplate: JdbcTemplate,
    private val properties: PgVectorStoreProperties,
    chunkerConfig: ContentChunker.Config,
    chunkTransformer: ChunkTransformer,
    embeddingService: EmbeddingService?,
    override val enhancers: List<RetrievableEnhancer> = emptyList(),
) : AbstractChunkingContentElementRepository(
    chunkerConfig = chunkerConfig,
    chunkTransformer = chunkTransformer,
    embeddingService = embeddingService,
), ChunkingContentElementRepository, CoreSearchOperations {

    override val name: String get() = properties.name

    override val luceneSyntaxNotes: String = """
        PostgreSQL full-text search syntax:
        - Use & for AND: 'machine & learning'
        - Use | for OR: 'kotlin | java'
        - Use ! for NOT: 'test & !unit'
        - Use :* for prefix matching: 'mach:*'
        - Phrases use proximity: 'machine <-> learning'
    """.trimIndent()

    override fun provision() {
        logger.info("Provisioning PgVectorStore with properties {}", properties)
        createContentElementTable()
        createFullTextIndex()
        logger.info("Provisioning complete")
    }

    private fun createContentElementTable() {
        jdbcTemplate.execute(
            """
            CREATE TABLE IF NOT EXISTS ${properties.contentElementTable} (
                id VARCHAR(255) PRIMARY KEY,
                uri TEXT,
                text TEXT,
                urtext TEXT,
                parent_id VARCHAR(255),
                labels TEXT[],
                metadata JSONB,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """.trimIndent()
        )
        logger.info("Created content element table: {}", properties.contentElementTable)
    }

    private fun createFullTextIndex() {
        jdbcTemplate.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_${properties.contentElementTable}_text_search
            ON ${properties.contentElementTable}
            USING GIN (to_tsvector('english', text))
            """.trimIndent()
        )
        logger.info("Created full-text search index on {}", properties.contentElementTable)
    }

    override fun commit() {
        // PostgreSQL transactions are handled by Spring's transaction management
    }

    override fun createInternalRelationships(root: NavigableDocument) {
        // In PostgreSQL, relationships are implicit via parent_id foreign keys
        // No explicit relationship creation needed
        logger.debug("Internal relationships managed via parent_id column for document {}", root.id)
    }

    override fun deleteRootAndDescendants(uri: String): DocumentDeletionResult? {
        logger.info("Deleting document with URI: {}", uri)

        // First find the root document
        val rootId = jdbcTemplate.queryForObject(
            """
            SELECT id FROM ${properties.contentElementTable}
            WHERE uri = ? AND 'Document' = ANY(labels)
            """.trimIndent(),
            String::class.java,
            uri
        ) ?: run {
            logger.warn("No document found with URI: {}", uri)
            return null
        }

        // Delete from vector store
        vectorStore.delete(listOf(rootId))

        // Delete all descendants using recursive CTE
        val deletedCount = jdbcTemplate.update(
            """
            WITH RECURSIVE descendants AS (
                SELECT id FROM ${properties.contentElementTable} WHERE id = ?
                UNION ALL
                SELECT ce.id FROM ${properties.contentElementTable} ce
                INNER JOIN descendants d ON ce.parent_id = d.id
            )
            DELETE FROM ${properties.contentElementTable}
            WHERE id IN (SELECT id FROM descendants)
            """.trimIndent(),
            rootId
        )

        logger.info("Deleted {} elements for document with URI: {}", deletedCount, uri)
        return DocumentDeletionResult(
            rootUri = uri,
            deletedCount = deletedCount
        )
    }

    override fun existsRootWithUri(uri: String): Boolean {
        val count = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM ${properties.contentElementTable}
            WHERE uri = ? AND ('Document' = ANY(labels) OR 'ContentRoot' = ANY(labels))
            """.trimIndent(),
            Int::class.java,
            uri
        ) ?: 0
        return count > 0
    }

    override fun findContentRootByUri(uri: String): ContentRoot? {
        logger.debug("Finding root document with URI: {}", uri)
        val results = jdbcTemplate.query(
            """
            SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
            FROM ${properties.contentElementTable}
            WHERE uri = ? AND ('Document' = ANY(labels) OR 'ContentRoot' = ANY(labels))
            """.trimIndent(),
            { rs, _ -> mapToContentElement(rs) },
            uri
        )
        return results.firstOrNull() as? ContentRoot
    }

    override fun persistChunksWithEmbeddings(
        chunks: List<Chunk>,
        embeddings: Map<String, FloatArray>
    ) {
        // Convert chunks to Spring AI Documents and add to vector store
        val documents = chunks.map { chunk ->
            Document(
                chunk.id,
                chunk.text,
                chunk.metadata.mapValues { it.value?.toString() ?: "" }
            )
        }
        vectorStore.add(documents)

        // Also persist chunk details in our content element table
        chunks.forEach { chunk ->
            saveChunkToTable(chunk)
        }

        logger.info("Persisted {} chunks with embeddings", chunks.size)
    }

    private fun saveChunkToTable(chunk: Chunk) {
        jdbcTemplate.update(
            """
            INSERT INTO ${properties.contentElementTable} (id, uri, text, urtext, parent_id, labels, metadata)
            VALUES (?, ?, ?, ?, ?, ?::text[], ?::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                uri = EXCLUDED.uri,
                text = EXCLUDED.text,
                urtext = EXCLUDED.urtext,
                parent_id = EXCLUDED.parent_id,
                labels = EXCLUDED.labels,
                metadata = EXCLUDED.metadata
            """.trimIndent(),
            chunk.id,
            chunk.uri,
            chunk.text,
            chunk.urtext,
            chunk.parentId,
            chunk.labels().toTypedArray(),
            toJsonb(chunk.metadata)
        )
    }

    override fun supportsType(type: String): Boolean {
        return type == Chunk::class.java.simpleName
    }

    override fun <T : Retrievable> vectorSearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore vectorSearch only supports Chunk class, got: $clazz")
        }

        val searchRequest = SearchRequest.builder()
            .query(request.query)
            .topK(request.topK)
            .similarityThreshold(request.similarityThreshold)
            .build()

        val results = vectorStore.similaritySearch(searchRequest)

        @Suppress("UNCHECKED_CAST")
        return results.map { doc ->
            val chunk = findChunkById(doc.id) ?: Chunk.create(
                id = doc.id,
                text = doc.text ?: "",
                parentId = doc.metadata["parent_id"]?.toString() ?: "",
                metadata = doc.metadata.mapValues { it.value }
            )
            SimpleSimilarityResult(
                match = chunk,
                score = doc.score?.toDouble() ?: 0.0
            )
        } as List<SimilarityResult<T>>
    }

    override fun <T : Retrievable> textSearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore textSearch only supports Chunk class, got: $clazz")
        }

        val tsQuery = convertToTsQuery(request.query)
        val results = jdbcTemplate.query(
            """
            SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp,
                   ts_rank(to_tsvector('english', text), to_tsquery('english', ?)) as score
            FROM ${properties.contentElementTable}
            WHERE 'Chunk' = ANY(labels)
                AND to_tsvector('english', text) @@ to_tsquery('english', ?)
            ORDER BY score DESC
            LIMIT ?
            """.trimIndent(),
            { rs, _ ->
                val chunk = mapToChunk(rs)
                val score = rs.getDouble("score")
                SimpleSimilarityResult(chunk, score)
            },
            tsQuery,
            tsQuery,
            request.topK
        )

        @Suppress("UNCHECKED_CAST")
        return results.filter { it.score >= request.similarityThreshold } as List<SimilarityResult<T>>
    }

    private fun convertToTsQuery(query: String): String {
        // Convert natural language query to PostgreSQL tsquery format
        return query.trim()
            .split("\\s+".toRegex())
            .filter { it.isNotBlank() }
            .joinToString(" & ")
    }

    override fun info(): ContentElementRepositoryInfo {
        val stats = jdbcTemplate.queryForMap(
            """
            SELECT
                (SELECT COUNT(*) FROM ${properties.contentElementTable} WHERE 'Chunk' = ANY(labels)) as chunk_count,
                (SELECT COUNT(*) FROM ${properties.contentElementTable} WHERE 'Document' = ANY(labels)) as document_count,
                (SELECT COUNT(*) FROM ${properties.contentElementTable}) as content_element_count
            """.trimIndent()
        )
        return PgVectorContentElementRepositoryInfo(
            chunkCount = (stats["chunk_count"] as? Number)?.toInt() ?: 0,
            documentCount = (stats["document_count"] as? Number)?.toInt() ?: 0,
            contentElementCount = (stats["content_element_count"] as? Number)?.toInt() ?: 0,
            hasEmbeddings = embeddingService != null,
            isPersistent = true
        )
    }

    override fun findAllChunksById(chunkIds: List<String>): Iterable<Chunk> {
        if (chunkIds.isEmpty()) return emptyList()
        return jdbcTemplate.query(
            """
            SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
            FROM ${properties.contentElementTable}
            WHERE id = ANY(?) AND 'Chunk' = ANY(labels)
            """.trimIndent(),
            { rs, _ -> mapToChunk(rs) },
            chunkIds.toTypedArray()
        )
    }

    override fun findById(id: String): ContentElement? {
        val results = jdbcTemplate.query(
            """
            SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
            FROM ${properties.contentElementTable}
            WHERE id = ?
            """.trimIndent(),
            { rs, _ -> mapToContentElement(rs) },
            id
        )
        return results.firstOrNull()
    }

    private fun findChunkById(id: String): Chunk? {
        val results = jdbcTemplate.query(
            """
            SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp
            FROM ${properties.contentElementTable}
            WHERE id = ? AND 'Chunk' = ANY(labels)
            """.trimIndent(),
            { rs, _ -> mapToChunk(rs) },
            id
        )
        return results.firstOrNull()
    }

    override fun save(element: ContentElement): ContentElement {
        jdbcTemplate.update(
            """
            INSERT INTO ${properties.contentElementTable} (id, uri, text, urtext, parent_id, labels, metadata)
            VALUES (?, ?, ?, ?, ?, ?::text[], ?::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                uri = EXCLUDED.uri,
                text = EXCLUDED.text,
                urtext = EXCLUDED.urtext,
                parent_id = EXCLUDED.parent_id,
                labels = EXCLUDED.labels,
                metadata = EXCLUDED.metadata
            """.trimIndent(),
            element.id,
            element.uri,
            (element as? Chunk)?.text,
            (element as? Chunk)?.urtext,
            (element as? com.embabel.agent.rag.model.HierarchicalContentElement)?.parentId,
            element.labels().toTypedArray(),
            toJsonb(element.propertiesToPersist())
        )
        return element
    }

    override fun findChunksForEntity(entityId: String): List<Chunk> {
        // Entity-chunk relationships would need to be stored separately if needed
        // For now, return empty list as this implementation focuses on basic RAG
        logger.warn("findChunksForEntity not implemented for PgVectorStore")
        return emptyList()
    }

    private fun mapToContentElement(rs: java.sql.ResultSet): ContentElement {
        val labels = (rs.getArray("labels")?.array as? Array<*>)?.map { it.toString() }?.toSet() ?: emptySet()
        return if ("Chunk" in labels) {
            mapToChunk(rs)
        } else {
            // Return a basic content element for non-chunk types
            GenericContentElement(
                id = rs.getString("id"),
                uri = rs.getString("uri"),
                labels = labels,
                metadata = parseJsonb(rs.getString("metadata"))
            )
        }
    }

    private fun mapToChunk(rs: java.sql.ResultSet): Chunk {
        return Chunk.create(
            id = rs.getString("id"),
            text = rs.getString("text") ?: "",
            urtext = rs.getString("urtext") ?: rs.getString("text") ?: "",
            parentId = rs.getString("parent_id") ?: "",
            metadata = parseJsonb(rs.getString("metadata"))
        )
    }

    private fun toJsonb(map: Map<String, Any?>): String {
        return com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(map)
    }

    private fun parseJsonb(json: String?): Map<String, Any?> {
        if (json.isNullOrBlank()) return emptyMap()
        @Suppress("UNCHECKED_CAST")
        return com.fasterxml.jackson.databind.ObjectMapper().readValue(json, Map::class.java) as Map<String, Any?>
    }
}

/**
 * Simple implementation of ContentElementRepositoryInfo for PgVectorStore.
 */
data class PgVectorContentElementRepositoryInfo(
    override val chunkCount: Int,
    override val documentCount: Int,
    override val contentElementCount: Int,
    override val hasEmbeddings: Boolean,
    override val isPersistent: Boolean
) : ContentElementRepositoryInfo

/**
 * Generic content element for non-chunk types stored in the database.
 */
data class GenericContentElement(
    override val id: String,
    override val uri: String?,
    private val labels: Set<String>,
    override val metadata: Map<String, Any?>
) : ContentElement {
    override fun labels(): Set<String> = labels + setOf("ContentElement")
}

/**
 * Simple implementation of SimilarityResult.
 */
data class SimpleSimilarityResult<T>(
    override val match: T,
    override val score: Double
) : SimilarityResult<T>
