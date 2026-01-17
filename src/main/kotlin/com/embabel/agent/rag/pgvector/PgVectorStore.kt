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

import com.embabel.agent.rag.filter.EntityFilter
import com.embabel.agent.rag.filter.PropertyFilter
import com.embabel.agent.rag.ingestion.ChunkTransformer
import com.embabel.agent.rag.ingestion.ContentChunker
import com.embabel.agent.rag.ingestion.RetrievableEnhancer
import com.embabel.agent.rag.model.Chunk
import com.embabel.agent.rag.model.ContentElement
import com.embabel.agent.rag.model.ContentRoot
import com.embabel.agent.rag.model.NavigableDocument
import com.embabel.agent.rag.model.Retrievable
import com.embabel.agent.rag.service.CoreSearchOperations
import com.embabel.agent.rag.service.FilteringTextSearch
import com.embabel.agent.rag.service.FilteringVectorSearch
import com.embabel.agent.rag.store.AbstractChunkingContentElementRepository
import com.embabel.agent.rag.store.ChunkingContentElementRepository
import com.embabel.agent.rag.store.ContentElementRepositoryInfo
import com.embabel.agent.rag.store.DocumentDeletionResult
import com.embabel.common.ai.model.EmbeddingService
import com.embabel.common.core.types.SimilarityResult
import com.embabel.common.core.types.TextSimilaritySearchRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.ResultSet

/**
 * PgVector-based implementation of [ChunkingContentElementRepository] using native pgvector.
 *
 * This implementation features hybrid search combining:
 * - Vector similarity search via pgvector for semantic matching
 * - Full-text search using PostgreSQL's tsvector/tsquery
 * - Trigram fuzzy matching via pg_trgm for typo-tolerant search
 * - Weighted hybrid scoring combining vector and lexical results
 *
 * Hybrid search architecture inspired by [Josh Long's](https://joshlong.com) article
 * [Building a Hybrid Search Engine with PostgreSQL and JDBC](https://joshlong.com/jl/blogPost/building-a-search-engine-with-postgresql-and-jdbc.html)
 *
 * @param jdbcClient JdbcClient for PostgreSQL operations
 * @param properties Configuration properties for this store
 * @param chunkerConfig Configuration for content chunking
 * @param chunkTransformer Transformer for processing chunks
 * @param embeddingService Service for generating embeddings
 * @param enhancers List of enhancers to apply to retrievables
 */
class PgVectorStore @JvmOverloads constructor(
    private val jdbcClient: JdbcClient,
    private val properties: PgVectorStoreProperties,
    chunkerConfig: ContentChunker.Config,
    chunkTransformer: ChunkTransformer,
    embeddingService: EmbeddingService?,
    override val enhancers: List<RetrievableEnhancer> = emptyList(),
) : AbstractChunkingContentElementRepository(
    chunkerConfig = chunkerConfig,
    chunkTransformer = chunkTransformer,
    embeddingService = embeddingService,
), ChunkingContentElementRepository, CoreSearchOperations,
    FilteringTextSearch, FilteringVectorSearch {

    private val objectMapper = ObjectMapper().apply {
        registerModule(JavaTimeModule())
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }
    private val filterConverter = SqlFilterConverter()
    private val sql = SqlResourceLoader.forProperties(properties)

    companion object {
        /**
         * Creates a new builder for PgVectorStore.
         *
         * Example usage in Java:
         * ```java
         * PgVectorStore store = PgVectorStore.builder()
         *     .withDataSource(dataSource)
         *     .withEmbeddingService(embeddingService)
         *     .withName("my-rag-store")
         *     .build();
         * ```
         */
        @JvmStatic
        fun builder(): PgVectorStoreBuilder = PgVectorStoreBuilder()

        /**
         * Creates a new builder with the specified name.
         */
        @JvmStatic
        fun withName(name: String): PgVectorStoreBuilder =
            PgVectorStoreBuilder().withName(name)
    }

    override val name: String get() = properties.name

    override val luceneSyntaxNotes: String = """
        PostgreSQL websearch syntax (similar to web search engines):
        - Multiple terms are ANDed by default: 'gold concierge' finds chunks with both
        - Use OR for alternatives: 'gold OR silver' finds chunks with either
        - Use - to exclude: 'gold -silver' finds gold but not silver
        - Use quotes for exact phrases: '"gold concierge"' finds that exact phrase
        - Keep queries simple - use key terms, not full sentences
    """.trimIndent()

    override fun provision() {
        logger.info("Provisioning PgVectorStore with properties {}", properties)
        createExtensions()
        createContentElementTable()
        createTsvTrigger()
        createIndexes()
        logger.info("Provisioning complete")
    }

    private fun createExtensions() {
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()
        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS pg_trgm").update()
        logger.info("Created PostgreSQL extensions: vector, pg_trgm")
    }

    private fun createContentElementTable() {
        // Create table if not exists
        jdbcClient.sql(sql.load("ddl/create-table")).update()

        // Add missing columns if table already existed (for schema migration)
        addColumnIfNotExists("embedding", "vector(${properties.embeddingDimension})")
        addColumnIfNotExists("clean_text", "TEXT")
        addColumnIfNotExists("tokens", "TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]")
        addColumnIfNotExists("tsv", "TSVECTOR")

        logger.info("Created/updated content element table: {}", properties.contentElementTable)
    }

    private fun addColumnIfNotExists(columnName: String, columnType: String) {
        jdbcClient.sql(
            sql.load(
                "ddl/add-column-if-not-exists",
                mapOf("columnName" to columnName, "columnType" to columnType)
            )
        ).update()
    }

    private fun createTsvTrigger() {
        // Create the trigger function for automatic tsvector maintenance
        jdbcClient.sql(sql.load("ddl/create-tsv-trigger-function")).update()

        // Drop existing trigger and create new one
        jdbcClient.sql(sql.load("ddl/drop-tsv-trigger")).update()
        jdbcClient.sql(sql.load("ddl/create-tsv-trigger")).update()

        logger.info("Created tsvector trigger for automatic maintenance on {}", properties.contentElementTable)

        // Backfill tsv for any existing rows that have NULL tsv
        val updated = jdbcClient.sql(sql.load("ddl/backfill-tsv")).update()
        if (updated > 0) {
            logger.info("Backfilled tsv for {} existing rows", updated)
        }
    }

    private fun createIndexes() {
        // HNSW index for vector similarity search (cosine distance)
        jdbcClient.sql(sql.load("ddl/create-index-embedding")).update()

        // GIN index for full-text search
        jdbcClient.sql(sql.load("ddl/create-index-tsv")).update()

        // GIN index for trigram fuzzy search
        jdbcClient.sql(sql.load("ddl/create-index-text-trgm")).update()

        // GIN index for metadata JSONB
        jdbcClient.sql(sql.load("ddl/create-index-metadata")).update()

        // Index on labels array
        jdbcClient.sql(sql.load("ddl/create-index-labels")).update()

        logger.info("Created indexes on {}", properties.contentElementTable)
    }

    override fun commit() {
        // PostgreSQL transactions are handled by Spring's transaction management
    }

    override fun createInternalRelationships(root: NavigableDocument) {
        // In PostgreSQL, relationships are implicit via parent_id foreign keys
        logger.debug("Internal relationships managed via parent_id column for document {}", root.id)
    }

    override fun deleteRootAndDescendants(uri: String): DocumentDeletionResult? {
        logger.info("Deleting document with URI: {}", uri)

        // First find the root document
        val rootId = jdbcClient.sql(sql.load("operations/find-root-by-uri"))
            .param("uri", uri)
            .query(String::class.java)
            .optional()
            .orElse(null)

        if (rootId == null) {
            logger.warn("No document found with URI: {}", uri)
            return null
        }

        // Delete all descendants using recursive CTE (includes embeddings)
        val deletedCount = jdbcClient.sql(sql.load("operations/delete-root-and-descendants"))
            .param("rootId", rootId)
            .update()

        logger.info("Deleted {} elements for document with URI: {}", deletedCount, uri)
        return DocumentDeletionResult(
            rootUri = uri,
            deletedCount = deletedCount
        )
    }

    override fun existsRootWithUri(uri: String): Boolean {
        val count = jdbcClient.sql(sql.load("operations/exists-root-with-uri"))
            .param("uri", uri)
            .query(Int::class.java)
            .single()
        return count > 0
    }

    override fun findContentRootByUri(uri: String): ContentRoot? {
        logger.debug("Finding root document with URI: {}", uri)
        return jdbcClient.sql(sql.load("operations/find-content-root-by-uri"))
            .param("uri", uri)
            .query { rs, _ -> mapToContentElement(rs) }
            .optional()
            .orElse(null) as? ContentRoot
    }

    override fun persistChunksWithEmbeddings(
        chunks: List<Chunk>,
        embeddings: Map<String, FloatArray>
    ) {
        chunks.forEach { chunk ->
            val embedding = embeddings[chunk.id]
            saveChunkWithEmbedding(chunk, embedding)
        }
        logger.info("Persisted {} chunks with embeddings", chunks.size)
    }

    private fun saveChunkWithEmbedding(chunk: Chunk, embedding: FloatArray?) {
        val embeddingString = embedding?.joinToString(",", "[", "]")
        val sqlFile = if (embeddingString != null) {
            "operations/upsert-chunk-with-embedding"
        } else {
            "operations/upsert-chunk-without-embedding"
        }

        jdbcClient.sql(sql.load(sqlFile))
            .param("id", chunk.id)
            .param("uri", chunk.uri)
            .param("text", chunk.text)
            .param("urtext", chunk.urtext)
            .param("parentId", chunk.parentId)
            .param("labels", chunk.labels().toTypedArray())
            .param("metadata", toJsonb(chunk.metadata))
            .apply { if (embeddingString != null) param("embedding", embeddingString) }
            .update()
    }

    override fun supportsType(type: String): Boolean {
        return type == Chunk::class.java.simpleName
    }

    /**
     * Performs hybrid search combining vector similarity and full-text search.
     *
     * This method uses a two-phase approach:
     * 1. Uses full-text search (FTS) as a cheap prefilter to find candidate chunks
     * 2. Computes expensive vector similarity only on FTS matches
     * 3. Combines scores with configurable weights (default: 70% vector, 30% FTS)
     * 4. Falls back to fuzzy trigram search if no results found
     *
     * @param request The search request containing query and parameters
     * @param clazz The class type of results (must be Chunk)
     * @return List of similarity results with combined scores
     */
    fun <T : Retrievable> hybridSearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore hybridSearch only supports Chunk class, got: $clazz")
        }

        // Generate embedding for the query
        val queryEmbedding = embeddingService?.embed(request.query)
            ?: throw IllegalStateException("EmbeddingService required for hybrid search")

        val embeddingString = queryEmbedding.joinToString(",", "[", "]")

        // Phase 1: Hybrid vector + full-text search
        // Normalize fts_score to 0-1 range using score/(1+score) transformation
        val results = jdbcClient.sql(sql.load("queries/hybrid-search"))
            .param("query", request.query)
            .param("embedding", embeddingString)
            .param("vectorWeight", properties.vectorWeight)
            .param("ftsWeight", properties.ftsWeight)
            .param("topK", request.topK)
            .query { rs, _ ->
                val chunk = mapToChunk(rs)
                val score = rs.getDouble("score")
                SimilarityResult(chunk, score)
            }
            .list()
            .filter { it.score >= request.similarityThreshold }

        // Phase 2: Fuzzy fallback if no results
        @Suppress("UNCHECKED_CAST")
        if (results.isEmpty()) {
            logger.debug("No hybrid search results, falling back to fuzzy search for query: {}", request.query)
            return fuzzySearch(request, clazz)
        }

        @Suppress("UNCHECKED_CAST")
        return results as List<SimilarityResult<T>>
    }

    /**
     * Performs fuzzy search using PostgreSQL's pg_trgm trigram similarity.
     *
     * This is used as a fallback when hybrid search returns no results,
     * handling typos, misspellings, and partial matches.
     *
     * @param request The search request containing query and parameters
     * @param clazz The class type of results (must be Chunk)
     * @return List of similarity results based on trigram similarity
     */
    fun <T : Retrievable> fuzzySearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore fuzzySearch only supports Chunk class, got: $clazz")
        }

        val results = jdbcClient.sql(sql.load("queries/fuzzy-search"))
            .param("query", request.query.lowercase())
            .param("threshold", properties.fuzzyThreshold)
            .param("topK", request.topK)
            .query { rs, _ ->
                val chunk = mapToChunk(rs)
                val score = rs.getDouble("score")
                SimilarityResult(chunk, score)
            }
            .list()

        logger.debug("Fuzzy search returned {} results for query: {}", results.size, request.query)

        @Suppress("UNCHECKED_CAST")
        return results as List<SimilarityResult<T>>
    }

    override fun <T : Retrievable> vectorSearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore vectorSearch only supports Chunk class, got: $clazz")
        }

        // Generate embedding for the query
        val queryEmbedding = embeddingService?.embed(request.query)
            ?: throw IllegalStateException("EmbeddingService required for vector search")

        val embeddingString = queryEmbedding.joinToString(",", "[", "]")

        val results = jdbcClient.sql(sql.load("queries/vector-search"))
            .param("embedding", embeddingString)
            .param("topK", request.topK)
            .query { rs, _ ->
                val chunk = mapToChunk(rs)
                val score = rs.getDouble("score")
                SimilarityResult(chunk, score)
            }
            .list()

        @Suppress("UNCHECKED_CAST")
        return results.filter { it.score >= request.similarityThreshold } as List<SimilarityResult<T>>
    }

    override fun <T : Retrievable> textSearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore textSearch only supports Chunk class, got: $clazz")
        }

        // Use plainto_tsquery for natural language queries (handles plain text better than to_tsquery)
        // Normalize ts_rank score to 0-1 range using 2*score/(1+score) transformation
        // This scales typical ts_rank values (0.01-0.6) to a range comparable with vector similarity (0-1)
        val results = jdbcClient.sql(sql.load("queries/text-search"))
            .param("query", request.query)
            .param("topK", request.topK)
            .query { rs, _ ->
                val chunk = mapToChunk(rs)
                val score = rs.getDouble("score")
                SimilarityResult(chunk, score)
            }
            .list()

        @Suppress("UNCHECKED_CAST")
        return results.filter { it.score >= request.similarityThreshold } as List<SimilarityResult<T>>
    }

    override fun <T : Retrievable> textSearchWithFilter(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>,
        metadataFilter: PropertyFilter?,
        entityFilter: EntityFilter?
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore textSearchWithFilter only supports Chunk class, got: $clazz")
        }

        val filterResult = filterConverter.combineFilters(metadataFilter, entityFilter)
        val filterClause = if (filterResult.isEmpty()) "" else " AND ${filterResult.whereClause}"

        // Normalize ts_rank score to 0-1 range using score * 3 (capped at 1.0)
        val sql = """
            SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp,
                   ts_rank(tsv, plainto_tsquery('english', :query)) as raw_score,
                   LEAST(1.0, ts_rank(tsv, plainto_tsquery('english', :query)) * 3) as score
            FROM ${properties.contentElementTable}
            WHERE 'Chunk' = ANY(labels)
                AND tsv @@ plainto_tsquery('english', :query)
                $filterClause
            ORDER BY raw_score DESC
            LIMIT :topK
        """.trimIndent()

        var query = jdbcClient.sql(sql)
            .param("query", request.query)
            .param("topK", request.topK)

        // Add filter parameters
        filterResult.parameters.forEach { (key, value) ->
            query = query.param(key, value)
        }

        val results = query
            .query { rs, _ ->
                val chunk = mapToChunk(rs)
                val score = rs.getDouble("score")
                SimilarityResult(chunk, score)
            }
            .list()

        @Suppress("UNCHECKED_CAST")
        return results.filter { it.score >= request.similarityThreshold } as List<SimilarityResult<T>>
    }

    override fun <T : Retrievable> vectorSearchWithFilter(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>,
        metadataFilter: PropertyFilter?,
        entityFilter: EntityFilter?
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("PgVectorStore vectorSearchWithFilter only supports Chunk class, got: $clazz")
        }

        // Generate embedding for the query
        val queryEmbedding = embeddingService?.embed(request.query)
            ?: throw IllegalStateException("EmbeddingService required for vector search")

        val embeddingString = queryEmbedding.joinToString(",", "[", "]")
        val filterResult = filterConverter.combineFilters(metadataFilter, entityFilter)

        val filterClause = if (filterResult.isEmpty()) "" else " AND ${filterResult.whereClause}"

        val sql = """
            SELECT id, uri, text, urtext, parent_id, labels, metadata, ingestion_timestamp,
                   (1 - (embedding <=> CAST(:embedding AS vector))) AS score
            FROM ${properties.contentElementTable}
            WHERE 'Chunk' = ANY(labels)
                $filterClause
            ORDER BY score DESC
            LIMIT :topK
        """.trimIndent()

        var query = jdbcClient.sql(sql)
            .param("embedding", embeddingString)
            .param("topK", request.topK)

        // Add filter parameters
        filterResult.parameters.forEach { (key, value) ->
            query = query.param(key, value)
        }

        val results = query
            .query { rs, _ ->
                val chunk = mapToChunk(rs)
                val score = rs.getDouble("score")
                SimilarityResult(chunk, score)
            }
            .list()

        @Suppress("UNCHECKED_CAST")
        return results.filter { it.score >= request.similarityThreshold } as List<SimilarityResult<T>>
    }

    override fun info(): ContentElementRepositoryInfo {
        val stats = jdbcClient.sql(sql.load("queries/store-info"))
            .query { rs, _ ->
                mapOf(
                    "chunk_count" to rs.getInt("chunk_count"),
                    "document_count" to rs.getInt("document_count"),
                    "content_element_count" to rs.getInt("content_element_count")
                )
            }
            .single()

        return PgVectorContentElementRepositoryInfo(
            chunkCount = stats["chunk_count"] ?: 0,
            documentCount = stats["document_count"] ?: 0,
            contentElementCount = stats["content_element_count"] ?: 0,
            hasEmbeddings = embeddingService != null,
            isPersistent = true
        )
    }

    override fun findAllChunksById(chunkIds: List<String>): Iterable<Chunk> {
        if (chunkIds.isEmpty()) return emptyList()
        return jdbcClient.sql(sql.load("queries/find-chunks-by-ids"))
            .param("ids", chunkIds.toTypedArray())
            .query { rs, _ -> mapToChunk(rs) }
            .list()
    }

    override fun findById(id: String): ContentElement? {
        return jdbcClient.sql(sql.load("queries/find-by-id"))
            .param("id", id)
            .query { rs, _ -> mapToContentElement(rs) }
            .optional()
            .orElse(null)
    }

    private fun findChunkById(id: String): Chunk? {
        return jdbcClient.sql(sql.load("queries/find-chunk-by-id"))
            .param("id", id)
            .query { rs, _ -> mapToChunk(rs) }
            .optional()
            .orElse(null)
    }

    override fun save(element: ContentElement): ContentElement {
        jdbcClient.sql(sql.load("operations/save-element"))
            .param("id", element.id)
            .param("uri", element.uri)
            .param("text", (element as? Chunk)?.text)
            .param("urtext", (element as? Chunk)?.urtext)
            .param("parentId", (element as? com.embabel.agent.rag.model.HierarchicalContentElement)?.parentId)
            .param("labels", element.labels().toTypedArray())
            .param("metadata", toJsonb(element.propertiesToPersist()))
            .update()
        return element
    }

    override fun findChunksForEntity(entityId: String): List<Chunk> {
        // Entity-chunk relationships would need to be stored separately if needed
        logger.warn("findChunksForEntity not implemented for PgVectorStore")
        return emptyList()
    }

    private fun mapToContentElement(rs: ResultSet): ContentElement {
        val labels = (rs.getArray("labels")?.array as? Array<*>)?.map { it.toString() }?.toSet() ?: emptySet()
        return if ("Chunk" in labels) {
            mapToChunk(rs)
        } else {
            GenericContentElement(
                id = rs.getString("id"),
                uri = rs.getString("uri"),
                labels = labels,
                metadata = parseJsonb(rs.getString("metadata"))
            )
        }
    }

    private fun mapToChunk(rs: ResultSet): Chunk {
        return Chunk.create(
            id = rs.getString("id"),
            text = rs.getString("text") ?: "",
            urtext = rs.getString("urtext") ?: rs.getString("text") ?: "",
            parentId = rs.getString("parent_id") ?: "",
            metadata = parseJsonb(rs.getString("metadata"))
        )
    }

    private fun toJsonb(map: Map<String, Any?>): String {
        return objectMapper.writeValueAsString(map)
    }

    private fun parseJsonb(json: String?): Map<String, Any?> {
        if (json.isNullOrBlank()) return emptyMap()
        @Suppress("UNCHECKED_CAST")
        return objectMapper.readValue(json, Map::class.java) as Map<String, Any?>
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
