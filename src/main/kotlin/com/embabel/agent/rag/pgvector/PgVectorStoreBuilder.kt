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
import com.embabel.agent.rag.ingestion.RetrievableEnhancer
import com.embabel.agent.rag.service.IngestingSearchOperationsBuilder
import com.embabel.common.ai.model.EmbeddingService
import org.springframework.jdbc.core.simple.JdbcClient
import javax.sql.DataSource

/**
 * Builder for [PgVectorStore] instances.
 *
 * Provides a fluent API for configuring and creating PgVectorStore instances,
 * following the same pattern as LuceneSearchOperationsBuilder for consistency.
 *
 * Example usage in Kotlin:
 * ```kotlin
 * val store = PgVectorStore.builder()
 *     .withDataSource(dataSource)
 *     .withEmbeddingService(embeddingService)
 *     .withName("my-rag-store")
 *     .build()
 * ```
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
data class PgVectorStoreBuilder(
    private val name: String = "pgvector-store",
    private val jdbcClient: JdbcClient? = null,
    private val dataSource: DataSource? = null,
    private val embeddingService: EmbeddingService? = null,
    private val chunkerConfig: ContentChunker.Config = ContentChunker.Config(),
    private val chunkTransformer: ChunkTransformer = ChunkTransformer.NO_OP,
    private val enhancers: List<RetrievableEnhancer> = emptyList(),
    private val contentElementTable: String = "content_elements",
    private val schemaName: String = "public",
    private val embeddingDimension: Int = 1536,
    private val vectorWeight: Double = 0.7,
    private val ftsWeight: Double = 0.3,
    private val fuzzyThreshold: Double = 0.2,
) : IngestingSearchOperationsBuilder<PgVectorStore, PgVectorStoreBuilder> {

    override fun withName(name: String): PgVectorStoreBuilder = copy(name = name)

    override fun withEmbeddingService(embeddingService: EmbeddingService): PgVectorStoreBuilder =
        copy(embeddingService = embeddingService)

    override fun withContentChunker(contentChunker: ContentChunker): PgVectorStoreBuilder =
        copy(chunkTransformer = chunkTransformer)

    override fun withChunkTransformer(chunkTransformer: ChunkTransformer): PgVectorStoreBuilder =
        copy(chunkTransformer = chunkTransformer)

    override fun withChunkerConfig(chunkerConfig: ContentChunker.Config): PgVectorStoreBuilder =
        copy(chunkerConfig = chunkerConfig)

    /**
     * Sets the JdbcClient for database operations.
     * Either this or [withDataSource] must be called before build().
     */
    fun withJdbcClient(jdbcClient: JdbcClient): PgVectorStoreBuilder =
        copy(jdbcClient = jdbcClient)

    /**
     * Sets the DataSource, from which a JdbcClient will be created.
     * Either this or [withJdbcClient] must be called before build().
     */
    fun withDataSource(dataSource: DataSource): PgVectorStoreBuilder =
        copy(dataSource = dataSource)

    /**
     * Sets the list of retrievable enhancers to apply to search results.
     */
    fun withEnhancers(enhancers: List<RetrievableEnhancer>): PgVectorStoreBuilder =
        copy(enhancers = enhancers)

    /**
     * Adds a single retrievable enhancer to the list.
     */
    fun withEnhancer(enhancer: RetrievableEnhancer): PgVectorStoreBuilder =
        copy(enhancers = enhancers + enhancer)

    /**
     * Sets the name of the PostgreSQL table for storing content elements.
     * Defaults to "content_elements".
     */
    fun withContentElementTable(contentElementTable: String): PgVectorStoreBuilder =
        copy(contentElementTable = contentElementTable)

    /**
     * Sets the PostgreSQL schema name.
     * Defaults to "public".
     */
    fun withSchemaName(schemaName: String): PgVectorStoreBuilder =
        copy(schemaName = schemaName)

    /**
     * Sets the dimension of embedding vectors.
     * Defaults to 1536 (OpenAI text-embedding-ada-002).
     */
    fun withEmbeddingDimension(embeddingDimension: Int): PgVectorStoreBuilder {
        require(embeddingDimension > 0) { "embeddingDimension must be positive" }
        return copy(embeddingDimension = embeddingDimension)
    }

    /**
     * Sets the weight for vector similarity score in hybrid search.
     * Must be between 0.0 and 1.0. Defaults to 0.7.
     */
    fun withVectorWeight(vectorWeight: Double): PgVectorStoreBuilder {
        require(vectorWeight in 0.0..1.0) { "vectorWeight must be between 0.0 and 1.0" }
        return copy(vectorWeight = vectorWeight)
    }

    /**
     * Sets the weight for full-text search score in hybrid search.
     * Must be between 0.0 and 1.0. Defaults to 0.3.
     */
    fun withFtsWeight(ftsWeight: Double): PgVectorStoreBuilder {
        require(ftsWeight in 0.0..1.0) { "ftsWeight must be between 0.0 and 1.0" }
        return copy(ftsWeight = ftsWeight)
    }

    /**
     * Sets the minimum trigram similarity threshold for fuzzy fallback search.
     * Must be between 0.0 and 1.0. Defaults to 0.2.
     */
    fun withFuzzyThreshold(fuzzyThreshold: Double): PgVectorStoreBuilder {
        require(fuzzyThreshold in 0.0..1.0) { "fuzzyThreshold must be between 0.0 and 1.0" }
        return copy(fuzzyThreshold = fuzzyThreshold)
    }

    /**
     * Configures hybrid search weights.
     * The weights should typically sum to 1.0 for normalized scoring.
     *
     * @param vectorWeight Weight for vector similarity (semantic matching)
     * @param ftsWeight Weight for full-text search (lexical matching)
     */
    fun withHybridWeights(vectorWeight: Double, ftsWeight: Double): PgVectorStoreBuilder {
        require(vectorWeight in 0.0..1.0) { "vectorWeight must be between 0.0 and 1.0" }
        require(ftsWeight in 0.0..1.0) { "ftsWeight must be between 0.0 and 1.0" }
        return copy(vectorWeight = vectorWeight, ftsWeight = ftsWeight)
    }

    override fun build(): PgVectorStore {
        val resolvedJdbcClient = jdbcClient
            ?: dataSource?.let { JdbcClient.create(it) }
            ?: throw IllegalStateException("Either jdbcClient or dataSource must be provided")

        val properties = PgVectorStoreProperties(
            name = name,
            contentElementTable = contentElementTable,
            schemaName = schemaName,
            embeddingDimension = embeddingDimension,
            vectorWeight = vectorWeight,
            ftsWeight = ftsWeight,
            fuzzyThreshold = fuzzyThreshold,
        )

        val store = PgVectorStore(
            jdbcClient = resolvedJdbcClient,
            properties = properties,
            chunkerConfig = chunkerConfig,
            chunkTransformer = chunkTransformer,
            embeddingService = embeddingService,
            enhancers = enhancers,
        )

        store.provision()
        return store
    }

    /**
     * Builds the PgVectorStore without calling provision().
     * Use this when connecting to an existing database schema.
     */
    fun buildWithoutProvision(): PgVectorStore {
        val resolvedJdbcClient = jdbcClient
            ?: dataSource?.let { JdbcClient.create(it) }
            ?: throw IllegalStateException("Either jdbcClient or dataSource must be provided")

        val properties = PgVectorStoreProperties(
            name = name,
            contentElementTable = contentElementTable,
            schemaName = schemaName,
            embeddingDimension = embeddingDimension,
            vectorWeight = vectorWeight,
            ftsWeight = ftsWeight,
            fuzzyThreshold = fuzzyThreshold,
        )

        return PgVectorStore(
            jdbcClient = resolvedJdbcClient,
            properties = properties,
            chunkerConfig = chunkerConfig,
            chunkTransformer = chunkTransformer,
            embeddingService = embeddingService,
            enhancers = enhancers,
        )
    }
}
