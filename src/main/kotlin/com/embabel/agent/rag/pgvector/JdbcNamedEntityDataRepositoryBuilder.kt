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

import com.embabel.agent.core.DataDictionary
import com.embabel.common.ai.model.EmbeddingService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.jdbc.core.simple.JdbcClient
import javax.sql.DataSource

/**
 * Builder for [JdbcNamedEntityDataRepository] instances.
 *
 * Provides a fluent API following the same pattern as [PgVectorStoreBuilder].
 *
 * Example:
 * ```java
 * JpaNamedEntityDataRepository repo = JpaNamedEntityDataRepository.builder()
 *     .withJdbcClient(jdbcClient)
 *     .withDataDictionary(dataDictionary)
 *     .withEmbeddingService(embeddingService)
 *     .withNativeLookup(Customer.class, customerLookup)
 *     .build();
 * ```
 */
data class JdbcNamedEntityDataRepositoryBuilder(
    private val jdbcClient: JdbcClient? = null,
    private val dataSource: DataSource? = null,
    private val dataDictionary: DataDictionary? = null,
    private val embeddingService: EmbeddingService? = null,
    private val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule(),
    private val tableName: String = "named_entities",
    private val relationshipTableName: String = "entity_relationships",
    private val embeddingDimension: Int = 1536,
    private val nativeLookups: Map<Class<*>, NativeEntityLookup<*>> = emptyMap(),
) {

    fun withJdbcClient(jdbcClient: JdbcClient): JdbcNamedEntityDataRepositoryBuilder =
        copy(jdbcClient = jdbcClient)

    fun withDataSource(dataSource: DataSource): JdbcNamedEntityDataRepositoryBuilder =
        copy(dataSource = dataSource)

    fun withDataDictionary(dataDictionary: DataDictionary): JdbcNamedEntityDataRepositoryBuilder =
        copy(dataDictionary = dataDictionary)

    fun withEmbeddingService(embeddingService: EmbeddingService): JdbcNamedEntityDataRepositoryBuilder =
        copy(embeddingService = embeddingService)

    fun withObjectMapper(objectMapper: ObjectMapper): JdbcNamedEntityDataRepositoryBuilder =
        copy(objectMapper = objectMapper)

    fun withTableName(tableName: String): JdbcNamedEntityDataRepositoryBuilder =
        copy(tableName = tableName)

    fun withRelationshipTableName(relationshipTableName: String): JdbcNamedEntityDataRepositoryBuilder =
        copy(relationshipTableName = relationshipTableName)

    fun withEmbeddingDimension(embeddingDimension: Int): JdbcNamedEntityDataRepositoryBuilder =
        copy(embeddingDimension = embeddingDimension)

    fun <T> withNativeLookup(type: Class<T>, lookup: NativeEntityLookup<T>): JdbcNamedEntityDataRepositoryBuilder =
        copy(nativeLookups = nativeLookups + (type to lookup))

    fun build(): JdbcNamedEntityDataRepository {
        val resolvedJdbcClient = jdbcClient
            ?: dataSource?.let { JdbcClient.create(it) }
            ?: throw IllegalStateException("Either jdbcClient or dataSource must be provided")

        val resolvedDataDictionary = dataDictionary
            ?: throw IllegalStateException("dataDictionary must be provided")

        val resolvedDimension = embeddingService?.dimensions ?: embeddingDimension

        val repository = JdbcNamedEntityDataRepository(
            jdbcClient = resolvedJdbcClient,
            dataDictionary = resolvedDataDictionary,
            embeddingService = embeddingService,
            objectMapper = objectMapper,
            tableName = tableName,
            relationshipTableName = relationshipTableName,
            embeddingDimension = resolvedDimension,
            nativeLookups = nativeLookups,
        )

        repository.provision()
        return repository
    }

    fun buildWithoutProvision(): JdbcNamedEntityDataRepository {
        val resolvedJdbcClient = jdbcClient
            ?: dataSource?.let { JdbcClient.create(it) }
            ?: throw IllegalStateException("Either jdbcClient or dataSource must be provided")

        val resolvedDataDictionary = dataDictionary
            ?: throw IllegalStateException("dataDictionary must be provided")

        val resolvedDimension = embeddingService?.dimensions ?: embeddingDimension

        return JdbcNamedEntityDataRepository(
            jdbcClient = resolvedJdbcClient,
            dataDictionary = resolvedDataDictionary,
            embeddingService = embeddingService,
            objectMapper = objectMapper,
            tableName = tableName,
            relationshipTableName = relationshipTableName,
            embeddingDimension = resolvedDimension,
            nativeLookups = nativeLookups,
        )
    }
}
