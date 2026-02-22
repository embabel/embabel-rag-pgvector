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
import com.embabel.agent.filter.PropertyFilter
import com.embabel.agent.rag.filter.EntityFilter
import com.embabel.agent.rag.model.NamedEntity
import com.embabel.agent.rag.model.NamedEntityData
import com.embabel.agent.rag.model.RelationshipDirection
import com.embabel.agent.rag.model.SimpleNamedEntityData
import com.embabel.agent.rag.service.NamedEntityDataRepository
import com.embabel.agent.rag.service.RelationshipData
import com.embabel.agent.rag.service.RetrievableIdentifier
import com.embabel.common.ai.model.EmbeddingService
import com.embabel.common.core.types.SimilarityResult
import com.embabel.common.core.types.TextSimilaritySearchRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.ResultSet

/**
 * Functional interface for native entity lookup, avoiding JPA dependency in this module.
 * Callers register these from their JPA repositories.
 */
interface NativeEntityLookup<T> {
    fun findById(id: String): T?
    fun findAll(): List<T>
}

/**
 * JPA/pgvector-backed implementation of [NamedEntityDataRepository].
 *
 * Stores named entities in a PostgreSQL table with pgvector embeddings,
 * supporting vector search, text search, label-based lookup, and
 * relationship navigation via a separate relationships table.
 *
 * Follows the same patterns as [PgVectorStore]: uses [JdbcClient],
 * [SqlResourceLoader], and self-provisioning DDL.
 */
class JdbcNamedEntityDataRepository @JvmOverloads constructor(
    private val jdbcClient: JdbcClient,
    override val dataDictionary: DataDictionary,
    private val embeddingService: EmbeddingService? = null,
    override val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule(),
    private val tableName: String = "named_entities",
    private val relationshipTableName: String = "entity_relationships",
    private val embeddingDimension: Int = embeddingService?.dimensions ?: 1536,
    private val nativeLookups: Map<Class<*>, NativeEntityLookup<*>> = emptyMap(),
) : NamedEntityDataRepository {

    private val logger = LoggerFactory.getLogger(JdbcNamedEntityDataRepository::class.java)

    private val sql = SqlResourceLoader(tableName, embeddingDimension)
    private val filterConverter = SqlFilterConverter()

    override val luceneSyntaxNotes: String = "PostgreSQL substring matching on name and description fields"

    /**
     * Provision the database schema: create tables and indexes.
     */
    fun provision() {
        logger.info("Provisioning named entities schema: table={}, relationships={}", tableName, relationshipTableName)

        jdbcClient.sql("CREATE EXTENSION IF NOT EXISTS vector").update()

        val createTable = sql.load("entities/create-entities-table")
        jdbcClient.sql(createTable).update()

        val createRelTable = sql.load(
            "entities/create-relationships-table",
            mapOf("relationshipTable" to relationshipTableName)
        )
        jdbcClient.sql(createRelTable).update()

        val createIndexes = sql.load(
            "entities/create-entity-indexes",
            mapOf("relationshipTable" to relationshipTableName)
        )
        for (statement in createIndexes.split(";").map { it.trim() }.filter { it.isNotBlank() }) {
            try {
                jdbcClient.sql(statement).update()
            } catch (e: Exception) {
                logger.debug("Index creation skipped (may already exist): {}", e.message)
            }
        }

        logger.info("Named entities schema provisioned successfully")
    }

    override fun save(entity: NamedEntityData): NamedEntityData {
        val embedding = embeddingService?.let { svc ->
            svc.embed(entity.embeddableValue()).joinToString(",", "[", "]")
        }

        val saveSql = sql.load("entities/save-entity")
        jdbcClient.sql(saveSql)
            .param("id", entity.id)
            .param("name", entity.name)
            .param("description", entity.description)
            .param("uri", entity.uri)
            .param("labels", entity.labels().toTypedArray())
            .param("properties", objectMapper.writeValueAsString(entity.properties))
            .param("metadata", objectMapper.writeValueAsString(entity.metadata))
            .param("contextId", (entity.metadata["context_id"] as? String))
            .param("embedding", embedding)
            .update()

        return entity
    }

    override fun findById(id: String): NamedEntityData? {
        val findSql = sql.load("entities/find-entity-by-id")
        return jdbcClient.sql(findSql)
            .param("id", id)
            .query { rs, _ -> mapRow(rs) }
            .optional()
            .orElse(null)
    }

    override fun delete(id: String): Boolean {
        val count = jdbcClient.sql("DELETE FROM $tableName WHERE id = :id")
            .param("id", id)
            .update()
        return count > 0
    }

    override fun findByLabel(label: String): List<NamedEntityData> {
        val findSql = sql.load("entities/find-entities-by-label")
        return jdbcClient.sql(findSql)
            .param("label", label)
            .query { rs, _ -> mapRow(rs) }
            .list()
    }

    @Suppress("UNCHECKED_CAST")
    override fun vectorSearch(
        request: TextSimilaritySearchRequest,
        metadataFilter: PropertyFilter?,
        entityFilter: EntityFilter?,
    ): List<SimilarityResult<NamedEntityData>> {
        if (embeddingService == null) {
            logger.warn("Vector search requested but no embedding service configured")
            return emptyList()
        }

        val queryEmbedding = embeddingService.embed(request.query).joinToString(",", "[", "]")

        var searchSql = sql.load("entities/vector-search-entities")
        val params = mutableMapOf<String, Any?>(
            "embedding" to queryEmbedding,
            "topK" to request.topK,
        )

        val filterResult = filterConverter.combineFilters(metadataFilter, entityFilter)
        if (!filterResult.isEmpty()) {
            searchSql = searchSql.replace(
                "WHERE embedding IS NOT NULL",
                "WHERE embedding IS NOT NULL AND ${filterResult.whereClause}"
            )
            params.putAll(filterResult.parameters)
        }

        var spec = jdbcClient.sql(searchSql)
        params.forEach { (key, value) -> spec = spec.param(key, value) }

        return spec.query { rs, _ ->
            val entity: NamedEntityData = mapRow(rs)
            val score = rs.getDouble("score")
            SimilarityResult(entity, score)
        }.list().filter { it.score >= request.similarityThreshold }
    }

    @Suppress("UNCHECKED_CAST")
    override fun textSearch(
        request: TextSimilaritySearchRequest,
        metadataFilter: PropertyFilter?,
        entityFilter: EntityFilter?,
    ): List<SimilarityResult<NamedEntityData>> {
        val query = request.query.lowercase()
        var searchSql = """
            SELECT id, name, description, uri, labels, properties, metadata, context_id, embedding,
                   1.0 AS score
            FROM $tableName
            WHERE (LOWER(name) LIKE :query OR LOWER(description) LIKE :query)
        """.trimIndent()

        val params = mutableMapOf<String, Any?>(
            "query" to "%$query%",
        )

        val filterResult = filterConverter.combineFilters(metadataFilter, entityFilter)
        if (!filterResult.isEmpty()) {
            searchSql += " AND ${filterResult.whereClause}"
            params.putAll(filterResult.parameters)
        }

        searchSql += " LIMIT :topK"
        params["topK"] = request.topK

        var spec = jdbcClient.sql(searchSql)
        params.forEach { (key, value) -> spec = spec.param(key, value) }

        return spec.query { rs, _ ->
            val entity: NamedEntityData = mapRow(rs)
            val score = rs.getDouble("score")
            SimilarityResult(entity, score)
        }.list()
    }

    override fun createRelationship(
        a: RetrievableIdentifier,
        b: RetrievableIdentifier,
        relationship: RelationshipData
    ) {
        val createSql = sql.load(
            "entities/create-relationship",
            mapOf("relationshipTable" to relationshipTableName)
        )
        jdbcClient.sql(createSql)
            .param("sourceId", a.id)
            .param("targetId", b.id)
            .param("relationshipName", relationship.name)
            .param("properties", objectMapper.writeValueAsString(relationship.properties))
            .update()
    }

    override fun mergeRelationship(a: RetrievableIdentifier, b: RetrievableIdentifier, relationship: RelationshipData) {
        val mergeSql = sql.load(
            "entities/merge-relationship",
            mapOf("relationshipTable" to relationshipTableName)
        )
        jdbcClient.sql(mergeSql)
            .param("sourceId", a.id)
            .param("targetId", b.id)
            .param("relationshipName", relationship.name)
            .param("properties", objectMapper.writeValueAsString(relationship.properties))
            .update()
    }

    override fun findRelated(
        source: RetrievableIdentifier,
        relationshipName: String,
        direction: RelationshipDirection,
    ): List<NamedEntityData> {
        val findSql = sql.load(
            "entities/find-related",
            mapOf("relationshipTable" to relationshipTableName)
        )
        return jdbcClient.sql(findSql)
            .param("sourceId", source.id)
            .param("relationshipName", relationshipName)
            .param("direction", direction.name)
            .query { rs, _ -> mapRow(rs) }
            .list()
    }

    override fun withContextScope(contextId: String): NamedEntityDataRepository {
        return ContextScopedRepository(this, contextId)
    }

    // === Native store hooks ===

    override fun isNativeType(type: Class<*>): Boolean =
        nativeLookups.containsKey(type)

    @Suppress("UNCHECKED_CAST")
    override fun <T : NamedEntity> findNativeById(id: String, type: Class<T>): T? {
        val lookup = nativeLookups[type] as? NativeEntityLookup<T> ?: return null
        return lookup.findById(id)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : NamedEntity> findNativeAll(type: Class<T>): List<T>? {
        val lookup = nativeLookups[type] as? NativeEntityLookup<T> ?: return null
        return lookup.findAll()
    }

    // === Row mapping ===

    @Suppress("UNCHECKED_CAST")
    private fun mapRow(rs: ResultSet): SimpleNamedEntityData {
        val labelsArray = rs.getArray("labels")
        val labelsSet = if (labelsArray != null) {
            (labelsArray.array as Array<String>).toSet()
        } else {
            emptySet()
        }

        val propertiesJson = rs.getString("properties") ?: "{}"
        val properties: Map<String, Any> = objectMapper.readValue(
            propertiesJson, objectMapper.typeFactory.constructMapType(
                HashMap::class.java, String::class.java, Any::class.java
            )
        )

        val metadataJson = rs.getString("metadata") ?: "{}"
        val metadata: Map<String, Any?> = objectMapper.readValue(
            metadataJson, objectMapper.typeFactory.constructMapType(
                HashMap::class.java, String::class.java, Any::class.java
            )
        )

        return SimpleNamedEntityData(
            id = rs.getString("id"),
            name = rs.getString("name"),
            description = rs.getString("description") ?: "",
            uri = rs.getString("uri"),
            labels = labelsSet,
            properties = properties,
            metadata = metadata,
        )
    }

    companion object {

        @JvmStatic
        fun builder(): JdbcNamedEntityDataRepositoryBuilder = JdbcNamedEntityDataRepositoryBuilder()
    }

    /**
     * Context-scoped wrapper that filters queries by context_id.
     */
    private class ContextScopedRepository(
        private val delegate: JdbcNamedEntityDataRepository,
        private val contextId: String,
    ) : NamedEntityDataRepository by delegate {

        override fun findByLabel(label: String): List<NamedEntityData> {
            return delegate.findByLabel(label).filter { entity ->
                entity.metadata["context_id"] == contextId
            }
        }

        override fun vectorSearch(
            request: TextSimilaritySearchRequest,
            metadataFilter: PropertyFilter?,
            entityFilter: EntityFilter?,
        ): List<SimilarityResult<NamedEntityData>> {
            return delegate.vectorSearch(request, metadataFilter, entityFilter).filter { result ->
                result.match.metadata["context_id"] == contextId
            }
        }

        override fun textSearch(
            request: TextSimilaritySearchRequest,
            metadataFilter: PropertyFilter?,
            entityFilter: EntityFilter?,
        ): List<SimilarityResult<NamedEntityData>> {
            return delegate.textSearch(request, metadataFilter, entityFilter).filter { result ->
                result.match.metadata["context_id"] == contextId
            }
        }
    }
}
