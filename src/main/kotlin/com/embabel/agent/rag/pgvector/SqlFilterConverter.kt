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

import com.embabel.agent.filter.PropertyFilter
import com.embabel.agent.rag.filter.EntityFilter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule

/**
 * Result of converting a [com.embabel.agent.rag.filter.PropertyFilter] to SQL WHERE clause components.
 *
 * @property whereClause The SQL WHERE clause fragment (without the "WHERE" keyword)
 * @property parameters Map of parameter names to values for parameterized queries
 */
data class SqlFilterResult(
    val whereClause: String,
    val parameters: Map<String, Any>,
) {
    companion object {
        val EMPTY = SqlFilterResult("", emptyMap())
    }

    /**
     * Returns true if this result represents no filter (empty WHERE clause).
     */
    fun isEmpty(): Boolean = whereClause.isEmpty()

    /**
     * Appends this filter to an existing WHERE clause with AND.
     * Returns the original if this filter is empty.
     */
    fun appendTo(existingWhereClause: String): String {
        if (isEmpty()) return existingWhereClause
        return if (existingWhereClause.isBlank()) {
            whereClause
        } else {
            "$existingWhereClause AND $whereClause"
        }
    }
}

/**
 * Converts [PropertyFilter] and [EntityFilter] to PostgreSQL WHERE clause components.
 *
 * This converter handles:
 * - **Metadata filters**: Properties stored in the JSONB `metadata` column
 * - **Entity filters**: Label-based filtering using the `labels` TEXT[] column
 *
 * Example usage:
 * ```kotlin
 * val converter = SqlFilterConverter()
 *
 * // Metadata filter
 * val filter = PropertyFilter.eq("owner", "alice") and PropertyFilter.gte("score", 0.8)
 * val result = converter.convert(filter, isMetadata = true)
 * // whereClause = "(metadata @> :_filter_0::jsonb) AND ((metadata->>'score')::numeric >= :_filter_1)"
 * // parameters = mapOf("_filter_0" to """{"owner":"alice"}""", "_filter_1" to 0.8)
 *
 * // Entity filter with labels
 * val entityFilter = EntityFilter.hasAnyLabel("Person", "Organization")
 * val result = converter.convertEntityFilter(entityFilter)
 * // whereClause = "labels && :_filter_0::text[]"
 * // parameters = mapOf("_filter_0" to arrayOf("Person", "Organization"))
 * ```
 *
 * @param paramPrefix Prefix for generated parameter names (default: "_filter_")
 */
class SqlFilterConverter(
    private val paramPrefix: String = "_filter_",
) {
    private val objectMapper = ObjectMapper().apply {
        registerModule(JavaTimeModule())
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    /**
     * Converts a [PropertyFilter] to SQL WHERE clause components for metadata filtering.
     *
     * @param filter The filter to convert, or null for no filtering.
     * @return [SqlFilterResult] with the WHERE clause and parameters.
     */
    fun convert(filter: PropertyFilter?): SqlFilterResult {
        if (filter == null) return SqlFilterResult.EMPTY

        val parameters = mutableMapOf<String, Any>()
        val paramCounter = ParamCounter()
        val whereClause = convertFilter(filter, parameters, paramCounter)
        return SqlFilterResult(whereClause, parameters)
    }

    /**
     * Converts an [EntityFilter] to SQL WHERE clause components.
     *
     * @param filter The entity filter to convert, or null for no filtering.
     * @return [SqlFilterResult] with the WHERE clause and parameters.
     */
    fun convertEntityFilter(filter: EntityFilter?): SqlFilterResult {
        if (filter == null) return SqlFilterResult.EMPTY

        val parameters = mutableMapOf<String, Any>()
        val paramCounter = ParamCounter()
        val whereClause = convertFilter(filter, parameters, paramCounter)
        return SqlFilterResult(whereClause, parameters)
    }

    /**
     * Combines metadata and entity filters into a single result.
     * Uses a shared parameter counter to ensure unique parameter names.
     *
     * @param metadataFilter Filter on metadata properties
     * @param entityFilter Filter on entity labels
     * @return Combined [SqlFilterResult]
     */
    fun combineFilters(
        metadataFilter: PropertyFilter?,
        entityFilter: EntityFilter?,
    ): SqlFilterResult {
        if (metadataFilter == null && entityFilter == null) {
            return SqlFilterResult.EMPTY
        }

        val parameters = mutableMapOf<String, Any>()
        val counter = ParamCounter()
        val clauses = mutableListOf<String>()

        if (metadataFilter != null) {
            clauses.add(convertFilter(metadataFilter, parameters, counter))
        }

        if (entityFilter != null) {
            clauses.add(convertFilter(entityFilter, parameters, counter))
        }

        val combinedClause = when (clauses.size) {
            0 -> ""
            1 -> clauses.first()
            else -> clauses.joinToString(" AND ") { "($it)" }
        }

        return SqlFilterResult(combinedClause, parameters)
    }

    private fun convertFilter(
        filter: PropertyFilter,
        params: MutableMap<String, Any>,
        counter: ParamCounter,
    ): String = when (filter) {
        is PropertyFilter.Eq -> {
            val paramName = "$paramPrefix${counter.next()}"
            // Use JSONB containment for efficient indexed lookup
            params[paramName] = objectMapper.writeValueAsString(mapOf(filter.key to filter.value))
            "metadata @> :$paramName::jsonb"
        }

        is PropertyFilter.Ne -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = objectMapper.writeValueAsString(mapOf(filter.key to filter.value))
            "NOT (metadata @> :$paramName::jsonb)"
        }

        is PropertyFilter.Gt -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.value
            "(metadata->>'${filter.key}')::numeric > :$paramName"
        }

        is PropertyFilter.Gte -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.value
            "(metadata->>'${filter.key}')::numeric >= :$paramName"
        }

        is PropertyFilter.Lt -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.value
            "(metadata->>'${filter.key}')::numeric < :$paramName"
        }

        is PropertyFilter.Lte -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.value
            "(metadata->>'${filter.key}')::numeric <= :$paramName"
        }

        is PropertyFilter.In -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.values.toTypedArray()
            "metadata->>'${filter.key}' = ANY(:$paramName)"
        }

        is PropertyFilter.Nin -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.values.toTypedArray()
            "NOT (metadata->>'${filter.key}' = ANY(:$paramName))"
        }

        is PropertyFilter.Contains -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = "%${filter.value}%"
            "metadata->>'${filter.key}' LIKE :$paramName"
        }

        is PropertyFilter.ContainsIgnoreCase -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = "%${filter.value.lowercase()}%"
            "LOWER(metadata->>'${filter.key}') LIKE :$paramName"
        }

        is PropertyFilter.EqIgnoreCase -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.value.lowercase()
            "LOWER(metadata->>'${filter.key}') = :$paramName"
        }

        is PropertyFilter.StartsWith -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = "${filter.value}%"
            "metadata->>'${filter.key}' LIKE :$paramName"
        }

        is PropertyFilter.EndsWith -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = "%${filter.value}"
            "metadata->>'${filter.key}' LIKE :$paramName"
        }

        is PropertyFilter.Like -> {
            val paramName = "$paramPrefix${counter.next()}"
            params[paramName] = filter.pattern
            "metadata->>'${filter.key}' ~ :$paramName"
        }

        is PropertyFilter.And -> {
            val clauses = filter.filters.map { convertFilter(it, params, counter) }
            if (clauses.size == 1) {
                clauses.first()
            } else {
                clauses.joinToString(" AND ") { "($it)" }
            }
        }

        is PropertyFilter.Or -> {
            val clauses = filter.filters.map { convertFilter(it, params, counter) }
            if (clauses.size == 1) {
                clauses.first()
            } else {
                clauses.joinToString(" OR ") { "($it)" }
            }
        }

        is PropertyFilter.Not -> {
            val inner = convertFilter(filter.filter, params, counter)
            "NOT ($inner)"
        }

        is EntityFilter.HasAnyLabel -> {
            val paramName = "$paramPrefix${counter.next()}"
            // Use array overlap operator for label matching
            params[paramName] = filter.labels.toTypedArray()
            "labels && :$paramName::text[]"
        }

        else -> {
            throw IllegalArgumentException("Unsupported filter type: ${filter::class}")
        }
    }

    /**
     * Counter for generating unique parameter names.
     */
    private class ParamCounter {
        private var count = 0
        fun next(): Int = count++
    }
}
