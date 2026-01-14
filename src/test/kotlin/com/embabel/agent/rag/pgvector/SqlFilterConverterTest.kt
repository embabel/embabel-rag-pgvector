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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SqlFilterConverterTest {

    private val converter = SqlFilterConverter()

    @Test
    fun `null filter returns empty result`() {
        val result = converter.convert(null)
        assertTrue(result.isEmpty())
        assertEquals("", result.whereClause)
        assertTrue(result.parameters.isEmpty())
    }

    @Test
    fun `Eq filter generates JSONB containment`() {
        val filter = PropertyFilter.eq("owner", "alice")
        val result = converter.convert(filter)

        assertFalse(result.isEmpty())
        assertEquals("metadata @> :_filter_0::jsonb", result.whereClause)
        assertEquals("""{"owner":"alice"}""", result.parameters["_filter_0"])
    }

    @Test
    fun `Ne filter generates negated JSONB containment`() {
        val filter = PropertyFilter.ne("status", "deleted")
        val result = converter.convert(filter)

        assertEquals("NOT (metadata @> :_filter_0::jsonb)", result.whereClause)
        assertEquals("""{"status":"deleted"}""", result.parameters["_filter_0"])
    }

    @Test
    fun `Gt filter generates numeric comparison`() {
        val filter = PropertyFilter.gt("score", 0.8)
        val result = converter.convert(filter)

        assertEquals("(metadata->>'score')::numeric > :_filter_0", result.whereClause)
        assertEquals(0.8, result.parameters["_filter_0"])
    }

    @Test
    fun `Gte filter generates numeric comparison`() {
        val filter = PropertyFilter.gte("count", 10)
        val result = converter.convert(filter)

        assertEquals("(metadata->>'count')::numeric >= :_filter_0", result.whereClause)
        assertEquals(10, result.parameters["_filter_0"])
    }

    @Test
    fun `Lt filter generates numeric comparison`() {
        val filter = PropertyFilter.lt("priority", 5)
        val result = converter.convert(filter)

        assertEquals("(metadata->>'priority')::numeric < :_filter_0", result.whereClause)
        assertEquals(5, result.parameters["_filter_0"])
    }

    @Test
    fun `Lte filter generates numeric comparison`() {
        val filter = PropertyFilter.lte("version", 2)
        val result = converter.convert(filter)

        assertEquals("(metadata->>'version')::numeric <= :_filter_0", result.whereClause)
        assertEquals(2, result.parameters["_filter_0"])
    }

    @Test
    fun `In filter generates ANY comparison`() {
        val filter = PropertyFilter.`in`("type", "blog", "article", "doc")
        val result = converter.convert(filter)

        assertEquals("metadata->>'type' = ANY(:_filter_0)", result.whereClause)
        val values = result.parameters["_filter_0"] as Array<*>
        assertEquals(3, values.size)
        assertTrue(values.contains("blog"))
        assertTrue(values.contains("article"))
        assertTrue(values.contains("doc"))
    }

    @Test
    fun `Nin filter generates NOT ANY comparison`() {
        val filter = PropertyFilter.nin("status", "deleted", "archived")
        val result = converter.convert(filter)

        assertEquals("NOT (metadata->>'status' = ANY(:_filter_0))", result.whereClause)
    }

    @Test
    fun `Contains filter generates LIKE comparison`() {
        val filter = PropertyFilter.contains("title", "kubernetes")
        val result = converter.convert(filter)

        assertEquals("metadata->>'title' LIKE :_filter_0", result.whereClause)
        assertEquals("%kubernetes%", result.parameters["_filter_0"])
    }

    @Test
    fun `And filter combines clauses with AND`() {
        val filter = PropertyFilter.eq("owner", "alice") and PropertyFilter.gte("score", 0.8)
        val result = converter.convert(filter)

        assertTrue(result.whereClause.contains(" AND "))
        assertTrue(result.whereClause.contains("metadata @> :_filter_0::jsonb"))
        assertTrue(result.whereClause.contains("(metadata->>'score')::numeric >= :_filter_1"))
        assertEquals(2, result.parameters.size)
    }

    @Test
    fun `Or filter combines clauses with OR`() {
        val filter = PropertyFilter.eq("type", "blog") or PropertyFilter.eq("type", "article")
        val result = converter.convert(filter)

        assertTrue(result.whereClause.contains(" OR "))
        assertEquals(2, result.parameters.size)
    }

    @Test
    fun `Not filter negates inner filter`() {
        val filter = !PropertyFilter.eq("status", "deleted")
        val result = converter.convert(filter)

        assertTrue(result.whereClause.startsWith("NOT ("))
        assertTrue(result.whereClause.endsWith(")"))
    }

    @Test
    fun `HasAnyLabel filter generates array overlap`() {
        val filter = EntityFilter.hasAnyLabel("Person", "Organization")
        val result = converter.convertEntityFilter(filter)

        assertEquals("labels && :_filter_0::text[]", result.whereClause)
        val labels = result.parameters["_filter_0"] as Array<*>
        assertEquals(2, labels.size)
        assertTrue(labels.contains("Person"))
        assertTrue(labels.contains("Organization"))
    }

    @Test
    fun `combineFilters combines metadata and entity filters`() {
        val metadataFilter = PropertyFilter.eq("owner", "alice")
        val entityFilter = EntityFilter.hasAnyLabel("Document")

        val result = converter.combineFilters(metadataFilter, entityFilter)

        assertFalse(result.isEmpty())
        assertTrue(result.whereClause.contains(" AND "))
        assertTrue(result.whereClause.contains("metadata @>"))
        assertTrue(result.whereClause.contains("labels &&"))
        assertEquals(2, result.parameters.size)
    }

    @Test
    fun `combineFilters with only metadata filter`() {
        val metadataFilter = PropertyFilter.eq("owner", "alice")

        val result = converter.combineFilters(metadataFilter, null)

        assertEquals("metadata @> :_filter_0::jsonb", result.whereClause)
        assertEquals(1, result.parameters.size)
    }

    @Test
    fun `combineFilters with only entity filter`() {
        val entityFilter = EntityFilter.hasAnyLabel("Document")

        val result = converter.combineFilters(null, entityFilter)

        assertEquals("labels && :_filter_0::text[]", result.whereClause)
        assertEquals(1, result.parameters.size)
    }

    @Test
    fun `combineFilters with no filters returns empty`() {
        val result = converter.combineFilters(null, null)
        assertTrue(result.isEmpty())
    }

    @Test
    fun `appendTo adds to existing where clause`() {
        val filter = PropertyFilter.eq("owner", "alice")
        val result = converter.convert(filter)

        val combined = result.appendTo("type = 'chunk'")

        assertEquals("type = 'chunk' AND metadata @> :_filter_0::jsonb", combined)
    }

    @Test
    fun `appendTo with empty filter returns original`() {
        val result = SqlFilterResult.EMPTY
        val combined = result.appendTo("type = 'chunk'")

        assertEquals("type = 'chunk'", combined)
    }

    @Test
    fun `complex nested filter generates correct SQL`() {
        val filter = (PropertyFilter.eq("owner", "alice") and PropertyFilter.gte("score", 0.5)) or
            (PropertyFilter.eq("owner", "bob") and PropertyFilter.gte("score", 0.8))
        val result = converter.convert(filter)

        assertTrue(result.whereClause.contains(" OR "))
        assertTrue(result.whereClause.contains(" AND "))
        assertEquals(4, result.parameters.size)
    }
}
