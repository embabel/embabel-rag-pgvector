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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Tests for [PgVectorStoreProperties].
 */
class PgVectorStorePropertiesTest {

    @Test
    fun `default values should be correct`() {
        val properties = PgVectorStoreProperties()

        assertEquals("pgvector-store", properties.name)
        assertEquals("content_elements", properties.contentElementTable)
        assertEquals("public", properties.schemaName)
        assertEquals(0.7, properties.vectorWeight, 0.001)
        assertEquals(0.3, properties.ftsWeight, 0.001)
        assertEquals(0.2, properties.fuzzyThreshold, 0.001)
    }

    @Test
    fun `custom values should be set`() {
        val properties = PgVectorStoreProperties(
            name = "custom-store",
            contentElementTable = "custom_table",
            schemaName = "custom_schema",
            vectorWeight = 0.8,
            ftsWeight = 0.2,
            fuzzyThreshold = 0.3
        )

        assertEquals("custom-store", properties.name)
        assertEquals("custom_table", properties.contentElementTable)
        assertEquals("custom_schema", properties.schemaName)
        assertEquals(0.8, properties.vectorWeight, 0.001)
        assertEquals(0.2, properties.ftsWeight, 0.001)
        assertEquals(0.3, properties.fuzzyThreshold, 0.001)
    }

    @Test
    fun `weights should sum to one by default`() {
        val properties = PgVectorStoreProperties()

        val sum = properties.vectorWeight + properties.ftsWeight
        assertEquals(1.0, sum, 0.001, "Default weights should sum to 1.0")
    }

    @Test
    fun `equality should work`() {
        val props1 = PgVectorStoreProperties(
            name = "store",
            contentElementTable = "table",
            schemaName = "schema",
            vectorWeight = 0.7,
            ftsWeight = 0.3,
            fuzzyThreshold = 0.2
        )
        val props2 = PgVectorStoreProperties(
            name = "store",
            contentElementTable = "table",
            schemaName = "schema",
            vectorWeight = 0.7,
            ftsWeight = 0.3,
            fuzzyThreshold = 0.2
        )

        assertEquals(props1, props2)
        assertEquals(props1.hashCode(), props2.hashCode())
    }

    @Test
    fun `toString should contain all fields`() {
        val properties = PgVectorStoreProperties(
            name = "test-store",
            contentElementTable = "test_table",
            schemaName = "test_schema",
            vectorWeight = 0.6,
            ftsWeight = 0.4,
            fuzzyThreshold = 0.25
        )

        val str = properties.toString()
        assertTrue(str.contains("test-store"))
        assertTrue(str.contains("test_table"))
        assertTrue(str.contains("test_schema"))
    }
}
