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

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Tests for [PgVectorStoreBuilder].
 */
class PgVectorStoreBuilderTest {

    @Test
    fun `builder should have default values`() {
        val builder = PgVectorStore.builder()
        assertNotNull(builder)
    }

    @Test
    fun `builder should allow method chaining`() {
        val builder = PgVectorStore.builder()
            .withName("test-store")
            .withContentElementTable("test_elements")
            .withSchemaName("test_schema")
            .withVectorWeight(0.8)
            .withFtsWeight(0.2)
            .withFuzzyThreshold(0.3)

        assertNotNull(builder)
    }

    @Test
    fun `withName should create new builder instance`() {
        val original = PgVectorStore.builder()
        val modified = original.withName("new-name")

        // Data class copy() creates a new instance
        assertNotSame(original, modified)
    }

    @Test
    fun `withHybridWeights should set both weights`() {
        val builder = PgVectorStore.builder()
            .withHybridWeights(0.6, 0.4)

        assertNotNull(builder)
    }

    @Test
    fun `withVectorWeight should reject invalid values`() {
        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withVectorWeight(-0.1)
        }

        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withVectorWeight(1.1)
        }
    }

    @Test
    fun `withFtsWeight should reject invalid values`() {
        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withFtsWeight(-0.1)
        }

        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withFtsWeight(1.1)
        }
    }

    @Test
    fun `withFuzzyThreshold should reject invalid values`() {
        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withFuzzyThreshold(-0.1)
        }

        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withFuzzyThreshold(1.1)
        }
    }

    @Test
    fun `withHybridWeights should reject invalid values`() {
        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withHybridWeights(-0.1, 0.5)
        }

        assertThrows<IllegalArgumentException> {
            PgVectorStore.builder().withHybridWeights(0.5, 1.5)
        }
    }

    @Test
    fun `build should fail without vectorStore`() {
        val builder = PgVectorStore.builder()
            .withName("test-store")

        val exception = assertThrows<IllegalStateException> { builder.build() }
        assertTrue(exception.message?.contains("jdbcClient") == true ||
               exception.message?.contains("dataSource") == true)
    }

    @Test
    fun `build should fail without dataSource or jdbcClient`() {
        val builder = PgVectorStore.builder()
            .withName("test-store")

        val exception = assertThrows<IllegalStateException> { builder.build() }
        assertTrue(exception.message?.contains("jdbcClient") == true ||
               exception.message?.contains("dataSource") == true)
    }

    @Test
    fun `static withName factory method should work`() {
        val builder = PgVectorStore.withName("my-store")
        assertNotNull(builder)
    }
}
