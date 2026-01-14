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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import javax.sql.DataSource

/**
 * Tests for [PgVectorAutoConfiguration].
 */
class PgVectorAutoConfigurationTest {

    @Test
    fun `jdbcClient bean should be created from dataSource`() {
        val config = PgVectorAutoConfiguration()
        val mockDataSource = mock(DataSource::class.java)

        val jdbcClient = config.jdbcClient(mockDataSource)

        assertNotNull(jdbcClient)
    }

    @Test
    fun `configuration class should have correct annotations`() {
        assertTrue(
            PgVectorAutoConfiguration::class.java.isAnnotationPresent(AutoConfiguration::class.java),
            "Should have @AutoConfiguration annotation"
        )

        assertTrue(
            PgVectorAutoConfiguration::class.java.isAnnotationPresent(EnableConfigurationProperties::class.java),
            "Should have @EnableConfigurationProperties annotation"
        )
    }

    @Test
    fun `EnableConfigurationProperties should reference PgVectorStoreProperties`() {
        val annotation = PgVectorAutoConfiguration::class.java
            .getAnnotation(EnableConfigurationProperties::class.java)

        assertNotNull(annotation)
        assertTrue(
            annotation.value.contains(PgVectorStoreProperties::class),
            "Should enable PgVectorStoreProperties"
        )
    }
}
