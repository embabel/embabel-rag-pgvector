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
import com.embabel.common.ai.model.EmbeddingService
import org.springframework.ai.vectorstore.VectorStore
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate

/**
 * Auto-configuration for [PgVectorStore].
 *
 * This configuration is activated when:
 * - Spring AI's VectorStore is on the classpath
 * - A VectorStore bean is available
 * - A JdbcTemplate bean is available
 */
@AutoConfiguration
@ConditionalOnClass(VectorStore::class)
@EnableConfigurationProperties(PgVectorStoreProperties::class)
class PgVectorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(VectorStore::class, JdbcTemplate::class)
    fun pgVectorStore(
        vectorStore: VectorStore,
        jdbcTemplate: JdbcTemplate,
        properties: PgVectorStoreProperties,
        chunkerConfig: ContentChunker.Config,
        chunkTransformer: ChunkTransformer,
        embeddingService: EmbeddingService?,
        enhancers: List<RetrievableEnhancer>
    ): PgVectorStore {
        return PgVectorStore(
            vectorStore = vectorStore,
            jdbcTemplate = jdbcTemplate,
            properties = properties,
            chunkerConfig = chunkerConfig,
            chunkTransformer = chunkTransformer,
            embeddingService = embeddingService,
            enhancers = enhancers
        )
    }
}
