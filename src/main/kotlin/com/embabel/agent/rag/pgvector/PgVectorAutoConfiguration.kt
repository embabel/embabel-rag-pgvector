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
import com.embabel.common.ai.model.EmbeddingService
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.simple.JdbcClient
import javax.sql.DataSource

/**
 * Auto-configuration for [PgVectorStore].
 *
 * This configuration is activated when:
 * - A DataSource bean is available
 * - JdbcClient can be created from the DataSource
 */
@AutoConfiguration
@EnableConfigurationProperties(PgVectorStoreProperties::class)
class PgVectorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(JdbcClient::class)
    @ConditionalOnBean(DataSource::class)
    fun jdbcClient(dataSource: DataSource): JdbcClient {
        return JdbcClient.create(dataSource)
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(JdbcClient::class)
    fun pgVectorStore(
        jdbcClient: JdbcClient,
        properties: PgVectorStoreProperties,
        chunkerConfig: ContentChunker.Config?,
        chunkTransformer: ChunkTransformer?,
        embeddingService: EmbeddingService?,
        enhancers: List<RetrievableEnhancer>?
    ): PgVectorStore {
        return PgVectorStore(
            jdbcClient = jdbcClient,
            properties = properties,
            chunkerConfig = chunkerConfig ?: ContentChunker.Config(),
            chunkTransformer = chunkTransformer ?: ChunkTransformer.NO_OP,
            embeddingService = embeddingService,
            enhancers = enhancers ?: emptyList()
        )
    }
}
