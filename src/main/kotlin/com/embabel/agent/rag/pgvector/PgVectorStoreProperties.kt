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

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Configuration properties for [PgVectorStore].
 *
 * @property name Name of this store instance
 * @property contentElementTable Name of the table for storing content elements
 * @property schemaName PostgreSQL schema name (defaults to public)
 */
@ConfigurationProperties(prefix = "embabel.rag.pgvector")
data class PgVectorStoreProperties(
    val name: String = "pgvector-store",
    val contentElementTable: String = "content_elements",
    val schemaName: String = "public"
)
