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

import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import java.nio.charset.StandardCharsets

/**
 * Loads and processes SQL templates from classpath resources.
 *
 * SQL templates can contain placeholders that are replaced at runtime:
 * - `{table}` - replaced with the content element table name
 * - `{embeddingDimension}` - replaced with the embedding dimension
 *
 * SQL files are loaded from `sql/` directory on the classpath.
 *
 * @property tableName The table name to substitute for `{table}` placeholders
 * @property embeddingDimension The embedding dimension to substitute
 */
class SqlResourceLoader(
    private val tableName: String,
    private val embeddingDimension: Int
) {
    private val logger = LoggerFactory.getLogger(SqlResourceLoader::class.java)
    private val cache = mutableMapOf<String, String>()

    /**
     * Loads a SQL template from the classpath and substitutes placeholders.
     *
     * @param name The SQL resource name (without `.sql` extension)
     * @return The processed SQL with placeholders replaced
     * @throws IllegalArgumentException if the resource cannot be found
     */
    fun load(name: String): String {
        return cache.getOrPut(name) {
            val resourcePath = "sql/$name.sql"
            logger.debug("Loading SQL resource: {}", resourcePath)

            val resource = ClassPathResource(resourcePath)
            if (!resource.exists()) {
                throw IllegalArgumentException("SQL resource not found: $resourcePath")
            }

            val sql = resource.inputStream.use { inputStream ->
                String(inputStream.readAllBytes(), StandardCharsets.UTF_8)
            }

            processTemplate(sql)
        }
    }

    /**
     * Loads a SQL template and allows additional runtime substitutions.
     *
     * @param name The SQL resource name (without `.sql` extension)
     * @param substitutions Additional key-value pairs for substitution
     * @return The processed SQL with all placeholders replaced
     */
    fun load(name: String, substitutions: Map<String, String>): String {
        val baseSql = load(name)
        return substitutions.entries.fold(baseSql) { sql, (key, value) ->
            sql.replace("{$key}", value)
        }
    }

    /**
     * Processes a SQL template string, replacing standard placeholders.
     */
    private fun processTemplate(sql: String): String {
        return sql
            .replace("{table}", tableName)
            .replace("{embeddingDimension}", embeddingDimension.toString())
    }

    /**
     * Clears the SQL cache. Useful for testing.
     */
    fun clearCache() {
        cache.clear()
    }

    companion object {
        /**
         * Creates a SQL resource loader for the given properties.
         */
        @JvmStatic
        fun forProperties(properties: PgVectorStoreProperties): SqlResourceLoader {
            return SqlResourceLoader(
                tableName = properties.contentElementTable,
                embeddingDimension = properties.embeddingDimension
            )
        }
    }
}
