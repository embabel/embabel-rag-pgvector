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

import com.embabel.agent.rag.model.NamedEntity
import com.embabel.agent.rag.service.NativeFinder

/**
 * Functional interface for native entity lookup, avoiding JPA dependency in this module.
 * Callers register these from their JPA repositories.
 */
interface NativeEntityLookup<T> {
    fun findById(id: String): T?
    fun findAll(): List<T>
}

/**
 * [NativeFinder] that delegates to a map of [NativeEntityLookup] instances
 * registered per entity type.
 *
 * Returns null for types without a registered lookup, allowing fallback to generic lookup.
 *
 * @param nativeLookups map from entity class to its native lookup implementation
 */
class JdbcNativeFinder(
    private val nativeLookups: Map<Class<*>, NativeEntityLookup<*>>,
) : NativeFinder {

    @Suppress("UNCHECKED_CAST")
    override fun <T : NamedEntity> findById(id: String, type: Class<T>): T? {
        val lookup = nativeLookups[type] as? NativeEntityLookup<T> ?: return null
        return lookup.findById(id)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : NamedEntity> findAll(type: Class<T>): List<T>? {
        val lookup = nativeLookups[type] as? NativeEntityLookup<T> ?: return null
        return lookup.findAll()
    }
}
