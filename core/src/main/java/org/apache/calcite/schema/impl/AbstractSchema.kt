/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.schema.impl

import org.apache.calcite.linq4j.tree.Expression

/**
 * Abstract implementation of [Schema].
 *
 *
 * Behavior is as follows:
 *
 *  * The schema has no tables unless you override
 * [.getTableMap].
 *  * The schema has no functions unless you override
 * [.getFunctionMultimap].
 *  * The schema has no sub-schemas unless you override
 * [.getSubSchemaMap].
 *  * The schema is mutable unless you override
 * [.isMutable].
 *  * The name and parent schema are as specified in the constructor
 * arguments.
 *
 */
class AbstractSchema : Schema {
    @get:Override
    val isMutable: Boolean
        get() = true

    @Override
    fun snapshot(version: SchemaVersion?): Schema {
        return this
    }

    @Override
    fun getExpression(@Nullable parentSchema: SchemaPlus?, name: String?): Expression {
        requireNonNull(parentSchema, "parentSchema")
        return Schemas.subSchemaExpression(parentSchema, name, getClass())
    }

    /**
     * Returns a map of tables in this schema by name.
     *
     *
     * The implementations of [.getTableNames]
     * and [.getTable] depend on this map.
     * The default implementation of this method returns the empty map.
     * Override this method to change their behavior.
     *
     * @return Map of tables in this schema by name
     */
    protected val tableMap: Map<String, Any>
        protected get() = ImmutableMap.of()

    @get:Override
    val tableNames: Set<String>
        get() = tableMap.keySet() as Set<String>

    @Override
    @Nullable
    fun getTable(name: String): Table? {
        return tableMap[name]
    }

    /**
     * Returns a map of types in this schema by name.
     *
     *
     * The implementations of [.getTypeNames]
     * and [.getType] depend on this map.
     * The default implementation of this method returns the empty map.
     * Override this method to change their behavior.
     *
     * @return Map of types in this schema by name
     */
    protected val typeMap: Map<String, Any>
        protected get() = ImmutableMap.of()

    @Override
    @Nullable
    fun getType(name: String): RelProtoDataType? {
        return typeMap[name]
    }

    @get:Override
    val typeNames: Set<String>
        get() = typeMap.keySet() as Set<String>

    /**
     * Returns a multi-map of functions in this schema by name.
     * It is a multi-map because functions are overloaded; there may be more than
     * one function in a schema with a given name (as long as they have different
     * parameter lists).
     *
     *
     * The implementations of [.getFunctionNames]
     * and [Schema.getFunctions] depend on this map.
     * The default implementation of this method returns the empty multi-map.
     * Override this method to change their behavior.
     *
     * @return Multi-map of functions in this schema by name
     */
    protected val functionMultimap: Multimap<String, Function>
        protected get() = ImmutableMultimap.of()

    @Override
    fun getFunctions(name: String?): Collection<Function> {
        return functionMultimap.get(name) // never null
    }

    @get:Override
    val functionNames: Set<String>
        get() = functionMultimap.keySet()

    /**
     * Returns a map of sub-schemas in this schema by name.
     *
     *
     * The implementations of [.getSubSchemaNames]
     * and [.getSubSchema] depend on this map.
     * The default implementation of this method returns the empty map.
     * Override this method to change their behavior.
     *
     * @return Map of sub-schemas in this schema by name
     */
    protected val subSchemaMap: Map<String, Any>
        protected get() = ImmutableMap.of()

    @get:Override
    val subSchemaNames: Set<String>
        get() = subSchemaMap.keySet() as Set<String>

    @Override
    @Nullable
    fun getSubSchema(name: String): Schema? {
        return subSchemaMap[name]
    }

    /** Schema factory that creates an
     * [org.apache.calcite.schema.impl.AbstractSchema].  */
    class Factory private constructor() : SchemaFactory {
        @Override
        fun create(
            parentSchema: SchemaPlus?, name: String?,
            operand: Map<String?, Object?>?
        ): Schema {
            return AbstractSchema()
        }

        companion object {
            val INSTANCE = Factory()
        }
    }
}
