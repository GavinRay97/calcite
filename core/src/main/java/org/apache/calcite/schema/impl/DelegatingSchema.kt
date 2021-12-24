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
 * Implementation of [org.apache.calcite.schema.Schema] that delegates to
 * an underlying schema.
 */
class DelegatingSchema(schema: Schema) : Schema {
    protected val schema: Schema

    /**
     * Creates a DelegatingSchema.
     *
     * @param schema Underlying schema
     */
    init {
        this.schema = schema
    }

    @Override
    override fun toString(): String {
        return "DelegatingSchema(delegate=$schema)"
    }

    @get:Override
    val isMutable: Boolean
        get() = schema.isMutable()

    @Override
    fun snapshot(version: SchemaVersion?): Schema {
        return schema.snapshot(version)
    }

    @Override
    fun getExpression(@Nullable parentSchema: SchemaPlus?, name: String?): Expression {
        return schema.getExpression(parentSchema, name)
    }

    @Override
    @Nullable
    fun getTable(name: String?): Table {
        return schema.getTable(name)
    }

    @get:Override
    val tableNames: Set<String>
        get() = schema.getTableNames()

    @Override
    @Nullable
    fun getType(name: String?): RelProtoDataType {
        return schema.getType(name)
    }

    @get:Override
    val typeNames: Set<String>
        get() = schema.getTypeNames()

    @Override
    fun getFunctions(name: String?): Collection<Function> {
        return schema.getFunctions(name)
    }

    @get:Override
    val functionNames: Set<String>
        get() = schema.getFunctionNames()

    @Override
    @Nullable
    fun getSubSchema(name: String?): Schema {
        return schema.getSubSchema(name)
    }

    @get:Override
    val subSchemaNames: Set<String>
        get() = schema.getSubSchemaNames()
}
