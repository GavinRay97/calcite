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

import org.apache.calcite.linq4j.AbstractQueryable

/**
 * Abstract implementation of [org.apache.calcite.linq4j.Queryable] for
 * [QueryableTable].
 *
 *
 * Not to be confused with
 * [org.apache.calcite.adapter.java.AbstractQueryableTable].
 *
 * @param <T> element type
</T> */
abstract class AbstractTableQueryable<T> protected constructor(
    queryProvider: QueryProvider?,
    schema: SchemaPlus?, table: QueryableTable, tableName: String?
) : AbstractQueryable<T>() {
    val queryProvider: QueryProvider?
    val schema: SchemaPlus?
    val table: QueryableTable
    val tableName: String?

    init {
        this.queryProvider = queryProvider
        this.schema = schema
        this.table = table
        this.tableName = tableName
    }

    @get:Override
    val expression: Expression
        get() = table.getExpression(schema, tableName, Queryable::class.java)

    @get:Override
    val provider: QueryProvider?
        get() = queryProvider

    @get:Override
    val elementType: Type
        get() = table.getElementType()

    @Override
    operator fun iterator(): Iterator<T> {
        return Linq4j.enumeratorIterator(enumerator())
    }
}
