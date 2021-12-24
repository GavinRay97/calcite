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
package org.apache.calcite.adapter.clone

import org.apache.calcite.adapter.java.AbstractQueryableTable

/**
 * Implementation of table that reads rows from a read-only list and returns
 * an enumerator of rows. Each row is object (if there is just one column) or
 * an object array (if there are multiple columns).
 */
internal class ListTable(
    elementType: Type?,
    protoRowType: RelProtoDataType,
    expression: Expression,
    list: List
) : AbstractQueryableTable(elementType) {
    private val protoRowType: RelProtoDataType
    private val expression: Expression
    private val list: List

    /** Creates a ListTable.  */
    init {
        this.protoRowType = protoRowType
        this.expression = expression
        this.list = list
    }

    @Override
    fun getRowType(typeFactory: RelDataTypeFactory?): RelDataType {
        return protoRowType.apply(typeFactory)
    }

    @get:Override
    val statistic: Statistic
        get() = Statistics.of(list.size(), ImmutableList.of())

    @Override
    fun <T> asQueryable(
        queryProvider: QueryProvider,
        schema: SchemaPlus?, tableName: String?
    ): Queryable<T> {
        return object : AbstractQueryable<T>() {
            @get:Override
            val elementType: Type

            @get:Override
            val expression: Expression

            @get:Override
            val provider: QueryProvider
                get() = queryProvider

            @Override
            operator fun iterator(): Iterator<T> {
                return list.iterator()
            }

            @Override
            fun enumerator(): Enumerator<T> {
                return Linq4j.enumerator(list)
            }
        }
    }
}
