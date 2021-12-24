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
package org.apache.calcite.schema

import org.apache.calcite.linq4j.QueryProvider

/**
 * Extension to [Table] that can translate itself to a [Queryable].
 */
interface QueryableTable : Table {
    /** Converts this table into a [Queryable].  */
    fun <T> asQueryable(
        queryProvider: QueryProvider?, schema: SchemaPlus?,
        tableName: String?
    ): Queryable<T>?

    /** Returns the element type of the collection that will implement this
     * table.  */
    val elementType: Type?

    /** Generates an expression with which this table can be referenced in
     * generated code.
     *
     * @param schema Schema
     * @param tableName Table name (unique within schema)
     * @param clazz The desired collection class; for example `Queryable`.
     */
    fun getExpression(schema: SchemaPlus?, tableName: String?, clazz: Class?): Expression?
}
