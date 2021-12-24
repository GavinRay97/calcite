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
package org.apache.calcite.jdbc

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Extension to Calcite's implementation of
 * [JDBC connection][java.sql.Connection] allows schemas to be defined
 * dynamically.
 *
 *
 * You can start off with an empty connection (no schemas), define one
 * or two schemas, and start querying them.
 *
 *
 * Since a `CalciteConnection` implements the linq4j
 * [QueryProvider] interface, you can use a connection to execute
 * expression trees as queries.
 */
interface CalciteConnection : Connection, QueryProvider {
    /**
     * Returns the root schema.
     *
     *
     * You can define objects (such as relations) in this schema, and
     * also nested schemas.
     *
     * @return Root schema
     */
    val rootSchema: SchemaPlus

    /**
     * Returns the type factory.
     *
     * @return Type factory
     */
    val typeFactory: JavaTypeFactory?

    /**
     * Returns an instance of the connection properties.
     *
     *
     * NOTE: The resulting collection of properties is same collection used
     * by the connection, and is writable, but behavior if you modify the
     * collection is undefined. Some implementations might, for example, see
     * a modified property, but only if you set it before you create a
     * statement. We will remove this method when there are better
     * implementations of stateful connections and configuration.
     *
     * @return properties
     */
    val properties: Properties?

    // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
    // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
    @get:Throws(SQLException::class)
    @get:Override
    @set:Throws(SQLException::class)
    @set:Override
    var schema: String?
    fun config(): CalciteConnectionConfig?

    /** Creates a context for preparing a statement for execution.  */
    fun createPrepareContext(): Context?
}
