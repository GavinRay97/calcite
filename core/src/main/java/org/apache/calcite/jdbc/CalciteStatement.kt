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

import org.apache.calcite.avatica.AvaticaStatement

/**
 * Implementation of [java.sql.Statement]
 * for the Calcite engine.
 */
abstract class CalciteStatement
/**
 * Creates a CalciteStatement.
 *
 * @param connection Connection
 * @param h Statement handle
 * @param resultSetType Result set type
 * @param resultSetConcurrency Result set concurrency
 * @param resultSetHoldability Result set holdability
 */
internal constructor(
    connection: CalciteConnectionImpl?, h: @Nullable Meta.StatementHandle?,
    resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int
) : AvaticaStatement(
    connection, h, resultSetType, resultSetConcurrency,
    resultSetHoldability
) {
    // implement Statement
    @Override
    @Throws(SQLException::class)
    fun <T> unwrap(iface: Class<T>): T {
        if (iface === CalciteServerStatement::class.java) {
            val statement: CalciteServerStatement
            statement = try {
                connection.server.getStatement(handle)
            } catch (e: NoSuchStatementException) {
                throw AssertionError("invalid statement", e)
            }
            return iface.cast(statement)
        }
        return super.unwrap(iface)
    }

    @get:Override
    val connection: org.apache.calcite.jdbc.CalciteConnectionImpl
        get() = field

    fun <T> prepare(
        queryable: Queryable<T>?
    ): CalcitePrepare.CalciteSignature<T> {
        val calciteConnection: CalciteConnectionImpl = connection
        val prepare: CalcitePrepare = calciteConnection.prepareFactory.apply()
        val serverStatement: CalciteServerStatement
        serverStatement = try {
            calciteConnection.server.getStatement(handle)
        } catch (e: NoSuchStatementException) {
            throw AssertionError("invalid statement", e)
        }
        val prepareContext: CalcitePrepare.Context = serverStatement.createPrepareContext()
        return prepare.prepareQueryable(prepareContext, queryable)
    }

    @Override
    protected fun close_() {
        if (!closed) {
            connection.server.removeStatement(handle)
            super.close_()
        }
    }
}
