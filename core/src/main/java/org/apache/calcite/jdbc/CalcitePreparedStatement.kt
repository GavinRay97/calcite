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

import org.apache.calcite.avatica.AvaticaPreparedStatement

/**
 * Implementation of [java.sql.PreparedStatement]
 * for the Calcite engine.
 *
 *
 * This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using
 * [org.apache.calcite.avatica.AvaticaFactory.newPreparedStatement].
 */
abstract class CalcitePreparedStatement
/**
 * Creates a CalcitePreparedStatement.
 *
 * @param connection Connection
 * @param h Statement handle
 * @param signature Result of preparing statement
 * @param resultSetType Result set type
 * @param resultSetConcurrency Result set concurrency
 * @param resultSetHoldability Result set holdability
 * @throws SQLException if database error occurs
 */
protected constructor(
    connection: CalciteConnectionImpl?,
    h: @Nullable Meta.StatementHandle?, signature: Meta.Signature?, resultSetType: Int,
    resultSetConcurrency: Int, resultSetHoldability: Int
) : AvaticaPreparedStatement(
    connection, h, signature, resultSetType, resultSetConcurrency,
    resultSetHoldability
) {
    @get:Throws(SQLException::class)
    @get:Override
    val connection: org.apache.calcite.jdbc.CalciteConnectionImpl
        get() = super.getConnection() as CalciteConnectionImpl
}
