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

import org.apache.calcite.avatica.AvaticaResultSet

/**
 * Implementation of [ResultSet]
 * for the Calcite engine.
 */
class CalciteResultSet
/** Creates a CalciteResultSet.  */
internal constructor(
    statement: AvaticaStatement?,
    calciteSignature: CalcitePrepare.CalciteSignature?,
    resultSetMetaData: ResultSetMetaData?, timeZone: TimeZone?,
    firstFrame: Meta.Frame?
) : AvaticaResultSet(statement, null, calciteSignature, resultSetMetaData, timeZone, firstFrame) {
    @Override
    @Throws(SQLException::class)
    protected fun execute(): CalciteResultSet {
        // Call driver's callback. It is permitted to throw a RuntimeException.
        val connection: CalciteConnectionImpl = calciteConnection
        val autoTemp: Boolean = connection.config().autoTemp()
        var resultSink: Handler.ResultSink? = null
        if (autoTemp) {
            resultSink = Handler.ResultSink {}
        }
        connection.getDriver().handler.onStatementExecute(statement, resultSink)
        super.execute()
        return this
    }

    @Override
    @Throws(SQLException::class)
    fun create(
        elementType: ColumnMetaData.AvaticaType,
        iterable: Iterable<Object?>
    ): ResultSet {
        val columnMetaDataList: List<ColumnMetaData>
        columnMetaDataList = if (elementType is ColumnMetaData.StructType) {
            (elementType as ColumnMetaData.StructType).columns
        } else {
            ImmutableList.of(ColumnMetaData.dummy(elementType, false))
        }
        val signature: CalcitePrepare.CalciteSignature = this.signature as CalcitePrepare.CalciteSignature
        val newSignature: CalcitePrepare.CalciteSignature<Object> = CalciteSignature(
            signature.sql,
            signature.parameters, signature.internalParameters,
            signature.rowType, columnMetaDataList, Meta.CursorFactory.ARRAY,
            signature.rootSchema, ImmutableList.of(), -1, null,
            statement.getStatementType()
        )
        val subResultSetMetaData: ResultSetMetaData = AvaticaResultSetMetaData(statement, null, newSignature)
        val resultSet = CalciteResultSet(
            statement, signature, subResultSetMetaData,
            localCalendar.getTimeZone(), Frame(0, true, iterable)
        )
        val cursor: Cursor = createCursor(elementType, iterable)
        return resultSet.execute2(cursor, columnMetaDataList)
    }

    // do not make public
    fun <T> getSignature(): CalcitePrepare.CalciteSignature<T> {
        return signature as CalcitePrepare.CalciteSignature
    }

    // do not make public
    @get:Throws(SQLException::class)
    val calciteConnection: org.apache.calcite.jdbc.CalciteConnectionImpl
        get() = statement.getConnection()

    companion object {
        private fun createCursor(
            elementType: ColumnMetaData.AvaticaType,
            iterable: Iterable
        ): Cursor {
            val enumerator: Enumerator = Linq4j.iterableEnumerator(iterable)
            return if (elementType !is ColumnMetaData.StructType
                || (elementType as ColumnMetaData.StructType).columns.size() === 1
            ) ObjectEnumeratorCursor(enumerator) else ArrayEnumeratorCursor(enumerator)
        }
    }
}
