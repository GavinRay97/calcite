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
package org.apache.calcite.runtime

import org.apache.calcite.DataContext

/**
 * Executes a SQL statement and returns the result as an [Enumerable].
 *
 * @param <T> Element type
</T> */
class ResultSetEnumerable<T> private constructor(
    dataSource: DataSource,
    sql: String,
    rowBuilderFactory: Function1<ResultSet, Function0<T>>,
    @Nullable preparedStatementEnricher: PreparedStatementEnricher?
) : AbstractEnumerable<T>() {
    private val dataSource: DataSource
    private val sql: String
    private val rowBuilderFactory: Function1<ResultSet, Function0<T>>

    @Nullable
    private val preparedStatementEnricher: PreparedStatementEnricher?

    @Nullable
    private var queryStart: Long? = null
    private var timeout: Long = 0
    private var timeoutSetFailed = false

    init {
        this.dataSource = dataSource
        this.sql = sql
        this.rowBuilderFactory = rowBuilderFactory
        this.preparedStatementEnricher = preparedStatementEnricher
    }

    private constructor(
        dataSource: DataSource,
        sql: String,
        rowBuilderFactory: Function1<ResultSet, Function0<T>>
    ) : this(dataSource, sql, rowBuilderFactory, null) {
    }

    fun setTimeout(context: DataContext) {
        queryStart = context.get(DataContext.Variable.UTC_TIMESTAMP.camelName)
        val timeout: Object = context.get(DataContext.Variable.TIMEOUT.camelName)
        if (timeout is Long) {
            this.timeout = timeout
        } else {
            if (timeout != null) {
                LOGGER.debug("Variable.TIMEOUT should be `long`. Given value was {}", timeout)
            }
            this.timeout = 0
        }
    }

    @Override
    fun enumerator(): Enumerator<T> {
        return if (preparedStatementEnricher == null) {
            enumeratorBasedOnStatement()
        } else {
            enumeratorBasedOnPreparedStatement()
        }
    }

    private fun enumeratorBasedOnStatement(): Enumerator<T> {
        var connection: Connection? = null
        var statement: Statement? = null
        return try {
            connection = dataSource.getConnection()
            statement = connection.createStatement()
            setTimeoutIfPossible(statement)
            if (statement.execute(sql)) {
                val resultSet: ResultSet = statement.getResultSet()
                statement = null
                connection = null
                ResultSetEnumerator<Any>(resultSet, rowBuilderFactory)
            } else {
                val updateCount: Integer = statement.getUpdateCount()
                Linq4j.singletonEnumerator(updateCount as T)
            }
        } catch (e: SQLException) {
            throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema(sql)
                .ex(e)
        } finally {
            closeIfPossible(connection, statement)
        }
    }

    private fun enumeratorBasedOnPreparedStatement(): Enumerator<T> {
        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        return try {
            connection = dataSource.getConnection()
            preparedStatement = connection.prepareStatement(sql)
            setTimeoutIfPossible(preparedStatement)
            castNonNull(preparedStatementEnricher).enrich(preparedStatement)
            if (preparedStatement.execute()) {
                val resultSet: ResultSet = preparedStatement.getResultSet()
                preparedStatement = null
                connection = null
                ResultSetEnumerator<Any>(resultSet, rowBuilderFactory)
            } else {
                val updateCount: Integer = preparedStatement.getUpdateCount()
                Linq4j.singletonEnumerator(updateCount as T)
            }
        } catch (e: SQLException) {
            throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema(sql)
                .ex(e)
        } finally {
            closeIfPossible(connection, preparedStatement)
        }
    }

    @Throws(SQLException::class)
    private fun setTimeoutIfPossible(statement: Statement?) {
        val queryStart = queryStart
        if (timeout == 0L || queryStart == null) {
            return
        }
        val now: Long = System.currentTimeMillis()
        val secondsLeft = (queryStart + timeout - now) / 1000
        if (secondsLeft <= 0) {
            throw Static.RESOURCE.queryExecutionTimeoutReached(
                String.valueOf(timeout),
                String.valueOf(Instant.ofEpochMilli(queryStart))
            ).ex()
        }
        if (secondsLeft > Integer.MAX_VALUE) {
            // Just ignore the timeout if it happens to be too big, we can't squeeze it into int
            return
        }
        try {
            statement.setQueryTimeout(secondsLeft.toInt())
        } catch (e: SQLFeatureNotSupportedException) {
            if (!timeoutSetFailed && LOGGER.isDebugEnabled()) {
                // We don't really want to print this again and again if enumerable is used multiple times
                LOGGER.debug("Failed to set query timeout $secondsLeft seconds", e)
                timeoutSetFailed = true
            }
        }
    }

    /** Implementation of [Enumerator] that reads from a
     * [ResultSet].
     *
     * @param <T> element type
    </T> */
    private class ResultSetEnumerator<T> internal constructor(
        resultSet: ResultSet?,
        rowBuilderFactory: Function1<ResultSet, Function0<T>?>
    ) : Enumerator<T> {
        private val rowBuilder: Function0<T>

        @Nullable
        private var resultSet: ResultSet?

        init {
            this.resultSet = resultSet
            rowBuilder = rowBuilderFactory.apply(resultSet)
        }

        private fun resultSet(): ResultSet {
            return castNonNull(resultSet)
        }

        @Override
        fun current(): T {
            return rowBuilder.apply()
        }

        @Override
        fun moveNext(): Boolean {
            return try {
                resultSet().next()
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }
        }

        @Override
        fun reset() {
            try {
                resultSet().beforeFirst()
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }
        }

        @Override
        fun close() {
            val savedResultSet: ResultSet? = resultSet
            if (savedResultSet != null) {
                try {
                    resultSet = null
                    val statement: Statement = savedResultSet.getStatement()
                    savedResultSet.close()
                    if (statement != null) {
                        val connection: Connection = statement.getConnection()
                        statement.close()
                        if (connection != null) {
                            connection.close()
                        }
                    }
                } catch (e: SQLException) {
                    // ignore
                }
            }
        }
    }

    /**
     * Consumer for decorating a [PreparedStatement], that is, setting
     * its parameters.
     */
    interface PreparedStatementEnricher {
        @Throws(SQLException::class)
        fun enrich(statement: PreparedStatement?)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(
            ResultSetEnumerable::class.java
        )
        private val AUTO_ROW_BUILDER_FACTORY: Function1<ResultSet, Function0<Object>> =
            label@ Function1<ResultSet, Function0<Object>> { resultSet ->
                val metaData: ResultSetMetaData
                val columnCount: Int
                try {
                    metaData = resultSet.getMetaData()
                    columnCount = metaData.getColumnCount()
                } catch (e: SQLException) {
                    throw RuntimeException(e)
                }
                if (columnCount == 1) {
                    return@label label@{
                        try {
                            return@label resultSet.getObject(1)
                        } catch (e: SQLException) {
                            throw RuntimeException(e)
                        }
                    }
                } else {
                    return@label { convertColumns(resultSet, metaData, columnCount) }
                }
            }

        @Nullable
        private fun convertColumns(
            resultSet: ResultSet, metaData: ResultSetMetaData,
            columnCount: Int
        ): Array<Object?> {
            val list: List<Object> = ArrayList(columnCount)
            return try {
                for (i in 0 until columnCount) {
                    if (metaData.getColumnType(i + 1) === Types.TIMESTAMP) {
                        val v: Long = resultSet.getLong(i + 1)
                        if (v == 0L && resultSet.wasNull()) {
                            list.add(null)
                        } else {
                            list.add(v)
                        }
                    } else {
                        list.add(resultSet.getObject(i + 1))
                    }
                }
                list.toArray()
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }
        }

        /** Creates a ResultSetEnumerable.  */
        fun of(dataSource: DataSource?, sql: String?): ResultSetEnumerable<Object> {
            return of(dataSource, sql, AUTO_ROW_BUILDER_FACTORY)
        }

        /** Creates a ResultSetEnumerable that retrieves columns as specific
         * Java types.  */
        fun of(
            dataSource: DataSource?, sql: String?,
            primitives: Array<Primitive>
        ): ResultSetEnumerable<Object> {
            return of(dataSource, sql, primitiveRowBuilderFactory(primitives))
        }

        /** Executes a SQL query and returns the results as an enumerator, using a
         * row builder to convert JDBC column values into rows.  */
        fun <T> of(
            dataSource: DataSource,
            sql: String,
            rowBuilderFactory: Function1<ResultSet?, Function0<T>>
        ): ResultSetEnumerable<T> {
            return ResultSetEnumerable<Any>(dataSource, sql, rowBuilderFactory)
        }

        /** Executes a SQL query and returns the results as an enumerator, using a
         * row builder to convert JDBC column values into rows.
         *
         *
         * It uses a [PreparedStatement] for computing the query result,
         * and that means that it can bind parameters.  */
        fun <T> of(
            dataSource: DataSource,
            sql: String,
            rowBuilderFactory: Function1<ResultSet?, Function0<T>>,
            consumer: PreparedStatementEnricher?
        ): ResultSetEnumerable<T> {
            return ResultSetEnumerable(dataSource, sql, rowBuilderFactory, consumer)
        }

        /** Called from generated code that proposes to create a
         * `ResultSetEnumerable` over a prepared statement.  */
        fun createEnricher(
            indexes: Array<Integer>,
            context: DataContext
        ): PreparedStatementEnricher {
            return PreparedStatementEnricher { preparedStatement: PreparedStatement ->
                for (i in indexes.indices) {
                    val index: Int = indexes[i]
                    setDynamicParam(
                        preparedStatement, i + 1,
                        context.get("?$index")
                    )
                }
            }
        }

        /** Assigns a value to a dynamic parameter in a prepared statement, calling
         * the appropriate `setXxx` method based on the type of the value.  */
        @Throws(SQLException::class)
        private fun setDynamicParam(
            preparedStatement: PreparedStatement,
            i: Int, @Nullable value: Object?
        ) {
            if (value == null) {
                // TODO: use proper type instead of ANY
                preparedStatement.setObject(i, null, SqlType.ANY.id)
            } else if (value is Timestamp) {
                preparedStatement.setTimestamp(i, value as Timestamp?)
            } else if (value is Time) {
                preparedStatement.setTime(i, value as Time?)
            } else if (value is String) {
                preparedStatement.setString(i, value as String?)
            } else if (value is Integer) {
                preparedStatement.setInt(i, value as Integer?)
            } else if (value is Double) {
                preparedStatement.setDouble(i, value as Double?)
            } else if (value is java.sql.Array) {
                preparedStatement.setArray(i, value as java.sql.Array?)
            } else if (value is BigDecimal) {
                preparedStatement.setBigDecimal(i, value as BigDecimal?)
            } else if (value is Boolean) {
                preparedStatement.setBoolean(i, value as Boolean?)
            } else if (value is Blob) {
                preparedStatement.setBlob(i, value as Blob?)
            } else if (value is Byte) {
                preparedStatement.setByte(i, value as Byte?)
            } else if (value is NClob) {
                preparedStatement.setNClob(i, value as NClob?)
            } else if (value is Clob) {
                preparedStatement.setClob(i, value as Clob?)
            } else if (value is ByteArray) {
                preparedStatement.setBytes(i, value as ByteArray?)
            } else if (value is Date) {
                preparedStatement.setDate(i, value as Date?)
            } else if (value is Float) {
                preparedStatement.setFloat(i, value as Float?)
            } else if (value is Long) {
                preparedStatement.setLong(i, value as Long?)
            } else if (value is Ref) {
                preparedStatement.setRef(i, value as Ref?)
            } else if (value is RowId) {
                preparedStatement.setRowId(i, value as RowId?)
            } else if (value is Short) {
                preparedStatement.setShort(i, value as Short?)
            } else if (value is URL) {
                preparedStatement.setURL(i, value as URL?)
            } else if (value is SQLXML) {
                preparedStatement.setSQLXML(i, value as SQLXML?)
            } else {
                preparedStatement.setObject(i, value)
            }
        }

        private fun closeIfPossible(
            @Nullable connection: Connection?,
            @Nullable statement: Statement?
        ) {
            if (statement != null) {
                try {
                    statement.close()
                } catch (e: SQLException) {
                    // ignore
                }
            }
            if (connection != null) {
                try {
                    connection.close()
                } catch (e: SQLException) {
                    // ignore
                }
            }
        }

        private fun primitiveRowBuilderFactory(primitives: Array<Primitive>): Function1<ResultSet, Function0<Object>> {
            return label@ Function1<ResultSet, Function0<Object>> { resultSet ->
                val metaData: ResultSetMetaData
                val columnCount: Int
                try {
                    metaData = resultSet.getMetaData()
                    columnCount = metaData.getColumnCount()
                } catch (e: SQLException) {
                    throw RuntimeException(e)
                }
                assert(columnCount == primitives.size)
                if (columnCount == 1) {
                    return@label label@{
                        try {
                            return@label resultSet.getObject(1)
                        } catch (e: SQLException) {
                            throw RuntimeException(e)
                        }
                    }
                }
                { convertPrimitiveColumns(primitives, resultSet, columnCount) }
            }
        }

        @Nullable
        private fun convertPrimitiveColumns(
            primitives: Array<Primitive>,
            resultSet: ResultSet, columnCount: Int
        ): Array<Object?> {
            val list: List<Object> = ArrayList(columnCount)
            return try {
                for (i in 0 until columnCount) {
                    list.add(primitives[i].jdbcGet(resultSet, i + 1))
                }
                list.toArray()
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }
        }
    }
}
