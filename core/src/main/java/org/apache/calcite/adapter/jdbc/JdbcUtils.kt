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
package org.apache.calcite.adapter.jdbc

import org.apache.calcite.avatica.ColumnMetaData

/**
 * Utilities for the JDBC provider.
 */
internal class JdbcUtils private constructor() {
    init {
        throw AssertionError("no instances!")
    }

    /** Pool of dialects.  */
    internal class DialectPool {
        private val cache: LoadingCache<Pair<SqlDialectFactory, DataSource>, SqlDialect> =
            CacheBuilder.newBuilder().softValues()
                .build(CacheLoader.from { key: Pair<SqlDialectFactory, DataSource> -> dialect(key) })

        operator fun get(dialectFactory: SqlDialectFactory?, dataSource: DataSource?): SqlDialect {
            val key: Pair<SqlDialectFactory, DataSource> = Pair.of(dialectFactory, dataSource)
            return cache.getUnchecked(key)
        }

        companion object {
            val INSTANCE = DialectPool()
            private fun dialect(
                key: Pair<SqlDialectFactory, DataSource>
            ): SqlDialect {
                val dialectFactory: SqlDialectFactory = key.left
                val dataSource: DataSource = key.right
                var connection: Connection? = null
                return try {
                    connection = dataSource.getConnection()
                    val metaData: DatabaseMetaData = connection.getMetaData()
                    val dialect: SqlDialect = dialectFactory.create(metaData)
                    connection.close()
                    connection = null
                    dialect
                } catch (e: SQLException) {
                    throw RuntimeException(e)
                } finally {
                    if (connection != null) {
                        try {
                            connection.close()
                        } catch (e: SQLException) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    /** Builder that calls [ResultSet.getObject] for every column,
     * or `getXxx` if the result type is a primitive `xxx`,
     * and returns an array of objects for each row.  */
    internal abstract class ObjectArrayRowBuilder(
        resultSet: ResultSet, reps: Array<ColumnMetaData.Rep>,
        types: IntArray
    ) : Function0<Array<Object?>?> {
        protected val resultSet: ResultSet
        protected var columnCount = 0
        protected val reps: Array<ColumnMetaData.Rep>
        protected val types: IntArray

        init {
            this.resultSet = resultSet
            this.reps = reps
            this.types = types
            try {
                columnCount = resultSet.getMetaData().getColumnCount()
            } catch (e: SQLException) {
                throw Util.throwAsRuntime(e)
            }
        }

        @Override
        @Nullable
        fun apply(): Array<Object?> {
            return try {
                @Nullable val values: Array<Object?> = arrayOfNulls<Object>(columnCount)
                for (i in 0 until columnCount) {
                    values[i] = value(i)
                }
                values
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }
        }

        /**
         * Gets a value from a given column in a JDBC result set.
         *
         * @param i Ordinal of column (1-based, per JDBC)
         */
        @Nullable
        @Throws(SQLException::class)
        protected abstract fun value(i: Int): Object?
        fun timestampToLong(v: Timestamp): Long {
            return v.getTime()
        }

        fun timeToLong(v: Time): Long {
            return v.getTime()
        }

        fun dateToLong(v: Date): Long {
            return v.getTime()
        }
    }

    /** Row builder that shifts DATE, TIME, TIMESTAMP values into local time
     * zone.  */
    internal class ObjectArrayRowBuilder1(
        resultSet: ResultSet, reps: Array<ColumnMetaData.Rep>,
        types: IntArray
    ) : ObjectArrayRowBuilder(resultSet, reps, types) {
        val timeZone: TimeZone = TimeZone.getDefault()
        @Override
        @Nullable
        @Throws(SQLException::class)
        override fun value(i: Int): Object? {
            // MySQL returns timestamps shifted into local time. Using
            // getTimestamp(int, Calendar) with a UTC calendar should prevent this,
            // but does not. So we shift explicitly.
            when (types[i]) {
                Types.TIMESTAMP -> {
                    val timestamp: Timestamp = resultSet.getTimestamp(i + 1)
                    return if (timestamp == null) null else Timestamp(timestampToLong(timestamp))
                }
                Types.TIME -> {
                    val time: Time = resultSet.getTime(i + 1)
                    return if (time == null) null else Time(timeToLong(time))
                }
                Types.DATE -> {
                    val date: Date = resultSet.getDate(i + 1)
                    return if (date == null) null else Date(dateToLong(date))
                }
                else -> {}
            }
            return reps[i].jdbcGet(resultSet, i + 1)
        }

        @Override
        override fun timestampToLong(v: Timestamp): Long {
            val time: Long = v.getTime()
            val offset: Int = timeZone.getOffset(time)
            return time + offset
        }

        @Override
        override fun timeToLong(v: Time): Long {
            val time: Long = v.getTime()
            val offset: Int = timeZone.getOffset(time)
            return (time + offset) % DateTimeUtils.MILLIS_PER_DAY
        }

        @Override
        override fun dateToLong(v: Date): Long {
            val time: Long = v.getTime()
            val offset: Int = timeZone.getOffset(time)
            return time + offset
        }
    }

    /** Row builder that converts JDBC values into internal values.  */
    internal class ObjectArrayRowBuilder2(
        resultSet: ResultSet, reps: Array<ColumnMetaData.Rep>,
        types: IntArray
    ) : ObjectArrayRowBuilder1(resultSet, reps, types) {
        @Override
        @Nullable
        @Throws(SQLException::class)
        override fun value(i: Int): Object? {
            return when (types[i]) {
                Types.TIMESTAMP -> {
                    val timestamp: Timestamp = resultSet.getTimestamp(i + 1)
                    if (timestamp == null) null else timestampToLong(timestamp)
                }
                Types.TIME -> {
                    val time: Time = resultSet.getTime(i + 1)
                    if (time == null) null else timeToLong(time).toInt()
                }
                Types.DATE -> {
                    val date: Date = resultSet.getDate(i + 1)
                    if (date == null) null else (dateToLong(date) / DateTimeUtils.MILLIS_PER_DAY)
                }
                else -> reps[i].jdbcGet(resultSet, i + 1)
            }
        }
    }

    /** Ensures that if two data sources have the same definition, they will use
     * the same object.
     *
     *
     * This in turn makes it easier to cache
     * [org.apache.calcite.sql.SqlDialect] objects. Otherwise, each time we
     * see a new data source, we have to open a connection to find out what
     * database product and version it is.  */
    internal class DataSourcePool {
        private val cache: LoadingCache<List<String>, BasicDataSource> = CacheBuilder.newBuilder().softValues()
            .build(CacheLoader.from { key: List<String?> -> dataSource(key) })

        operator fun get(
            url: String?, @Nullable driverClassName: String?,
            @Nullable username: String?, @Nullable password: String?
        ): DataSource {
            // Get data source objects from a cache, so that we don't have to sniff
            // out what kind of database they are quite as often.
            val key: List<String> = ImmutableNullableList.of(url, username, password, driverClassName)
            return cache.getUnchecked(key)
        }

        companion object {
            val INSTANCE = DataSourcePool()
            private fun dataSource(
                key: List<String?>
            ): BasicDataSource {
                val dataSource = BasicDataSource()
                dataSource.setUrl(key[0])
                dataSource.setUsername(key[1])
                dataSource.setPassword(key[2])
                dataSource.setDriverClassName(key[3])
                return dataSource
            }
        }
    }

    companion object {
        /** Returns a function that, given a [ResultSet], returns a function
         * that will yield successive rows from that result set.  */
        fun rowBuilderFactory(
            list: List<Pair<ColumnMetaData.Rep?, Integer?>?>?
        ): Function1<ResultSet, Function0<Array<Object>>> {
            val reps: Array<ColumnMetaData.Rep> = Pair.left(list).toArray(arrayOfNulls<ColumnMetaData.Rep>(0))
            val types: IntArray = Ints.toArray(Pair.right(list))
            return Function1<ResultSet, Function0<Array<Object>>> { resultSet ->
                ObjectArrayRowBuilder1(
                    resultSet,
                    reps,
                    types
                )
            }
        }

        /** Returns a function that, given a [ResultSet], returns a function
         * that will yield successive rows from that result set;
         * as [.rowBuilderFactory] except that values are in Calcite's
         * internal format (e.g. DATE represented as int).  */
        fun rowBuilderFactory2(
            list: List<Pair<ColumnMetaData.Rep?, Integer?>?>?
        ): Function1<ResultSet, Function0<Array<Object>>> {
            val reps: Array<ColumnMetaData.Rep> = Pair.left(list).toArray(arrayOfNulls<ColumnMetaData.Rep>(0))
            val types: IntArray = Ints.toArray(Pair.right(list))
            return Function1<ResultSet, Function0<Array<Object>>> { resultSet ->
                ObjectArrayRowBuilder2(
                    resultSet,
                    reps,
                    types
                )
            }
        }
    }
}
