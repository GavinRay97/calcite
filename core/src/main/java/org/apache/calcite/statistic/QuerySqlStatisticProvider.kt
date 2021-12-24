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
package org.apache.calcite.statistic

import org.apache.calcite.materialize.SqlStatisticProvider
import org.apache.calcite.plan.Contexts
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptSchema
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.ViewExpanders
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.rel.rel2sql.SqlImplementor
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.tools.Frameworks
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util
import com.google.common.cache.CacheBuilder
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.List
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.stream.Collectors
import javax.sql.DataSource
import java.util.Objects.requireNonNull

/**
 * Implementation of [SqlStatisticProvider] that generates and executes
 * SQL queries.
 */
class QuerySqlStatisticProvider(sqlConsumer: Consumer<String?>?) : SqlStatisticProvider {
    private val sqlConsumer: Consumer<String>

    /** Creates a QuerySqlStatisticProvider.
     *
     * @param sqlConsumer Called when each SQL statement is generated
     */
    init {
        this.sqlConsumer = requireNonNull(sqlConsumer, "sqlConsumer")
    }

    @Override
    fun tableCardinality(table: RelOptTable): Double {
        val dialect: SqlDialect = table.unwrapOrThrow(SqlDialect::class.java)
        val dataSource: DataSource = table.unwrapOrThrow(DataSource::class.java)
        return withBuilder(
            BuilderAction<Double> { cluster: RelOptCluster?, relOptSchema: RelOptSchema?, relBuilder: RelBuilder ->
                // Generate:
                //   SELECT COUNT(*) FROM `EMP`
                relBuilder.push(table.toRel(ViewExpanders.simpleContext(cluster)))
                    .aggregate(relBuilder.groupKey(), relBuilder.count())
                val sql = toSql(relBuilder.build(), dialect)
                try {
                    dataSource.getConnection().use { connection ->
                        connection.createStatement().use { statement ->
                            statement.executeQuery(sql).use { resultSet ->
                                if (!resultSet.next()) {
                                    throw AssertionError("expected exactly 1 row: $sql")
                                }
                                val cardinality: Double = resultSet.getDouble(1)
                                if (resultSet.next()) {
                                    throw AssertionError("expected exactly 1 row: $sql")
                                }
                                return@withBuilder cardinality
                            }
                        }
                    }
                } catch (e: SQLException) {
                    throw handle(e, sql)
                }
            })
    }

    @Override
    fun isForeignKey(
        fromTable: RelOptTable, fromColumns: List<Integer?>,
        toTable: RelOptTable, toColumns: List<Integer?>?
    ): Boolean {
        val dialect: SqlDialect = fromTable.unwrapOrThrow(SqlDialect::class.java)
        val dataSource: DataSource = fromTable.unwrapOrThrow(DataSource::class.java)
        return withBuilder(
            BuilderAction<Boolean> { cluster: RelOptCluster?, relOptSchema: RelOptSchema?, relBuilder: RelBuilder ->
                // EMP(DEPTNO) is a foreign key to DEPT(DEPTNO) if the following
                // query returns 0:
                //
                //   SELECT COUNT(*) FROM (
                //     SELECT deptno FROM `EMP` WHERE deptno IS NOT NULL
                //     MINUS
                //     SELECT deptno FROM `DEPT`)
                val toRelContext: RelOptTable.ToRelContext = ViewExpanders.simpleContext(cluster)
                relBuilder.push(fromTable.toRel(toRelContext))
                    .filter(fromColumns.stream()
                        .map { column -> relBuilder.isNotNull(relBuilder.field(column)) }
                        .collect(Collectors.toList()))
                    .project(relBuilder.fields(fromColumns))
                    .push(toTable.toRel(toRelContext))
                    .project(relBuilder.fields(toColumns))
                    .minus(false, 2)
                    .aggregate(relBuilder.groupKey(), relBuilder.count())
                val sql = toSql(relBuilder.build(), dialect)
                try {
                    dataSource.getConnection().use { connection ->
                        connection.createStatement().use { statement ->
                            statement.executeQuery(sql).use { resultSet ->
                                if (!resultSet.next()) {
                                    throw AssertionError("expected exactly 1 row: $sql")
                                }
                                val count: Int = resultSet.getInt(1)
                                if (resultSet.next()) {
                                    throw AssertionError("expected exactly 1 row: $sql")
                                }
                                return@withBuilder count == 0
                            }
                        }
                    }
                } catch (e: SQLException) {
                    throw handle(e, sql)
                }
            })
    }

    @Override
    fun isKey(table: RelOptTable, columns: List<Integer?>?): Boolean {
        val dialect: SqlDialect = table.unwrapOrThrow(SqlDialect::class.java)
        val dataSource: DataSource = table.unwrapOrThrow(DataSource::class.java)
        return withBuilder(
            BuilderAction<Boolean> { cluster: RelOptCluster?, relOptSchema: RelOptSchema?, relBuilder: RelBuilder ->
                // The collection of columns ['DEPTNO'] is a key for 'EMP' if the
                // following query returns no rows:
                //
                //   SELECT 1
                //   FROM `EMP`
                //   GROUP BY `DEPTNO`
                //   HAVING COUNT(*) > 1
                //
                val toRelContext: RelOptTable.ToRelContext = ViewExpanders.simpleContext(cluster)
                relBuilder.push(table.toRel(toRelContext))
                    .aggregate(
                        relBuilder.groupKey(relBuilder.fields(columns)),
                        relBuilder.count()
                    )
                    .filter(
                        relBuilder.call(
                            SqlStdOperatorTable.GREATER_THAN,
                            Util.last(relBuilder.fields()), relBuilder.literal(1)
                        )
                    )
                val sql = toSql(relBuilder.build(), dialect)
                try {
                    dataSource.getConnection().use { connection ->
                        connection.createStatement().use { statement ->
                            statement.executeQuery(sql).use { resultSet -> return@withBuilder !resultSet.next() }
                        }
                    }
                } catch (e: SQLException) {
                    throw handle(e, sql)
                }
            })
    }

    protected fun toSql(rel: RelNode?, dialect: SqlDialect?): String {
        val converter = RelToSqlConverter(dialect)
        val result: SqlImplementor.Result = converter.visitRoot(rel)
        val sqlNode: SqlNode = result.asStatement()
        val sql: String = sqlNode.toSqlString(dialect).getSql()
        sqlConsumer.accept(sql)
        return sql
    }

    /** Performs an action with a [RelBuilder].
     *
     * @param <R> Result type
    </R> */
    @FunctionalInterface
    private interface BuilderAction<R> {
        fun apply(
            cluster: RelOptCluster?, relOptSchema: RelOptSchema?,
            relBuilder: RelBuilder?
        ): R
    }

    companion object {
        /** Instance that uses SQL to compute statistics,
         * does not log SQL statements,
         * and caches up to 1,024 results for up to 30 minutes.
         * (That period should be sufficient for the
         * duration of Calcite's tests, and many other purposes.)  */
        val SILENT_CACHING_INSTANCE: SqlStatisticProvider = CachingSqlStatisticProvider(
            QuerySqlStatisticProvider(Consumer<String> { sql -> }),
            CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES)
                .maximumSize(1024).build()
        )

        /** As [.SILENT_CACHING_INSTANCE] but prints SQL statements to
         * [System.out].  */
        val VERBOSE_CACHING_INSTANCE: SqlStatisticProvider = CachingSqlStatisticProvider(
            QuerySqlStatisticProvider(Consumer<String> { sql -> System.out.println(sql.toString() + ":") }),
            CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES)
                .maximumSize(1024).build()
        )

        private fun handle(e: SQLException, sql: String): RuntimeException {
            return RuntimeException(
                "Error while executing SQL for statistics: "
                        + sql, e
            )
        }

        private fun <R> withBuilder(action: BuilderAction<R>): R {
            return Frameworks.withPlanner { cluster, relOptSchema, rootSchema ->
                val relBuilder: RelBuilder = RelBuilder.proto(Contexts.of()).create(cluster, relOptSchema)
                action.apply(cluster, relOptSchema, relBuilder)
            }
        }
    }
}
