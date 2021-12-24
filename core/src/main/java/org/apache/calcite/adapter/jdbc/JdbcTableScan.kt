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

import org.apache.calcite.plan.Convention

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
class JdbcTableScan protected constructor(
    cluster: RelOptCluster,
    hints: List<RelHint?>?,
    table: RelOptTable?,
    jdbcTable: JdbcTable?,
    jdbcConvention: JdbcConvention?
) : TableScan(cluster, cluster.traitSetOf(jdbcConvention), hints, table), JdbcRel {
    val jdbcTable: JdbcTable

    init {
        this.jdbcTable = Objects.requireNonNull(jdbcTable, "jdbcTable")
    }

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): RelNode {
        assert(inputs.isEmpty())
        return JdbcTableScan(
            getCluster(), getHints(), table, jdbcTable, castNonNull(getConvention()) as JdbcConvention?
        )
    }

    @Override
    fun implement(implementor: JdbcImplementor): JdbcImplementor.Result {
        return implementor.result(
            jdbcTable.tableName(),
            ImmutableList.of(JdbcImplementor.Clause.FROM), this, null
        )
    }

    @Override
    fun withHints(hintList: List<RelHint?>?): RelNode {
        val convention: Convention = requireNonNull(getConvention(), "getConvention()")
        return JdbcTableScan(
            getCluster(), hintList, getTable(), jdbcTable,
            convention as JdbcConvention
        )
    }
}
