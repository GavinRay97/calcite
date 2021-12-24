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
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.Util
import com.google.common.cache.Cache
import com.google.common.collect.ImmutableList
import com.google.common.util.concurrent.UncheckedExecutionException
import java.util.List
import java.util.concurrent.ExecutionException

/**
 * Implementation of [SqlStatisticProvider] that reads and writes a
 * cache.
 */
class CachingSqlStatisticProvider(
    provider: SqlStatisticProvider,
    cache: Cache<List?, Object?>
) : SqlStatisticProvider {
    private val provider: SqlStatisticProvider
    private val cache: Cache<List, Object>

    init {
        this.provider = provider
        this.cache = cache
    }

    @Override
    fun tableCardinality(table: RelOptTable): Double {
        return try {
            val key: ImmutableList<Object> = ImmutableList.of(
                "tableCardinality",
                table.getQualifiedName()
            )
            cache.get(
                key
            ) { provider.tableCardinality(table) }
        } catch (e: UncheckedExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        } catch (e: ExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        }
    }

    @Override
    fun isForeignKey(
        fromTable: RelOptTable, fromColumns: List<Integer?>?,
        toTable: RelOptTable, toColumns: List<Integer?>?
    ): Boolean {
        return try {
            val key: ImmutableList<Object> = ImmutableList.of(
                "isForeignKey",
                fromTable.getQualifiedName(),
                ImmutableIntList.copyOf(fromColumns),
                toTable.getQualifiedName(),
                ImmutableIntList.copyOf(toColumns)
            )
            cache.get(
                key
            ) {
                provider.isForeignKey(
                    fromTable, fromColumns, toTable,
                    toColumns
                )
            }
        } catch (e: UncheckedExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        } catch (e: ExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        }
    }

    @Override
    fun isKey(table: RelOptTable, columns: List<Integer?>?): Boolean {
        return try {
            val key: ImmutableList<Object> = ImmutableList.of(
                "isKey", table.getQualifiedName(),
                ImmutableIntList.copyOf(columns)
            )
            cache.get(key) { provider.isKey(table, columns) }
        } catch (e: UncheckedExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        } catch (e: ExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        }
    }
}
