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

import org.apache.calcite.adapter.jdbc.JdbcTable
import org.apache.calcite.materialize.SqlStatisticProvider
import org.apache.calcite.plan.RelOptTable
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableMultimap
import java.util.Arrays
import java.util.List
import java.util.stream.Collectors

/**
 * Implementation of [SqlStatisticProvider] that looks up values in a
 * table.
 *
 *
 * Only for testing.
 */
enum class MapSqlStatisticProvider : SqlStatisticProvider {
    INSTANCE;

    private val cardinalityMap: ImmutableMap<String, Double>
    private val keyMap: ImmutableMultimap<String, ImmutableList<String>>

    init {
        val initializer = Initializer()
            .put("foodmart", "agg_c_14_sales_fact_1997", 86805, "id")
            .put("foodmart", "account", 11, "account_id")
            .put("foodmart", "category", 4, "category_id")
            .put("foodmart", "currency", 10281, "currency_id")
            .put("foodmart", "customer", 10281, "customer_id")
            .put("foodmart", "days", 7, "day")
            .put("foodmart", "employee", 1155, "employee_id")
            .put("foodmart", "employee_closure", 7179)
            .put("foodmart", "department", 10281, "department_id")
            .put("foodmart", "inventory_fact_1997", 4070)
            .put("foodmart", "position", 18, "position_id")
            .put("foodmart", "product", 1560, "product_id")
            .put("foodmart", "product_class", 110, "product_class_id")
            .put(
                "foodmart",
                "promotion",
                1864,
                "promotion_id"
            ) // region really has 110 rows; made it smaller than store to trick FK
            .put("foodmart", "region", 24, "region_id")
            .put("foodmart", "salary", 21252)
            .put("foodmart", "sales_fact_1997", 86837)
            .put("foodmart", "store", 25, "store_id")
            .put("foodmart", "store_ragged", 25, "store_id")
            .put("foodmart", "time_by_day", 730, "time_id", "the_date") // 2 keys
            .put("foodmart", "warehouse", 24, "warehouse_id")
            .put("foodmart", "warehouse_class", 6, "warehouse_class_id")
            .put("scott", "EMP", 10, "EMPNO")
            .put("scott", "DEPT", 4, "DEPTNO")
            .put("tpcds", "CALL_CENTER", 8, "id")
            .put("tpcds", "CATALOG_PAGE", 11718, "id")
            .put("tpcds", "CATALOG_RETURNS", 144067, "id")
            .put("tpcds", "CATALOG_SALES", 1441548, "id")
            .put("tpcds", "CUSTOMER", 100000, "id")
            .put("tpcds", "CUSTOMER_ADDRESS", 50000, "id")
            .put("tpcds", "CUSTOMER_DEMOGRAPHICS", 1920800, "id")
            .put("tpcds", "DATE_DIM", 73049, "id")
            .put("tpcds", "DBGEN_VERSION", 1, "id")
            .put("tpcds", "HOUSEHOLD_DEMOGRAPHICS", 7200, "id")
            .put("tpcds", "INCOME_BAND", 20, "id")
            .put("tpcds", "INVENTORY", 11745000, "id")
            .put("tpcds", "ITEM", 18000, "id")
            .put("tpcds", "PROMOTION", 300, "id")
            .put("tpcds", "REASON", 35, "id")
            .put("tpcds", "SHIP_MODE", 20, "id")
            .put("tpcds", "STORE", 12, "id")
            .put("tpcds", "STORE_RETURNS", 287514, "id")
            .put("tpcds", "STORE_SALES", 2880404, "id")
            .put("tpcds", "TIME_DIM", 86400, "id")
            .put("tpcds", "WAREHOUSE", 5, "id")
            .put("tpcds", "WEB_PAGE", 60, "id")
            .put("tpcds", "WEB_RETURNS", 71763, "id")
            .put("tpcds", "WEB_SALES", 719384, "id")
            .put("tpcds", "WEB_SITE", 1, "id")
        cardinalityMap = initializer.cardinalityMapBuilder.build()
        keyMap = initializer.keyMapBuilder.build()
    }

    @Override
    fun tableCardinality(table: RelOptTable): Double {
        val qualifiedName: List<String> = table.maybeUnwrap(JdbcTable::class.java)
            .map { value -> Arrays.asList(value.jdbcSchemaName, value.jdbcTableName) }
            .orElseGet(table::getQualifiedName)
        return cardinalityMap.get(qualifiedName.toString())
    }

    @Override
    fun isForeignKey(
        fromTable: RelOptTable, fromColumns: List<Integer?>,
        toTable: RelOptTable, toColumns: List<Integer?>
    ): Boolean {
        // Assume that anything that references a primary key is a foreign key.
        // It's wrong but it's enough for our current test cases.
        return (isKey(toTable, toColumns) // supervisor_id contains one 0 value, which does not match any
                // employee_id, therefore it is not a foreign key
                && !"[foodmart, employee].[supervisor_id]"
            .equals(
                fromTable.getQualifiedName() + "."
                        + columnNames(fromTable, fromColumns)
            ))
    }

    @Override
    fun isKey(table: RelOptTable, columns: List<Integer?>): Boolean {
        // In order to match, all column ordinals must be in range 0 .. columnCount
        return (columns.stream().allMatch { columnOrdinal ->
            columnOrdinal >= 0
                    && columnOrdinal < table.getRowType().getFieldCount()
        } // ... and the column names match the name of the primary key
                && keyMap.get(table.getQualifiedName().toString())
            .contains(columnNames(table, columns)))
    }

    /** Helper during construction.  */
    private class Initializer {
        val cardinalityMapBuilder: ImmutableMap.Builder<String, Double> = ImmutableMap.builder()
        val keyMapBuilder: ImmutableMultimap.Builder<String, ImmutableList<String>> = ImmutableMultimap.builder()
        fun put(schema: String?, table: String?, count: Int, vararg keys: Object?): Initializer {
            val qualifiedName: String = Arrays.asList(schema, table).toString()
            cardinalityMapBuilder.put(qualifiedName, count.toDouble())
            for (key in keys) {
                val keyList: ImmutableList<String>
                keyList = if (key is String) {
                    ImmutableList.of(key as String)
                } else if (key is Array<String>) {
                    ImmutableList.copyOf(key as Array<String?>) // composite key
                } else {
                    throw AssertionError("unknown key $key")
                }
                keyMapBuilder.put(qualifiedName, keyList)
            }
            return this
        }
    }

    companion object {
        private fun columnNames(table: RelOptTable, columns: List<Integer?>): List<String> {
            return columns.stream()
                .map { columnOrdinal ->
                    table.getRowType().getFieldNames()
                        .get(columnOrdinal)
                }
                .collect(Collectors.toList())
        }
    }
}
