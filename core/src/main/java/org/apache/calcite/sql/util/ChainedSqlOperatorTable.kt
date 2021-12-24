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
package org.apache.calcite.sql.util

import org.apache.calcite.sql.SqlFunctionCategory

/**
 * ChainedSqlOperatorTable implements the [SqlOperatorTable] interface by
 * chaining together any number of underlying operator table instances.
 *
 *
 * To create, call [SqlOperatorTables.chain].
 */
class ChainedSqlOperatorTable protected constructor(tableList: ImmutableList<SqlOperatorTable?>?) : SqlOperatorTable {
    //~ Instance fields --------------------------------------------------------
    protected val tableList: List<SqlOperatorTable>

    //~ Constructors -----------------------------------------------------------
    @Deprecated // to be removed before 2.0
    constructor(tableList: List<SqlOperatorTable?>?) : this(ImmutableList.copyOf(tableList)) {
    }

    /** Internal constructor; call [SqlOperatorTables.chain].  */
    init {
        this.tableList = ImmutableList.copyOf(tableList)
    }

    //~ Methods ----------------------------------------------------------------
    @Deprecated // to be removed before 2.0
    fun add(table: SqlOperatorTable) {
        if (!tableList.contains(table)) {
            tableList.add(table)
        }
    }

    @Override
    fun lookupOperatorOverloads(
        opName: SqlIdentifier?,
        @Nullable category: SqlFunctionCategory?, syntax: SqlSyntax?,
        operatorList: List<SqlOperator?>?, nameMatcher: SqlNameMatcher?
    ) {
        for (table in tableList) {
            table.lookupOperatorOverloads(
                opName, category, syntax, operatorList,
                nameMatcher
            )
        }
    }

    @get:Override
    val operatorList: List<Any>
        get() {
            val list: List<SqlOperator> = ArrayList()
            for (table in tableList) {
                list.addAll(table.getOperatorList())
            }
            return list
        }
}
