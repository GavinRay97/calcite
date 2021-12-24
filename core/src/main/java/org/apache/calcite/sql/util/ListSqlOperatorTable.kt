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

import org.apache.calcite.sql.SqlFunction

/**
 * Implementation of the [SqlOperatorTable] interface by using a list of
 * [operators][SqlOperator].
 */
class ListSqlOperatorTable @JvmOverloads constructor(operatorList: List<SqlOperator> = ArrayList()) : SqlOperatorTable {
    //~ Instance fields --------------------------------------------------------
    private val operatorList: List<SqlOperator>

    //~ Constructors -----------------------------------------------------------
    init {
        this.operatorList = operatorList
    }

    //~ Methods ----------------------------------------------------------------
    fun add(op: SqlOperator?) {
        operatorList.add(op)
    }

    @Override
    fun lookupOperatorOverloads(
        opName: SqlIdentifier,
        @Nullable category: SqlFunctionCategory?,
        syntax: SqlSyntax,
        operatorList: List<SqlOperator?>,
        nameMatcher: SqlNameMatcher
    ) {
        for (operator in this.operatorList) {
            if (operator.getSyntax().family !== syntax) {
                continue
            }
            if (!opName.isSimple()
                || !nameMatcher.matches(operator.getName(), opName.getSimple())
            ) {
                continue
            }
            if (category != null && category !== category(operator) && !category.isUserDefinedNotSpecificFunction()) {
                continue
            }
            operatorList.add(operator)
        }
    }

    @Override
    fun getOperatorList(): List<SqlOperator> {
        return operatorList
    }

    companion object {
        protected fun category(operator: SqlOperator): SqlFunctionCategory {
            return if (operator is SqlFunction) {
                (operator as SqlFunction).getFunctionType()
            } else {
                SqlFunctionCategory.SYSTEM
            }
        }
    }
}
