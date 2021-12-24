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
package org.apache.calcite.sql.advise

import org.apache.calcite.DataContext

/**
 * Table function that returns completion hints for a given SQL statement.
 */
class SqlAdvisorGetHintsFunction : TableFunction, ImplementableFunction {
    @get:Override
    val implementor: CallImplementor
        get() = IMPLEMENTOR

    @Override
    fun getRowType(
        typeFactory: RelDataTypeFactory,
        arguments: List<Object?>?
    ): RelDataType {
        return typeFactory.createJavaType(SqlAdvisorHint::class.java)
    }

    @Override
    fun getElementType(arguments: List<Object?>?): Type {
        return SqlAdvisorHint::class.java
    }

    @get:Override
    val parameters: List<Any>
        get() = PARAMETERS

    companion object {
        private val ADVISOR: Expression = Expressions.convert_(
            Expressions.call(
                DataContext.ROOT,
                BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant(DataContext.Variable.SQL_ADVISOR.camelName)
            ),
            SqlAdvisor::class.java
        )
        private val GET_COMPLETION_HINTS: Method = Types.lookupMethod(
            SqlAdvisorGetHintsFunction::class.java, "getCompletionHints",
            SqlAdvisor::class.java, String::class.java, Int::class.javaPrimitiveType
        )
        private val IMPLEMENTOR: CallImplementor = RexImpTable.createImplementor(
            { translator, call, operands ->
                Expressions.call(
                    GET_COMPLETION_HINTS,
                    Iterables.concat(Collections.singleton(ADVISOR), operands)
                )
            },
            NullPolicy.ANY, false
        )
        private val PARAMETERS: List<FunctionParameter> = ReflectiveFunctionBase.builder()
            .add(String::class.java, "sql")
            .add(Int::class.javaPrimitiveType, "pos")
            .build()

        /**
         * Returns completion hints for a given SQL statement.
         *
         *
         * Typically this is called from generated code
         * (via [SqlAdvisorGetHintsFunction.IMPLEMENTOR]).
         *
         * @param advisor Advisor to produce completion hints
         * @param sql     SQL to complete
         * @param pos     Cursor position in SQL
         * @return the table that contains completion hints for a given SQL statement
         */
        fun getCompletionHints(
            advisor: SqlAdvisor, sql: String, pos: Int
        ): Enumerable<SqlAdvisorHint> {
            val replaced = arrayOfNulls<String>(1)
            val hints: List<SqlMoniker> = advisor.getCompletionHints(
                sql,
                pos, replaced
            )
            val res: List<SqlAdvisorHint> = ArrayList(hints.size() + 1)
            res.add(SqlAdvisorHint(replaced[0], null, "MATCH"))
            for (hint in hints) {
                res.add(SqlAdvisorHint(hint))
            }
            return Linq4j.asEnumerable(res).asQueryable()
        }
    }
}
