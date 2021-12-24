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
package org.apache.calcite.sql.ddl

import org.apache.calcite.schema.ColumnStrategy
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import com.google.common.collect.ImmutableList
import java.util.List

/**
 * Parse tree for `UNIQUE`, `PRIMARY KEY` constraints.
 *
 *
 * And `FOREIGN KEY`, when we support it.
 */
class SqlColumnDeclaration internal constructor(
    pos: SqlParserPos?, name: SqlIdentifier,
    dataType: SqlDataTypeSpec, @Nullable expression: SqlNode?,
    strategy: ColumnStrategy
) : SqlCall(pos) {
    val name: SqlIdentifier
    val dataType: SqlDataTypeSpec

    @Nullable
    val expression: SqlNode?
    val strategy: ColumnStrategy

    /** Creates a SqlColumnDeclaration; use [SqlDdlNodes.column].  */
    init {
        this.name = name
        this.dataType = dataType
        this.expression = expression
        this.strategy = strategy
    }

    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    val operandList: List<Any>
        get() = ImmutableList.of(name, dataType)

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        name.unparse(writer, 0, 0)
        dataType.unparse(writer, 0, 0)
        if (Boolean.FALSE.equals(dataType.getNullable())) {
            writer.keyword("NOT NULL")
        }
        val expression: SqlNode? = expression
        if (expression != null) {
            when (strategy) {
                VIRTUAL, STORED -> {
                    writer.keyword("AS")
                    exp(writer, expression)
                    writer.keyword(strategy.name())
                }
                DEFAULT -> {
                    writer.keyword("DEFAULT")
                    exp(writer, expression)
                }
                else -> throw AssertionError("unexpected: $strategy")
            }
        }
    }

    companion object {
        private val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL)
        fun exp(writer: SqlWriter, expression: SqlNode) {
            if (writer.isAlwaysUseParentheses()) {
                expression.unparse(writer, 0, 0)
            } else {
                writer.sep("(")
                expression.unparse(writer, 0, 0)
                writer.sep(")")
            }
        }
    }
}
