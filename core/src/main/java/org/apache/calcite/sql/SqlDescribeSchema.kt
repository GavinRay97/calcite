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
package org.apache.calcite.sql

import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ImmutableNullableList
import java.util.List

/**
 * A `SqlDescribeSchema` is a node of a parse tree that represents a
 * `DESCRIBE SCHEMA` statement.
 */
class SqlDescribeSchema(pos: SqlParserPos?, schema: SqlIdentifier) : SqlCall(pos) {
    var schema: SqlIdentifier

    /** Creates a SqlDescribeSchema.  */
    init {
        this.schema = schema
    }

    @Override
    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("DESCRIBE")
        writer.keyword("SCHEMA")
        schema.unparse(writer, leftPrec, rightPrec)
    }

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    override fun setOperand(i: Int, @Nullable operand: SqlNode) {
        schema = when (i) {
            0 -> operand as SqlIdentifier
            else -> throw AssertionError(i)
        }
    }

    override val operator: SqlOperator
        @Override get() = OPERATOR
    override val operandList: List<Any>
        @Override get() = ImmutableNullableList.of(schema)

    fun getSchema(): SqlIdentifier {
        return schema
    }

    companion object {
        val OPERATOR: SqlSpecialOperator = object : SqlSpecialOperator("DESCRIBE_SCHEMA", SqlKind.DESCRIBE_SCHEMA) {
            @SuppressWarnings("argument.type.incompatible")
            @Override
            fun createCall(
                @Nullable functionQualifier: SqlLiteral?,
                pos: SqlParserPos?, @Nullable vararg operands: SqlNode
            ): SqlCall {
                return SqlDescribeSchema(pos, operands[0] as SqlIdentifier)
            }
        }
    }
}
