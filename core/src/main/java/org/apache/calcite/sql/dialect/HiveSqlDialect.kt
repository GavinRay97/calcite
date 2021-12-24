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
package org.apache.calcite.sql.dialect

import org.apache.calcite.config.NullCollation
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSyntax
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.`fun`.SqlSubstringFunction
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.BasicSqlType
import org.apache.calcite.util.RelToSqlConverterUtil

/**
 * A `SqlDialect` implementation for the Apache Hive database.
 */
class HiveSqlDialect(context: Context) : SqlDialect(context) {
    private val emulateNullDirection: Boolean

    /** Creates a HiveSqlDialect.  */
    init {
        // Since 2.1.0, Hive natively supports "NULLS FIRST" and "NULLS LAST".
        // See https://issues.apache.org/jira/browse/HIVE-12994.
        emulateNullDirection = (context.databaseMajorVersion() < 2
                || (context.databaseMajorVersion() === 2
                && context.databaseMinorVersion() < 1))
    }

    @Override
    protected fun allowsAs(): Boolean {
        return false
    }

    @Override
    fun supportsAliasedValues(): Boolean {
        return false
    }

    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter?, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode?
    ) {
        unparseFetchUsingLimit(writer, offset, fetch)
    }

    @Override
    @Nullable
    fun emulateNullDirection(
        node: SqlNode?,
        nullsFirst: Boolean, desc: Boolean
    ): SqlNode? {
        return if (emulateNullDirection) {
            emulateNullDirectionWithIsNull(node, nullsFirst, desc)
        } else null
    }

    @Override
    fun unparseCall(
        writer: SqlWriter, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        when (call.getKind()) {
            POSITION -> {
                val frame: SqlWriter.Frame = writer.startFunCall("INSTR")
                writer.sep(",")
                call.operand(1).unparse(writer, leftPrec, rightPrec)
                writer.sep(",")
                call.operand(0).unparse(writer, leftPrec, rightPrec)
                if (3 == call.operandCount()) {
                    throw RuntimeException("3rd operand Not Supported for Function INSTR in Hive")
                }
                writer.endFunCall(frame)
            }
            MOD -> {
                val op: SqlOperator = SqlStdOperatorTable.PERCENT_REMAINDER
                SqlSyntax.BINARY.unparse(writer, op, call, leftPrec, rightPrec)
            }
            TRIM -> RelToSqlConverterUtil.unparseHiveTrim(writer, call, leftPrec, rightPrec)
            OTHER_FUNCTION -> if (call.getOperator() is SqlSubstringFunction) {
                val funCallFrame: SqlWriter.Frame = writer.startFunCall(call.getOperator().getName())
                call.operand(0).unparse(writer, leftPrec, rightPrec)
                writer.sep(",", true)
                call.operand(1).unparse(writer, leftPrec, rightPrec)
                if (3 == call.operandCount()) {
                    writer.sep(",", true)
                    call.operand(2).unparse(writer, leftPrec, rightPrec)
                }
                writer.endFunCall(funCallFrame)
            } else {
                super.unparseCall(writer, call, leftPrec, rightPrec)
            }
            else -> super.unparseCall(writer, call, leftPrec, rightPrec)
        }
    }

    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun supportsGroupByWithRollup(): Boolean {
        return true
    }

    @Override
    fun supportsGroupByWithCube(): Boolean {
        return true
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    @Override
    @Nullable
    fun getCastSpec(type: RelDataType): SqlNode {
        if (type is BasicSqlType) {
            when (type.getSqlTypeName()) {
                INTEGER -> {
                    val typeNameSpec = SqlAlienSystemTypeNameSpec(
                        "INT", type.getSqlTypeName(), SqlParserPos.ZERO
                    )
                    return SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO)
                }
                else -> {}
            }
        }
        return super.getCastSpec(type)
    }

    companion object {
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
            .withNullCollation(NullCollation.LOW)
        val DEFAULT: SqlDialect = HiveSqlDialect(DEFAULT_CONTEXT)
    }
}
