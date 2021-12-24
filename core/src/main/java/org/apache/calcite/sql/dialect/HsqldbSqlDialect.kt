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

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlCase
import org.apache.calcite.sql.`fun`.SqlFloorFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos

/**
 * A `SqlDialect` implementation for the Hsqldb database.
 */
class HsqldbSqlDialect
/** Creates an HsqldbSqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun supportsAggregateFunctionFilter(): Boolean {
        return false
    }

    @Override
    fun supportsWindowFunctions(): Boolean {
        return false
    }

    @Override
    fun unparseCall(
        writer: SqlWriter?, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        when (call.getKind()) {
            FLOOR -> {
                if (call.operandCount() !== 2) {
                    super.unparseCall(writer, call, leftPrec, rightPrec)
                    return
                }
                val timeUnitNode: SqlLiteral = call.operand(1)
                val timeUnit: TimeUnitRange = timeUnitNode.getValueAs(TimeUnitRange::class.java)
                val translatedLit = convertTimeUnit(timeUnit)
                val call2: SqlCall = SqlFloorFunction.replaceTimeUnitOperand(
                    call, translatedLit,
                    timeUnitNode.getParserPosition()
                )
                SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true)
            }
            else -> super.unparseCall(writer, call, leftPrec, rightPrec)
        }
    }

    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter?, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode?
    ) {
        unparseFetchUsingLimit(writer, offset, fetch)
    }

    @Override
    fun rewriteSingleValueExpr(aggCall: SqlNode): SqlNode {
        val operand: SqlNode = (aggCall as SqlBasicCall).operand(0)
        val nullLiteral: SqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO)
        val unionOperand: SqlNode = SqlStdOperatorTable.VALUES.createCall(
            SqlParserPos.ZERO,
            SqlLiteral.createApproxNumeric("0", SqlParserPos.ZERO)
        )
        // For hsqldb, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN MIN(<result>)
        //   ELSE (VALUES 1 UNION ALL VALUES 1)
        //   END
        val caseExpr: SqlNode = SqlCase(
            SqlParserPos.ZERO,
            SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, operand),
            SqlNodeList.of(
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
            ),
            SqlNodeList.of(
                nullLiteral,
                SqlStdOperatorTable.MIN.createCall(SqlParserPos.ZERO, operand)
            ),
            SqlStdOperatorTable.SCALAR_QUERY.createCall(
                SqlParserPos.ZERO,
                SqlStdOperatorTable.UNION_ALL
                    .createCall(SqlParserPos.ZERO, unionOperand, unionOperand)
            )
        )
        LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr)
        return caseExpr
    }

    companion object {
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.HSQLDB)
        val DEFAULT: SqlDialect = HsqldbSqlDialect(DEFAULT_CONTEXT)
        private fun convertTimeUnit(unit: TimeUnitRange): String {
            return when (unit) {
                YEAR -> "YYYY"
                MONTH -> "MM"
                DAY -> "DD"
                WEEK -> "WW"
                HOUR -> "HH24"
                MINUTE -> "MI"
                SECOND -> "SS"
                else -> throw AssertionError(
                    "could not convert time unit to HSQLDB equivalent: "
                            + unit
                )
            }
        }
    }
}
