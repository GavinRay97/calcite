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
import org.apache.calcite.config.NullCollation
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlFunctionCategory
import org.apache.calcite.sql.SqlIntervalLiteral
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.ReturnTypes
import java.util.Objects.requireNonNull

/**
 * A `SqlDialect` implementation for the Microsoft SQL Server
 * database.
 */
class MssqlSqlDialect(context: Context) : SqlDialect(context) {
    /** Whether to generate "SELECT TOP(fetch)" rather than
     * "SELECT ... FETCH NEXT fetch ROWS ONLY".  */
    private val top: Boolean

    /** Creates a MssqlSqlDialect.  */
    init {
        // MSSQL 2008 (version 10) and earlier only supports TOP
        // MSSQL 2012 (version 11) and higher supports OFFSET and FETCH
        top = context.databaseMajorVersion() < 11
    }

    /** {@inheritDoc}
     *
     *
     * MSSQL does not support NULLS FIRST, so we emulate using CASE
     * expressions. For example,
     *
     * <blockquote>`ORDER BY x NULLS FIRST`</blockquote>
     *
     *
     * becomes
     *
     * <blockquote>
     * `ORDER BY CASE WHEN x IS NULL THEN 0 ELSE 1 END, x`
    </blockquote> *
     */
    @Override
    @Nullable
    fun emulateNullDirection(
        node: SqlNode,
        nullsFirst: Boolean, desc: Boolean
    ): SqlNode? {
        // Default ordering preserved
        if (nullCollation.isDefaultOrder(nullsFirst, desc)) {
            return null
        }

        // Grouping node should preserve grouping, no emulation needed
        if (node.getKind() === SqlKind.GROUPING) {
            return node
        }

        // Emulate nulls first/last with case ordering
        val pos: SqlParserPos = SqlParserPos.ZERO
        val whenList: SqlNodeList = SqlNodeList.of(SqlStdOperatorTable.IS_NULL.createCall(pos, node))
        val oneLiteral: SqlNode = SqlLiteral.createExactNumeric("1", pos)
        val zeroLiteral: SqlNode = SqlLiteral.createExactNumeric("0", pos)
        return if (nullsFirst) {
            // IS NULL THEN 0 ELSE 1 END
            SqlStdOperatorTable.CASE.createCall(
                null, pos,
                null, whenList, SqlNodeList.of(zeroLiteral), oneLiteral
            )
        } else {
            // IS NULL THEN 1 ELSE 0 END
            SqlStdOperatorTable.CASE.createCall(
                null, pos,
                null, whenList, SqlNodeList.of(oneLiteral), zeroLiteral
            )
        }
    }

    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter?, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode?
    ) {
        if (!top) {
            super.unparseOffsetFetch(writer, offset, fetch)
        }
    }

    @Override
    fun unparseTopN(
        writer: SqlWriter, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode
    ) {
        if (top) {
            // Per Microsoft:
            //   "For backward compatibility, the parentheses are optional in SELECT
            //   statements. We recommend that you always use parentheses for TOP in
            //   SELECT statements. Doing so provides consistency with its required
            //   use in INSERT, UPDATE, MERGE, and DELETE statements."
            //
            // Note that "fetch" is ignored.
            writer.keyword("TOP")
            writer.keyword("(")
            requireNonNull(fetch, "fetch")
            fetch.unparse(writer, -1, -1)
            writer.keyword(")")
        }
    }

    @Override
    fun unparseDateTimeLiteral(
        writer: SqlWriter,
        literal: SqlAbstractDateTimeLiteral, leftPrec: Int, rightPrec: Int
    ) {
        writer.literal("'" + literal.toFormattedString().toString() + "'")
    }

    @Override
    fun unparseCall(
        writer: SqlWriter?, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        if (call.getOperator() === SqlStdOperatorTable.SUBSTRING) {
            if (call.operandCount() !== 3) {
                throw IllegalArgumentException("MSSQL SUBSTRING requires FROM and FOR arguments")
            }
            SqlUtil.unparseFunctionSyntax(MSSQL_SUBSTRING, writer, call, false)
        } else {
            when (call.getKind()) {
                FLOOR -> {
                    if (call.operandCount() !== 2) {
                        super.unparseCall(writer, call, leftPrec, rightPrec)
                        return
                    }
                    unparseFloor(writer, call)
                }
                else -> super.unparseCall(writer, call, leftPrec, rightPrec)
            }
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
    fun unparseSqlDatetimeArithmetic(
        writer: SqlWriter,
        call: SqlCall, sqlKind: SqlKind, leftPrec: Int, rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall("DATEADD")
        val operand: SqlNode = call.operand(1)
        if (operand is SqlIntervalLiteral) {
            //There is no DATESUB method available, so change the sign.
            unparseSqlIntervalLiteralMssql(
                writer, operand as SqlIntervalLiteral, if (sqlKind === SqlKind.MINUS) -1 else 1
            )
        } else {
            operand.unparse(writer, leftPrec, rightPrec)
        }
        writer.sep(",", true)
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.endList(frame)
    }

    @Override
    fun unparseSqlIntervalQualifier(
        writer: SqlWriter,
        qualifier: SqlIntervalQualifier, typeSystem: RelDataTypeSystem?
    ) {
        when (qualifier.timeUnitRange) {
            YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> {
                val timeUnit: String = qualifier.timeUnitRange.startUnit.name()
                writer.keyword(timeUnit)
            }
            else -> throw AssertionError("Unsupported type: " + qualifier.timeUnitRange)
        }
        if (null != qualifier.timeUnitRange.endUnit) {
            throw AssertionError(
                "End unit is not supported now: "
                        + qualifier.timeUnitRange.endUnit
            )
        }
    }

    @Override
    fun unparseSqlIntervalLiteral(
        writer: SqlWriter, literal: SqlIntervalLiteral, leftPrec: Int, rightPrec: Int
    ) {
        unparseSqlIntervalLiteralMssql(writer, literal, 1)
    }

    private fun unparseSqlIntervalLiteralMssql(
        writer: SqlWriter, literal: SqlIntervalLiteral, sign: Int
    ) {
        val interval: SqlIntervalLiteral.IntervalValue =
            literal.getValueAs(SqlIntervalLiteral.IntervalValue::class.java)
        unparseSqlIntervalQualifier(
            writer, interval.getIntervalQualifier(),
            RelDataTypeSystem.DEFAULT
        )
        writer.sep(",", true)
        if (interval.getSign() * sign === -1) {
            writer.print("-")
        }
        writer.literal(interval.getIntervalLiteral())
    }

    companion object {
        val DEFAULT_CONTEXT: Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.MSSQL)
            .withIdentifierQuoteString("[")
            .withCaseSensitive(false)
            .withNullCollation(NullCollation.LOW)
        val DEFAULT: SqlDialect = MssqlSqlDialect(DEFAULT_CONTEXT)
        private val MSSQL_SUBSTRING: SqlFunction = SqlFunction(
            "SUBSTRING", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
            SqlFunctionCategory.STRING
        )

        /**
         * Unparses datetime floor for Microsoft SQL Server.
         * There is no TRUNC function, so simulate this using calls to CONVERT.
         *
         * @param writer Writer
         * @param call Call
         */
        private fun unparseFloor(writer: SqlWriter, call: SqlCall) {
            val node: SqlLiteral = call.operand(1)
            val unit: TimeUnitRange = node.getValueAs(TimeUnitRange::class.java)
            when (unit) {
                YEAR -> unparseFloorWithUnit(writer, call, 4, "-01-01")
                MONTH -> unparseFloorWithUnit(writer, call, 7, "-01")
                WEEK -> {
                    writer.print(
                        "CONVERT(DATETIME, CONVERT(VARCHAR(10), "
                                + "DATEADD(day, - (6 + DATEPART(weekday, "
                    )
                    call.operand(0).unparse(writer, 0, 0)
                    writer.print(")) % 7, ")
                    call.operand(0).unparse(writer, 0, 0)
                    writer.print("), 126))")
                }
                DAY -> unparseFloorWithUnit(writer, call, 10, "")
                HOUR -> unparseFloorWithUnit(writer, call, 13, ":00:00")
                MINUTE -> unparseFloorWithUnit(writer, call, 16, ":00")
                SECOND -> unparseFloorWithUnit(writer, call, 19, ":00")
                else -> throw IllegalArgumentException(
                    "MSSQL does not support FLOOR for time unit: "
                            + unit
                )
            }
        }

        private fun unparseFloorWithUnit(
            writer: SqlWriter, call: SqlCall, charLen: Int,
            offset: String
        ) {
            writer.print("CONVERT")
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            writer.print("DATETIME, CONVERT(VARCHAR($charLen), ")
            call.operand(0).unparse(writer, 0, 0)
            writer.print(", 126)")
            if (offset.length() > 0) {
                writer.print("+'$offset'")
            }
            writer.endList(frame)
        }
    }
}
