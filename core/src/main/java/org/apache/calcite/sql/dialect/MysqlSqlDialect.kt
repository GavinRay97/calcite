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

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.config.NullCollation
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.rel.type.RelDataTypeSystemImpl
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlBasicTypeNameSpec
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlFunctionCategory
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlCase
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.InferTypes
import org.apache.calcite.sql.type.OperandTypes
import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlTypeName

/**
 * A `SqlDialect` implementation for the MySQL database.
 */
class MysqlSqlDialect(context: Context) : SqlDialect(context) {
    private val majorVersion: Int

    /** Creates a MysqlSqlDialect.  */
    init {
        majorVersion = context.databaseMajorVersion()
    }

    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun requiresAliasForFromItems(): Boolean {
        return true
    }

    @Override
    fun supportsAliasedValues(): Boolean {
        // MySQL supports VALUES only in INSERT; not in a FROM clause
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
    ): SqlNode {
        return emulateNullDirectionWithIsNull(node, nullsFirst, desc)
    }

    @Override
    fun supportsAggregateFunction(kind: SqlKind?): Boolean {
        when (kind) {
            COUNT, SUM, SUM0, MIN, MAX, SINGLE_VALUE -> return true
            ROLLUP ->       // MySQL 5 does not support standard "GROUP BY ROLLUP(x, y)",
                // only the non-standard "GROUP BY x, y WITH ROLLUP".
                return majorVersion >= 8
            else -> {}
        }
        return false
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    @Override
    fun supportsGroupByWithRollup(): Boolean {
        return true
    }

    @get:Override
    val calendarPolicy: CalendarPolicy
        get() = CalendarPolicy.SHIFT

    @Override
    @Nullable
    fun getCastSpec(type: RelDataType): SqlNode {
        when (type.getSqlTypeName()) {
            VARCHAR -> {
                // MySQL doesn't have a VARCHAR type, only CHAR.
                val vcMaxPrecision: Int = this.getTypeSystem().getMaxPrecision(SqlTypeName.CHAR)
                var precision: Int = type.getPrecision()
                if (vcMaxPrecision > 0 && precision > vcMaxPrecision) {
                    precision = vcMaxPrecision
                }
                return SqlDataTypeSpec(
                    SqlBasicTypeNameSpec(SqlTypeName.CHAR, precision, SqlParserPos.ZERO),
                    SqlParserPos.ZERO
                )
            }
            INTEGER, BIGINT -> return SqlDataTypeSpec(
                SqlAlienSystemTypeNameSpec(
                    "SIGNED",
                    type.getSqlTypeName(),
                    SqlParserPos.ZERO
                ),
                SqlParserPos.ZERO
            )
            TIMESTAMP -> return SqlDataTypeSpec(
                SqlAlienSystemTypeNameSpec(
                    "DATETIME",
                    type.getSqlTypeName(),
                    SqlParserPos.ZERO
                ),
                SqlParserPos.ZERO
            )
            else -> {}
        }
        return super.getCastSpec(type)
    }

    @Override
    fun rewriteSingleValueExpr(aggCall: SqlNode): SqlNode {
        val operand: SqlNode = (aggCall as SqlBasicCall).operand(0)
        val nullLiteral: SqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO)
        val unionOperand: SqlNode = SqlSelect(
            SqlParserPos.ZERO, SqlNodeList.EMPTY,
            SqlNodeList.of(nullLiteral), null, null, null, null,
            SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY
        )
        // For MySQL, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN <result>
        //   ELSE (SELECT NULL UNION ALL SELECT NULL)
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
                operand
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
                unparseFloor(writer, call)
            }
            else -> super.unparseCall(writer, call, leftPrec, rightPrec)
        }
    }

    @Override
    fun unparseSqlIntervalQualifier(
        writer: SqlWriter,
        qualifier: SqlIntervalQualifier, typeSystem: RelDataTypeSystem?
    ) {

        //  Unit Value         | Expected Format
        // --------------------+-------------------------------------------
        //  MICROSECOND        | MICROSECONDS
        //  SECOND             | SECONDS
        //  MINUTE             | MINUTES
        //  HOUR               | HOURS
        //  DAY                | DAYS
        //  WEEK               | WEEKS
        //  MONTH              | MONTHS
        //  QUARTER            | QUARTERS
        //  YEAR               | YEARS
        //  MINUTE_SECOND      | 'MINUTES:SECONDS'
        //  HOUR_MINUTE        | 'HOURS:MINUTES'
        //  DAY_HOUR           | 'DAYS HOURS'
        //  YEAR_MONTH         | 'YEARS-MONTHS'
        //  MINUTE_MICROSECOND | 'MINUTES:SECONDS.MICROSECONDS'
        //  HOUR_MICROSECOND   | 'HOURS:MINUTES:SECONDS.MICROSECONDS'
        //  SECOND_MICROSECOND | 'SECONDS.MICROSECONDS'
        //  DAY_MINUTE         | 'DAYS HOURS:MINUTES'
        //  DAY_MICROSECOND    | 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
        //  DAY_SECOND         | 'DAYS HOURS:MINUTES:SECONDS'
        //  HOUR_SECOND        | 'HOURS:MINUTES:SECONDS'
        if (!qualifier.useDefaultFractionalSecondPrecision()) {
            throw AssertionError("Fractional second precision is not supported now ")
        }
        val start: String = validate(qualifier.timeUnitRange.startUnit).name()
        if (qualifier.timeUnitRange.startUnit === TimeUnit.SECOND
            || qualifier.timeUnitRange.endUnit == null
        ) {
            writer.keyword(start)
        } else {
            writer.keyword(start + "_" + qualifier.timeUnitRange.endUnit.name())
        }
    }

    @Override
    fun supportsJoinType(joinType: JoinRelType): Boolean {
        return joinType !== JoinRelType.FULL
    }

    companion object {
        /** MySQL type system.  */
        val MYSQL_TYPE_SYSTEM: RelDataTypeSystem = object : RelDataTypeSystemImpl() {
            @Override
            fun getMaxPrecision(typeName: SqlTypeName?): Int {
                return when (typeName) {
                    CHAR -> 255
                    VARCHAR -> 65535
                    TIMESTAMP -> 6
                    else -> super.getMaxPrecision(typeName)
                }
            }
        }
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL)
            .withIdentifierQuoteString("`")
            .withDataTypeSystem(MYSQL_TYPE_SYSTEM)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withNullCollation(NullCollation.LOW)
        val DEFAULT: SqlDialect = MysqlSqlDialect(DEFAULT_CONTEXT)

        /** MySQL specific function.  */
        val ISNULL_FUNCTION: SqlFunction = SqlFunction(
            "ISNULL", SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN, InferTypes.FIRST_KNOWN,
            OperandTypes.ANY, SqlFunctionCategory.SYSTEM
        )

        /**
         * Unparses datetime floor for MySQL. There is no TRUNC function, so simulate
         * this using calls to DATE_FORMAT.
         *
         * @param writer Writer
         * @param call Call
         */
        private fun unparseFloor(writer: SqlWriter, call: SqlCall) {
            val node: SqlLiteral = call.operand(1)
            val unit: TimeUnitRange = node.getValueAs(TimeUnitRange::class.java)
            if (unit === TimeUnitRange.WEEK) {
                writer.print("STR_TO_DATE")
                val frame: SqlWriter.Frame = writer.startList("(", ")")
                writer.print("DATE_FORMAT(")
                call.operand(0).unparse(writer, 0, 0)
                writer.print(", '%x%v-1'), '%x%v-%w'")
                writer.endList(frame)
                return
            }
            val format: String
            format = when (unit) {
                YEAR -> "%Y-01-01"
                MONTH -> "%Y-%m-01"
                DAY -> "%Y-%m-%d"
                HOUR -> "%Y-%m-%d %H:00:00"
                MINUTE -> "%Y-%m-%d %H:%i:00"
                SECOND -> "%Y-%m-%d %H:%i:%s"
                else -> throw AssertionError(
                    "MYSQL does not support FLOOR for time unit: "
                            + unit
                )
            }
            writer.print("DATE_FORMAT")
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            call.operand(0).unparse(writer, 0, 0)
            writer.sep(",", true)
            writer.print("'$format'")
            writer.endList(frame)
        }

        private fun validate(timeUnit: TimeUnit): TimeUnit {
            return when (timeUnit) {
                MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR -> timeUnit
                else -> throw AssertionError(" Time unit " + timeUnit + "is not supported now.")
            }
        }
    }
}
