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
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral
import org.apache.calcite.sql.SqlBasicTypeNameSpec
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDateLiteral
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlTimeLiteral
import org.apache.calcite.sql.SqlTimestampLiteral
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.BasicSqlType
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.RelToSqlConverterUtil
import java.util.Objects.requireNonNull

/**
 * A `SqlDialect` implementation for the ClickHouse database.
 */
class ClickHouseSqlDialect
/** Creates a ClickHouseSqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    @Override
    fun supportsWindowFunctions(): Boolean {
        return false
    }

    @Override
    fun supportsAliasedValues(): Boolean {
        return false
    }

    @get:Override
    val calendarPolicy: CalendarPolicy
        get() = CalendarPolicy.SHIFT

    @Override
    @Nullable
    fun getCastSpec(type: RelDataType): SqlNode {
        if (type is BasicSqlType) {
            val typeName: SqlTypeName = type.getSqlTypeName()
            when (typeName) {
                VARCHAR -> return createSqlDataTypeSpecByName("String", typeName)
                TINYINT -> return createSqlDataTypeSpecByName("Int8", typeName)
                SMALLINT -> return createSqlDataTypeSpecByName("Int16", typeName)
                INTEGER -> return createSqlDataTypeSpecByName("Int32", typeName)
                BIGINT -> return createSqlDataTypeSpecByName("Int64", typeName)
                FLOAT -> return createSqlDataTypeSpecByName("Float32", typeName)
                DOUBLE -> return createSqlDataTypeSpecByName("Float64", typeName)
                DATE -> return createSqlDataTypeSpecByName("Date", typeName)
                TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> return createSqlDataTypeSpecByName("DateTime", typeName)
                else -> {}
            }
        }
        return super.getCastSpec(type)
    }

    @Override
    fun unparseDateTimeLiteral(
        writer: SqlWriter,
        literal: SqlAbstractDateTimeLiteral, leftPrec: Int, rightPrec: Int
    ) {
        val toFunc: String
        toFunc = if (literal is SqlDateLiteral) {
            "toDate"
        } else if (literal is SqlTimestampLiteral) {
            "toDateTime"
        } else if (literal is SqlTimeLiteral) {
            "toTime"
        } else {
            throw RuntimeException(
                "ClickHouse does not support DateTime literal: "
                        + literal
            )
        }
        writer.literal(toFunc + "('" + literal.toFormattedString() + "')")
    }

    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode
    ) {
        requireNonNull(fetch, "fetch")
        writer.newlineAndIndent()
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FETCH)
        writer.keyword("LIMIT")
        if (offset != null) {
            offset.unparse(writer, -1, -1)
            writer.sep(",", true)
        }
        fetch.unparse(writer, -1, -1)
        writer.endList(frame)
    }

    @Override
    fun unparseCall(
        writer: SqlWriter, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        if (call.getOperator() === SqlStdOperatorTable.SUBSTRING) {
            RelToSqlConverterUtil.specialOperatorByName("substring")
                .unparse(writer, call, 0, 0)
        } else {
            when (call.getKind()) {
                FLOOR -> {
                    if (call.operandCount() !== 2) {
                        super.unparseCall(writer, call, leftPrec, rightPrec)
                        return
                    }
                    unparseFloor(writer, call)
                }
                COUNT ->         // CH returns NULL rather than 0 for COUNT(DISTINCT) of NULL values.
                    // https://github.com/yandex/ClickHouse/issues/2494
                    // Wrap the call in a CH specific coalesce (assumeNotNull).
                    if (call.getFunctionQuantifier() != null
                        && call.getFunctionQuantifier().toString().equals("DISTINCT")
                    ) {
                        writer.print("assumeNotNull")
                        val frame: SqlWriter.Frame = writer.startList("(", ")")
                        super.unparseCall(writer, call, leftPrec, rightPrec)
                        writer.endList(frame)
                    } else {
                        super.unparseCall(writer, call, leftPrec, rightPrec)
                    }
                else -> super.unparseCall(writer, call, leftPrec, rightPrec)
            }
        }
    }

    companion object {
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.CLICKHOUSE)
            .withIdentifierQuoteString("`")
            .withNullCollation(NullCollation.LOW)
        val DEFAULT: SqlDialect = ClickHouseSqlDialect(DEFAULT_CONTEXT)
        private fun createSqlDataTypeSpecByName(
            typeAlias: String,
            typeName: SqlTypeName
        ): SqlDataTypeSpec {
            val spec: SqlBasicTypeNameSpec = object : SqlBasicTypeNameSpec(typeName, SqlParserPos.ZERO) {
                @Override
                fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
                    // unparse as an identifier to ensure that type names are cased correctly
                    writer.identifier(typeAlias, true)
                }
            }
            return SqlDataTypeSpec(spec, SqlParserPos.ZERO)
        }

        /**
         * Unparses datetime floor for ClickHouse.
         *
         * @param writer Writer
         * @param call Call
         */
        private fun unparseFloor(writer: SqlWriter, call: SqlCall) {
            val timeUnitNode: SqlLiteral = call.operand(1)
            val unit: TimeUnitRange = timeUnitNode.getValueAs(TimeUnitRange::class.java)
            val funName: String
            funName = when (unit) {
                YEAR -> "toStartOfYear"
                MONTH -> "toStartOfMonth"
                WEEK -> "toMonday"
                DAY -> "toDate"
                HOUR -> "toStartOfHour"
                MINUTE -> "toStartOfMinute"
                else -> throw RuntimeException(
                    "ClickHouse does not support FLOOR for time unit: "
                            + unit
                )
            }
            writer.print(funName)
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            call.operand(0).unparse(writer, 0, 0)
            writer.endList(frame)
        }
    }
}
