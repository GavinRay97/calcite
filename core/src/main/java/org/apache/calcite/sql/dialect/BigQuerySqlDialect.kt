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
import org.apache.calcite.config.Lex
import org.apache.calcite.config.NullCollation
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlIntervalLiteral
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSetOperator
import org.apache.calcite.sql.SqlSyntax
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlTrimFunction
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.BasicSqlType
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import com.google.common.collect.ImmutableList
import java.util.Arrays
import java.util.List
import java.util.Locale
import java.util.regex.Pattern
import java.util.Objects.requireNonNull

/**
 * A `SqlDialect` implementation for Google BigQuery's "Standard SQL"
 * dialect.
 */
class BigQuerySqlDialect
/** Creates a BigQuerySqlDialect.  */
    (context: SqlDialect.Context?) : SqlDialect(context) {
    @Override
    protected fun identifierNeedsQuote(`val`: String): Boolean {
        return (!IDENTIFIER_REGEX.matcher(`val`).matches()
                || RESERVED_KEYWORDS.contains(`val`.toUpperCase(Locale.ROOT)))
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
    fun supportsImplicitTypeCoercion(call: RexCall): Boolean {
        return (super.supportsImplicitTypeCoercion(call)
                && RexUtil.isLiteral(call.getOperands().get(0), false)
                && !SqlTypeUtil.isNumeric(call.type))
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    @Override
    fun supportsAggregateFunctionFilter(): Boolean {
        return false
    }

    @Override
    fun configureParser(
        configBuilder: SqlParser.Config?
    ): SqlParser.Config {
        return super.configureParser(configBuilder)
            .withCharLiteralStyles(Lex.BIG_QUERY.charLiteralStyles)
    }

    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter?, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode?
    ) {
        unparseFetchUsingLimit(writer, offset, fetch)
    }

    @Override
    fun supportsAliasedValues(): Boolean {
        return false
    }

    @Override
    fun unparseCall(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        when (call.getKind()) {
            POSITION -> {
                val frame: SqlWriter.Frame = writer.startFunCall("STRPOS")
                writer.sep(",")
                call.operand(1).unparse(writer, leftPrec, rightPrec)
                writer.sep(",")
                call.operand(0).unparse(writer, leftPrec, rightPrec)
                if (3 == call.operandCount()) {
                    throw RuntimeException("3rd operand Not Supported for Function STRPOS in Big Query")
                }
                writer.endFunCall(frame)
            }
            UNION -> if ((call.getOperator() as SqlSetOperator).isAll()) {
                super.unparseCall(writer, call, leftPrec, rightPrec)
            } else {
                SqlSyntax.BINARY.unparse(
                    writer, UNION_DISTINCT, call, leftPrec,
                    rightPrec
                )
            }
            EXCEPT -> {
                if ((call.getOperator() as SqlSetOperator).isAll()) {
                    throw RuntimeException("BigQuery does not support EXCEPT ALL")
                }
                SqlSyntax.BINARY.unparse(
                    writer, EXCEPT_DISTINCT, call, leftPrec,
                    rightPrec
                )
            }
            INTERSECT -> {
                if ((call.getOperator() as SqlSetOperator).isAll()) {
                    throw RuntimeException("BigQuery does not support INTERSECT ALL")
                }
                SqlSyntax.BINARY.unparse(
                    writer, INTERSECT_DISTINCT, call, leftPrec,
                    rightPrec
                )
            }
            TRIM -> unparseTrim(writer, call, leftPrec, rightPrec)
            else -> super.unparseCall(writer, call, leftPrec, rightPrec)
        }
    }

    /** BigQuery interval syntax: INTERVAL int64 time_unit.  */
    @Override
    fun unparseSqlIntervalLiteral(
        writer: SqlWriter,
        literal: SqlIntervalLiteral, leftPrec: Int, rightPrec: Int
    ) {
        val interval: SqlIntervalLiteral.IntervalValue =
            literal.getValueAs(SqlIntervalLiteral.IntervalValue::class.java)
        writer.keyword("INTERVAL")
        if (interval.getSign() === -1) {
            writer.print("-")
        }
        try {
            Long.parseLong(interval.getIntervalLiteral())
        } catch (e: NumberFormatException) {
            throw RuntimeException("Only INT64 is supported as the interval value for BigQuery.")
        }
        writer.literal(interval.getIntervalLiteral())
        unparseSqlIntervalQualifier(
            writer, interval.getIntervalQualifier(),
            RelDataTypeSystem.DEFAULT
        )
    }

    @Override
    fun unparseSqlIntervalQualifier(
        writer: SqlWriter, qualifier: SqlIntervalQualifier, typeSystem: RelDataTypeSystem?
    ) {
        val start: String = validate(qualifier.timeUnitRange.startUnit).name()
        if (qualifier.timeUnitRange.endUnit == null) {
            writer.keyword(start)
        } else {
            throw RuntimeException("Range time unit is not supported for BigQuery.")
        }
    }

    /** {@inheritDoc}
     *
     *
     * BigQuery data type reference:
     * [
 * BigQuery Standard SQL Data Types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
     */
    @Override
    @Nullable
    fun getCastSpec(type: RelDataType): SqlNode {
        if (type is BasicSqlType) {
            val typeName: SqlTypeName = type.getSqlTypeName()
            when (typeName) {
                TINYINT, SMALLINT, INTEGER, BIGINT -> return createSqlDataTypeSpecByName("INT64", typeName)
                FLOAT, DOUBLE -> return createSqlDataTypeSpecByName("FLOAT64", typeName)
                DECIMAL -> return createSqlDataTypeSpecByName("NUMERIC", typeName)
                BOOLEAN -> return createSqlDataTypeSpecByName("BOOL", typeName)
                CHAR, VARCHAR -> return createSqlDataTypeSpecByName("STRING", typeName)
                BINARY, VARBINARY -> return createSqlDataTypeSpecByName("BYTES", typeName)
                DATE -> return createSqlDataTypeSpecByName("DATE", typeName)
                TIME -> return createSqlDataTypeSpecByName("TIME", typeName)
                TIMESTAMP -> return createSqlDataTypeSpecByName("TIMESTAMP", typeName)
                else -> {}
            }
        }
        return super.getCastSpec(type)
    }

    companion object {
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.BIG_QUERY)
            .withLiteralQuoteString("'")
            .withLiteralEscapedQuoteString("\\'")
            .withIdentifierQuoteString("`")
            .withIdentifierEscapedQuoteString("\\`")
            .withNullCollation(NullCollation.LOW)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED)
            .withCaseSensitive(false)
        val DEFAULT: SqlDialect = BigQuerySqlDialect(DEFAULT_CONTEXT)
        private val RESERVED_KEYWORDS: List<String> = ImmutableList.copyOf(
            Arrays.asList(
                "ALL", "AND", "ANY", "ARRAY", "AS", "ASC",
                "ASSERT_ROWS_MODIFIED", "AT", "BETWEEN", "BY", "CASE", "CAST",
                "COLLATE", "CONTAINS", "CREATE", "CROSS", "CUBE", "CURRENT",
                "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END", "ENUM",
                "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE",
                "FETCH", "FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPING",
                "GROUPS", "HASH", "HAVING", "IF", "IGNORE", "IN", "INNER",
                "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT",
                "LIKE", "LIMIT", "LOOKUP", "MERGE", "NATURAL", "NEW", "NO",
                "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER",
                "OVER", "PARTITION", "PRECEDING", "PROTO", "RANGE", "RECURSIVE",
                "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME",
                "STRUCT", "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE",
                "UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN", "WHERE",
                "WINDOW", "WITH", "WITHIN"
            )
        )

        /** An unquoted BigQuery identifier must start with a letter and be followed
         * by zero or more letters, digits or _.  */
        private val IDENTIFIER_REGEX: Pattern = Pattern.compile("[A-Za-z][A-Za-z0-9_]*")

        /**
         * For usage of TRIM, LTRIM and RTRIM in BQ see
         * [
 * BQ Trim Function](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#trim).
         */
        private fun unparseTrim(
            writer: SqlWriter, call: SqlCall, leftPrec: Int,
            rightPrec: Int
        ) {
            val operatorName: String
            val trimFlag: SqlLiteral = call.operand(0)
            val valueToTrim: SqlLiteral = call.operand(1)
            operatorName = when (trimFlag.getValueAs(SqlTrimFunction.Flag::class.java)) {
                LEADING -> "LTRIM"
                TRAILING -> "RTRIM"
                else -> call.getOperator().getName()
            }
            val trimFrame: SqlWriter.Frame = writer.startFunCall(operatorName)
            call.operand(2).unparse(writer, leftPrec, rightPrec)

            // If the trimmed character is a non-space character, add it to the target SQL.
            // eg: TRIM(BOTH 'A' from 'ABCD'
            // Output Query: TRIM('ABC', 'A')
            val value: String = requireNonNull(valueToTrim.toValue(), "valueToTrim.toValue()")
            if (!value.matches("\\s+")) {
                writer.literal(",")
                call.operand(1).unparse(writer, leftPrec, rightPrec)
            }
            writer.endFunCall(trimFrame)
        }

        private fun validate(timeUnit: TimeUnit): TimeUnit {
            return when (timeUnit) {
                MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, ISOYEAR -> timeUnit
                else -> throw RuntimeException("Time unit $timeUnit is not supported for BigQuery.")
            }
        }

        private fun createSqlDataTypeSpecByName(
            typeAlias: String,
            typeName: SqlTypeName
        ): SqlDataTypeSpec {
            val typeNameSpec = SqlAlienSystemTypeNameSpec(
                typeAlias, typeName, SqlParserPos.ZERO
            )
            return SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO)
        }

        /**
         * List of BigQuery Specific Operators needed to form Syntactically Correct SQL.
         */
        private val UNION_DISTINCT: SqlOperator = SqlSetOperator(
            "UNION DISTINCT", SqlKind.UNION, 14, false
        )
        private val EXCEPT_DISTINCT: SqlSetOperator = SqlSetOperator("EXCEPT DISTINCT", SqlKind.EXCEPT, 14, false)
        private val INTERSECT_DISTINCT: SqlSetOperator =
            SqlSetOperator("INTERSECT DISTINCT", SqlKind.INTERSECT, 18, false)
    }
}
