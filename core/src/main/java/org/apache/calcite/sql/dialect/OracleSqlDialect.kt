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
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.rel.type.RelDataTypeSystemImpl
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDateLiteral
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlTimeLiteral
import org.apache.calcite.sql.SqlTimestampLiteral
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlFloorFunction
import org.apache.calcite.sql.`fun`.SqlLibraryOperators
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import com.google.common.collect.ImmutableList
import java.util.List

/**
 * A `SqlDialect` implementation for the Oracle database.
 */
class OracleSqlDialect
/** Creates an OracleSqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun supportsDataType(type: RelDataType): Boolean {
        return when (type.getSqlTypeName()) {
            BOOLEAN -> false
            else -> super.supportsDataType(type)
        }
    }

    @Override
    @Nullable
    fun getCastSpec(type: RelDataType): SqlNode {
        val castSpec: String
        castSpec = when (type.getSqlTypeName()) {
            SMALLINT -> "NUMBER(5)"
            INTEGER -> "NUMBER(10)"
            BIGINT -> "NUMBER(19)"
            DOUBLE -> "DOUBLE PRECISION"
            else -> return super.getCastSpec(type)
        }
        return SqlDataTypeSpec(
            SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
            SqlParserPos.ZERO
        )
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
    fun unparseDateTimeLiteral(
        writer: SqlWriter,
        literal: SqlAbstractDateTimeLiteral, leftPrec: Int, rightPrec: Int
    ) {
        if (literal is SqlTimestampLiteral) {
            writer.literal(
                ("TO_TIMESTAMP('"
                        + literal.toFormattedString()) + "', 'YYYY-MM-DD HH24:MI:SS.FF')"
            )
        } else if (literal is SqlDateLiteral) {
            writer.literal(
                ("TO_DATE('"
                        + literal.toFormattedString()) + "', 'YYYY-MM-DD')"
            )
        } else if (literal is SqlTimeLiteral) {
            writer.literal(
                ("TO_TIME('"
                        + literal.toFormattedString()) + "', 'HH24:MI:SS.FF')"
            )
        } else {
            super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec)
        }
    }

    @get:Override
    val singleRowTableName: List<String>
        get() = ImmutableList.of("DUAL")

    @Override
    fun unparseCall(
        writer: SqlWriter?, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        if (call.getOperator() === SqlStdOperatorTable.SUBSTRING) {
            SqlUtil.unparseFunctionSyntax(
                SqlLibraryOperators.SUBSTR_ORACLE, writer,
                call, false
            )
        } else {
            when (call.getKind()) {
                FLOOR -> {
                    if (call.operandCount() !== 2) {
                        super.unparseCall(writer, call, leftPrec, rightPrec)
                        return
                    }
                    val timeUnitNode: SqlLiteral = call.operand(1)
                    val timeUnit: TimeUnitRange = timeUnitNode.getValueAs(TimeUnitRange::class.java)
                    val call2: SqlCall = SqlFloorFunction.replaceTimeUnitOperand(
                        call, timeUnit.name(),
                        timeUnitNode.getParserPosition()
                    )
                    SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true)
                }
                else -> super.unparseCall(writer, call, leftPrec, rightPrec)
            }
        }
    }

    companion object {
        /** OracleDB type system.  */
        private val ORACLE_TYPE_SYSTEM: RelDataTypeSystem = object : RelDataTypeSystemImpl() {
            @Override
            fun getMaxPrecision(typeName: SqlTypeName?): Int {
                return when (typeName) {
                    VARCHAR ->             // Maximum size of 4000 bytes for varchar2.
                        4000
                    else -> super.getMaxPrecision(typeName)
                }
            }
        }
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.ORACLE)
            .withIdentifierQuoteString("\"")
            .withDataTypeSystem(ORACLE_TYPE_SYSTEM)
        val DEFAULT: SqlDialect = OracleSqlDialect(DEFAULT_CONTEXT)
    }
}
