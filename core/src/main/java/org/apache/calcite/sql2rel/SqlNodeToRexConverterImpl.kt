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
package org.apache.calcite.sql2rel

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlTimeLiteral
import org.apache.calcite.sql.SqlTimestampLiteral
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.util.BitString
import org.apache.calcite.util.DateString
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.TimeString
import org.apache.calcite.util.TimestampString
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import java.math.BigDecimal

/**
 * Standard implementation of [SqlNodeToRexConverter].
 */
class SqlNodeToRexConverterImpl internal constructor(convertletTable: SqlRexConvertletTable) : SqlNodeToRexConverter {
    //~ Instance fields --------------------------------------------------------
    private val convertletTable: SqlRexConvertletTable

    //~ Constructors -----------------------------------------------------------
    init {
        this.convertletTable = convertletTable
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun convertCall(cx: SqlRexContext?, call: SqlCall?): RexNode {
        val convertlet: SqlRexConvertlet = convertletTable.get(call)
        if (convertlet != null) {
            return convertlet.convertCall(cx, call)
        }
        throw Util.needToImplement(call)
    }

    @Override
    fun convertInterval(
        cx: SqlRexContext,
        intervalQualifier: SqlIntervalQualifier?
    ): RexLiteral {
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        return rexBuilder.makeIntervalLiteral(intervalQualifier)
    }

    @Override
    fun convertLiteral(
        cx: SqlRexContext,
        literal: SqlLiteral
    ): RexNode {
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val typeFactory: RelDataTypeFactory = cx.getTypeFactory()
        val validator: SqlValidator = cx.getValidator()
        if (literal.getValue() == null) {
            // Since there is no eq. RexLiteral of SqlLiteral.Unknown we
            // treat it as a cast(null as boolean)
            var type: RelDataType
            if (literal.getTypeName() === SqlTypeName.BOOLEAN) {
                type = typeFactory.createSqlType(SqlTypeName.BOOLEAN)
                type = typeFactory.createTypeWithNullability(type, true)
            } else {
                type = validator.getValidatedNodeType(literal)
            }
            return rexBuilder.makeNullLiteral(type)
        }
        val bitString: BitString
        return when (literal.getTypeName()) {
            DECIMAL -> {
                // exact number
                val bd: BigDecimal = literal.getValueAs(BigDecimal::class.java)
                rexBuilder.makeExactLiteral(
                    bd,
                    literal.createSqlType(typeFactory)
                )
            }
            DOUBLE ->       // approximate type
                // TODO:  preserve fixed-point precision and large integers
                rexBuilder.makeApproxLiteral(literal.getValueAs(BigDecimal::class.java))
            CHAR -> rexBuilder.makeCharLiteral(literal.getValueAs(NlsString::class.java))
            BOOLEAN -> rexBuilder.makeLiteral(literal.getValueAs(Boolean::class.java))
            BINARY -> {
                bitString = literal.getValueAs(BitString::class.java)
                Preconditions.checkArgument(
                    bitString.getBitCount() % 8 === 0,
                    "incomplete octet"
                )

                // An even number of hexits (e.g. X'ABCD') makes whole number
                // of bytes.
                val byteString = ByteString(bitString.getAsByteArray())
                rexBuilder.makeBinaryLiteral(byteString)
            }
            SYMBOL -> rexBuilder.makeFlag(literal.getValueAs(Enum::class.java))
            TIMESTAMP -> rexBuilder.makeTimestampLiteral(
                literal.getValueAs(TimestampString::class.java),
                (literal as SqlTimestampLiteral).getPrec()
            )
            TIME -> rexBuilder.makeTimeLiteral(
                literal.getValueAs(TimeString::class.java),
                (literal as SqlTimeLiteral).getPrec()
            )
            DATE -> rexBuilder.makeDateLiteral(literal.getValueAs(DateString::class.java))
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                val sqlIntervalQualifier: SqlIntervalQualifier = literal.getValueAs(SqlIntervalQualifier::class.java)
                rexBuilder.makeIntervalLiteral(
                    literal.getValueAs(BigDecimal::class.java),
                    sqlIntervalQualifier
                )
            }
            else -> throw Util.unexpected(literal.getTypeName())
        }
    }
}
