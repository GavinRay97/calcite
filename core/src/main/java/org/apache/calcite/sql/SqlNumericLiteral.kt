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

import org.apache.calcite.rel.type.RelDataType

/**
 * A numeric SQL literal.
 */
class SqlNumericLiteral protected constructor(
    value: BigDecimal?,
    @Nullable prec: Integer,
    @Nullable scale: Integer?,
    isExact: Boolean,
    pos: SqlParserPos?
) : SqlLiteral(
    value,
    if (isExact) SqlTypeName.DECIMAL else SqlTypeName.DOUBLE,
    pos
) {
    //~ Instance fields --------------------------------------------------------
    @Nullable
    private val prec: Integer

    @Nullable
    private val scale: Integer?
    val isExact: Boolean

    //~ Constructors -----------------------------------------------------------
    init {
        this.prec = prec
        this.scale = scale
        this.isExact = isExact
    }

    //~ Methods ----------------------------------------------------------------
    private val valueNonNull: BigDecimal
        private get() = requireNonNull(value, "value") as BigDecimal

    @Nullable
    fun getPrec(): Integer {
        return prec
    }

    @Pure
    @Nullable
    fun getScale(): Integer? {
        return scale
    }

    @Override
    fun clone(pos: SqlParserPos?): SqlNumericLiteral {
        return SqlNumericLiteral(
            valueNonNull, getPrec(), getScale(),
            isExact, pos
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.literal(toValue())
    }

    @Override
    fun toValue(): String {
        val bd: BigDecimal = valueNonNull
        return if (isExact) {
            valueNonNull.toString()
        } else Util.toScientificNotation(bd)
    }

    @Override
    fun createSqlType(typeFactory: RelDataTypeFactory): RelDataType {
        if (isExact) {
            val scaleValue: Int = requireNonNull(scale, "scale")
            if (0 == scaleValue) {
                val bd: BigDecimal = valueNonNull
                val result: SqlTypeName
                val l: Long = bd.longValue()
                result = if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    SqlTypeName.INTEGER
                } else {
                    SqlTypeName.BIGINT
                }
                return typeFactory.createSqlType(result)
            }

            // else we have a decimal
            return typeFactory.createSqlType(
                SqlTypeName.DECIMAL,
                requireNonNull(prec, "prec"),
                scaleValue
            )
        }

        // else we have a a float, real or double.  make them all double for
        // now.
        return typeFactory.createSqlType(SqlTypeName.DOUBLE)
    }

    val isInteger: Boolean
        get() = scale != null && 0 == scale.intValue()
}
