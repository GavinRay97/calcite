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
package org.apache.calcite.rel.type

import org.apache.calcite.sql.type.BasicSqlType
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName

/** Default implementation of
 * [org.apache.calcite.rel.type.RelDataTypeSystem],
 * providing parameters from the SQL standard.
 *
 *
 * To implement other type systems, create a derived class and override
 * values as needed.
 *
 * <table border='1'>
 * <caption>Parameter values</caption>
 * <tr><th>Parameter</th>         <th>Value</th></tr>
 * <tr><td>MAX_NUMERIC_SCALE</td> <td>19</td></tr>
</table> *
 */
abstract class RelDataTypeSystemImpl : RelDataTypeSystem {
    @Override
    override fun getMaxScale(typeName: SqlTypeName?): Int {
        return when (typeName) {
            DECIMAL -> getMaxNumericScale()
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> SqlTypeName.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION
            else -> -1
        }
    }

    @Override
    override fun getDefaultPrecision(typeName: SqlTypeName?): Int {
        // Following BasicSqlType precision as the default
        return when (typeName) {
            CHAR, BINARY -> 1
            VARCHAR, VARBINARY -> RelDataType.PRECISION_NOT_SPECIFIED
            DECIMAL -> getMaxNumericPrecision()
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> SqlTypeName.DEFAULT_INTERVAL_START_PRECISION
            BOOLEAN -> 1
            TINYINT -> 3
            SMALLINT -> 5
            INTEGER -> 10
            BIGINT -> 19
            REAL -> 7
            FLOAT, DOUBLE -> 15
            TIME, TIME_WITH_LOCAL_TIME_ZONE, DATE -> 0 // SQL99 part 2 section 6.1 syntax rule 30
            TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->       // farrago supports only 0 (see
                // SqlTypeName.getDefaultPrecision), but it should be 6
                // (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
                0
            else -> -1
        }
    }

    @Override
    override fun getMaxPrecision(typeName: SqlTypeName?): Int {
        return when (typeName) {
            DECIMAL -> getMaxNumericPrecision()
            VARCHAR, CHAR -> 65536
            VARBINARY, BINARY -> 65536
            TIME, TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> SqlTypeName.MAX_DATETIME_PRECISION
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> SqlTypeName.MAX_INTERVAL_START_PRECISION
            else -> getDefaultPrecision(typeName)
        }
    }

    @Override
    override fun getMaxNumericScale(): Int {
        return 19
    }

    @Override
    override fun getMaxNumericPrecision(): Int {
        return 19
    }

    @Override
    @Nullable
    override fun getLiteral(typeName: SqlTypeName?, isPrefix: Boolean): String? {
        return when (typeName) {
            VARBINARY, VARCHAR, CHAR -> "'"
            BINARY -> if (isPrefix) "x'" else "'"
            TIMESTAMP -> if (isPrefix) "TIMESTAMP '" else "'"
            TIMESTAMP_WITH_LOCAL_TIME_ZONE -> if (isPrefix) "TIMESTAMP WITH LOCAL TIME ZONE '" else "'"
            INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> if (isPrefix) "INTERVAL '" else "' DAY"
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> if (isPrefix) "INTERVAL '" else "' YEAR TO MONTH"
            TIME -> if (isPrefix) "TIME '" else "'"
            TIME_WITH_LOCAL_TIME_ZONE -> if (isPrefix) "TIME WITH LOCAL TIME ZONE '" else "'"
            DATE -> if (isPrefix) "DATE '" else "'"
            ARRAY -> if (isPrefix) "(" else ")"
            else -> null
        }
    }

    @Override
    override fun isCaseSensitive(typeName: SqlTypeName?): Boolean {
        return when (typeName) {
            CHAR, VARCHAR -> true
            else -> false
        }
    }

    @Override
    override fun isAutoincrement(typeName: SqlTypeName?): Boolean {
        return false
    }

    @Override
    override fun getNumTypeRadix(typeName: SqlTypeName): Int {
        return if (typeName.getFamily() === SqlTypeFamily.NUMERIC
            && getDefaultPrecision(typeName) != -1
        ) {
            10
        } else 0
    }

    @Override
    fun deriveSumType(
        typeFactory: RelDataTypeFactory,
        argumentType: RelDataType
    ): RelDataType {
        var argumentType: RelDataType = argumentType
        if (argumentType is BasicSqlType) {
            val typeName: SqlTypeName = argumentType.getSqlTypeName()
            if (typeName.allowsPrec()
                && argumentType.getPrecision() !== RelDataType.PRECISION_NOT_SPECIFIED
            ) {
                val precision: Int = typeFactory.getTypeSystem().getMaxPrecision(typeName)
                argumentType = if (typeName.allowsScale()) {
                    typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(typeName, precision, argumentType.getScale()),
                        argumentType.isNullable()
                    )
                } else {
                    typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(typeName, precision), argumentType.isNullable()
                    )
                }
            }
        }
        return argumentType
    }

    @Override
    fun deriveAvgAggType(
        typeFactory: RelDataTypeFactory?,
        argumentType: RelDataType
    ): RelDataType {
        return argumentType
    }

    @Override
    fun deriveCovarType(
        typeFactory: RelDataTypeFactory?,
        arg0Type: RelDataType, arg1Type: RelDataType?
    ): RelDataType {
        return arg0Type
    }

    @Override
    fun deriveFractionalRankType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.DOUBLE), false
        )
    }

    @Override
    fun deriveRankType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT), false
        )
    }

    @Override
    override fun isSchemaCaseSensitive(): Boolean {
        return true
    }

    @Override
    override fun shouldConvertRaggedUnionTypesToVarying(): Boolean {
        return false
    }

    fun allowExtendedTrim(): Boolean {
        return false
    }
}
