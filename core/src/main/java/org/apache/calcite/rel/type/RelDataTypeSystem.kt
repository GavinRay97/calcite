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

import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.util.Glossary

/**
 * Type system.
 *
 *
 * Provides behaviors concerning type limits and behaviors. For example,
 * in the default system, a DECIMAL can have maximum precision 19, but Hive
 * overrides to 38.
 *
 *
 * The default implementation is [.DEFAULT].
 */
interface RelDataTypeSystem {
    /** Returns the maximum scale of a given type.  */
    fun getMaxScale(typeName: SqlTypeName?): Int

    /**
     * Returns default precision for this type if supported, otherwise -1 if
     * precision is either unsupported or must be specified explicitly.
     *
     * @return Default precision
     */
    fun getDefaultPrecision(typeName: SqlTypeName?): Int

    /**
     * Returns the maximum precision (or length) allowed for this type, or -1 if
     * precision/length are not applicable for this type.
     *
     * @return Maximum allowed precision
     */
    fun getMaxPrecision(typeName: SqlTypeName?): Int

    /** Returns the maximum scale of a NUMERIC or DECIMAL type.  */
    fun getMaxNumericScale(): Int

    /** Returns the maximum precision of a NUMERIC or DECIMAL type.  */
    fun getMaxNumericPrecision(): Int

    /** Returns the LITERAL string for the type, either PREFIX/SUFFIX.  */
    @Nullable
    fun getLiteral(typeName: SqlTypeName?, isPrefix: Boolean): String

    /** Returns whether the type is case sensitive.  */
    fun isCaseSensitive(typeName: SqlTypeName?): Boolean

    /** Returns whether the type can be auto increment.  */
    fun isAutoincrement(typeName: SqlTypeName?): Boolean

    /** Returns the numeric type radix, typically 2 or 10.
     * 0 means "not applicable".  */
    fun getNumTypeRadix(typeName: SqlTypeName?): Int

    /** Returns the return type of a call to the `SUM` aggregate function,
     * inferred from its argument type.  */
    fun deriveSumType(
        typeFactory: RelDataTypeFactory?,
        argumentType: RelDataType?
    ): RelDataType

    /** Returns the return type of a call to the `AVG`, `STDDEV` or
     * `VAR` aggregate functions, inferred from its argument type.
     */
    fun deriveAvgAggType(
        typeFactory: RelDataTypeFactory?,
        argumentType: RelDataType?
    ): RelDataType

    /** Returns the return type of a call to the `COVAR` aggregate function,
     * inferred from its argument types.  */
    fun deriveCovarType(
        typeFactory: RelDataTypeFactory?,
        arg0Type: RelDataType?, arg1Type: RelDataType?
    ): RelDataType

    /** Returns the return type of the `CUME_DIST` and `PERCENT_RANK`
     * aggregate functions.  */
    fun deriveFractionalRankType(typeFactory: RelDataTypeFactory?): RelDataType

    /** Returns the return type of the `NTILE`, `RANK`,
     * `DENSE_RANK`, and `ROW_NUMBER` aggregate functions.  */
    fun deriveRankType(typeFactory: RelDataTypeFactory?): RelDataType

    /** Whether two record types are considered distinct if their field names
     * are the same but in different cases.  */
    fun isSchemaCaseSensitive(): Boolean

    /** Whether the least restrictive type of a number of CHAR types of different
     * lengths should be a VARCHAR type. And similarly BINARY to VARBINARY.  */
    fun shouldConvertRaggedUnionTypesToVarying(): Boolean

    /**
     * Returns whether a decimal multiplication should be implemented by casting
     * arguments to double values.
     *
     *
     * Pre-condition: `createDecimalProduct(type1, type2) != null`
     */
    fun shouldUseDoubleMultiplication(
        typeFactory: RelDataTypeFactory,
        type1: RelDataType,
        type2: RelDataType
    ): Boolean {
        assert(deriveDecimalMultiplyType(typeFactory, type1, type2) != null)
        return false
    }

    /**
     * Infers the return type of a decimal addition. Decimal addition involves
     * at least one decimal operand and requires both operands to have exact
     * numeric types.
     *
     *
     * Rules:
     *
     *
     *  * Let p1, s1 be the precision and scale of the first operand
     *  * Let p2, s2 be the precision and scale of the second operand
     *  * Let p, s be the precision and scale of the result
     *  * Let d be the number of whole digits in the result
     *  * Then the result type is a decimal with:
     *
     *  * s = max(s1, s2)
     *  * p = max(p1 - s1, p2 - s2) + s + 1
     *
     *
     *  * p and s are capped at their maximum values
     *
     *
     * @see Glossary.SQL2003 SQL:2003 Part 2 Section 6.26
     *
     *
     * @param typeFactory TypeFactory used to create output type
     * @param type1       Type of the first operand
     * @param type2       Type of the second operand
     * @return Result type for a decimal addition
     */
    @Nullable
    fun deriveDecimalPlusType(
        typeFactory: RelDataTypeFactory,
        type1: RelDataType, type2: RelDataType
    ): RelDataType? {
        var type1: RelDataType = type1
        var type2: RelDataType = type2
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2)
        ) {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2)
            ) {
                // Java numeric will always have invalid precision/scale,
                // use its default decimal precision/scale instead.
                type1 = if (RelDataTypeFactoryImpl.isJavaType(type1)) typeFactory.decimalOf(type1) else type1
                type2 = if (RelDataTypeFactoryImpl.isJavaType(type2)) typeFactory.decimalOf(type2) else type2
                val p1: Int = type1.getPrecision()
                val p2: Int = type2.getPrecision()
                val s1: Int = type1.getScale()
                val s2: Int = type2.getScale()
                val scale: Int = Math.max(s1, s2)
                assert(scale <= getMaxNumericScale())
                var precision: Int = Math.max(p1 - s1, p2 - s2) + scale + 1
                precision = Math.min(
                    precision,
                    getMaxNumericPrecision()
                )
                assert(precision > 0)
                return typeFactory.createSqlType(
                    SqlTypeName.DECIMAL,
                    precision,
                    scale
                )
            }
        }
        return null
    }

    /**
     * Infers the return type of a decimal multiplication. Decimal
     * multiplication involves at least one decimal operand and requires both
     * operands to have exact numeric types.
     *
     *
     * The default implementation is SQL:2003 compliant.
     *
     *
     * Rules:
     *
     *
     *  * Let p1, s1 be the precision and scale of the first operand
     *  * Let p2, s2 be the precision and scale of the second operand
     *  * Let p, s be the precision and scale of the result
     *  * Let d be the number of whole digits in the result
     *  * Then the result type is a decimal with:
     *
     *  * p = p1 + p2)
     *  * s = s1 + s2
     *
     *
     *  * p and s are capped at their maximum values
     *
     *
     *
     * p and s are capped at their maximum values
     *
     * @see Glossary.SQL2003 SQL:2003 Part 2 Section 6.26
     *
     *
     * @param typeFactory TypeFactory used to create output type
     * @param type1       Type of the first operand
     * @param type2       Type of the second operand
     * @return Result type for a decimal multiplication, or null if decimal
     * multiplication should not be applied to the operands
     */
    @Nullable
    fun deriveDecimalMultiplyType(
        typeFactory: RelDataTypeFactory,
        type1: RelDataType, type2: RelDataType
    ): RelDataType? {
        var type1: RelDataType = type1
        var type2: RelDataType = type2
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2)
        ) {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2)
            ) {
                // Java numeric will always have invalid precision/scale,
                // use its default decimal precision/scale instead.
                type1 = if (RelDataTypeFactoryImpl.isJavaType(type1)) typeFactory.decimalOf(type1) else type1
                type2 = if (RelDataTypeFactoryImpl.isJavaType(type2)) typeFactory.decimalOf(type2) else type2
                val p1: Int = type1.getPrecision()
                val p2: Int = type2.getPrecision()
                val s1: Int = type1.getScale()
                val s2: Int = type2.getScale()
                var scale = s1 + s2
                scale = Math.min(scale, getMaxNumericScale())
                var precision = p1 + p2
                precision = Math.min(
                    precision,
                    getMaxNumericPrecision()
                )
                val ret: RelDataType
                ret = typeFactory.createSqlType(
                    SqlTypeName.DECIMAL,
                    precision,
                    scale
                )
                return ret
            }
        }
        return null
    }

    /**
     * Infers the return type of a decimal division. Decimal division involves
     * at least one decimal operand and requires both operands to have exact
     * numeric types.
     *
     *
     * The default implementation is SQL:2003 compliant.
     *
     *
     * Rules:
     *
     *
     *  * Let p1, s1 be the precision and scale of the first operand
     *  * Let p2, s2 be the precision and scale of the second operand
     *  * Let p, s be the precision and scale of the result
     *  * Let d be the number of whole digits in the result
     *  * Then the result type is a decimal with:
     *
     *  * d = p1 - s1 + s2
     *  * s &lt; max(6, s1 + p2 + 1)
     *  * p = d + s
     *
     *
     *  * p and s are capped at their maximum values
     *
     *
     * @see Glossary.SQL2003 SQL:2003 Part 2 Section 6.26
     *
     *
     * @param typeFactory TypeFactory used to create output type
     * @param type1       Type of the first operand
     * @param type2       Type of the second operand
     * @return Result type for a decimal division, or null if decimal
     * division should not be applied to the operands
     */
    @Nullable
    fun deriveDecimalDivideType(
        typeFactory: RelDataTypeFactory,
        type1: RelDataType, type2: RelDataType
    ): RelDataType? {
        var type1: RelDataType = type1
        var type2: RelDataType = type2
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2)
        ) {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2)
            ) {
                // Java numeric will always have invalid precision/scale,
                // use its default decimal precision/scale instead.
                type1 = if (RelDataTypeFactoryImpl.isJavaType(type1)) typeFactory.decimalOf(type1) else type1
                type2 = if (RelDataTypeFactoryImpl.isJavaType(type2)) typeFactory.decimalOf(type2) else type2
                val p1: Int = type1.getPrecision()
                val p2: Int = type2.getPrecision()
                val s1: Int = type1.getScale()
                val s2: Int = type2.getScale()
                val maxNumericPrecision = getMaxNumericPrecision()
                val dout: Int = Math.min(
                    p1 - s1 + s2,
                    maxNumericPrecision
                )
                var scale: Int = Math.max(6, s1 + p2 + 1)
                scale = Math.min(
                    scale,
                    maxNumericPrecision - dout
                )
                scale = Math.min(scale, getMaxNumericScale())
                val precision = dout + scale
                assert(precision <= maxNumericPrecision)
                assert(precision > 0)
                val ret: RelDataType
                ret = typeFactory.createSqlType(
                    SqlTypeName.DECIMAL,
                    precision,
                    scale
                )
                return ret
            }
        }
        return null
    }

    /**
     * Infers the return type of a decimal modulus operation. Decimal modulus
     * involves at least one decimal operand.
     *
     *
     * The default implementation is SQL:2003 compliant: the declared type of
     * the result is the declared type of the second operand (expression divisor).
     *
     * @see Glossary.SQL2003 SQL:2003 Part 2 Section 6.27
     *
     *
     * Rules:
     *
     *
     *  * Let p1, s1 be the precision and scale of the first operand
     *  * Let p2, s2 be the precision and scale of the second operand
     *  * Let p, s be the precision and scale of the result
     *  * Let d be the number of whole digits in the result
     *  * Then the result type is a decimal with:
     *
     *  * s = max
     * @param typeFactory TypeFactory used to create output type
     * @param type1       Type of the first operand
     * @param type2       Type of the second operand
     * @return Result type for a decimal modulus, or null if decimal
     * modulus should not be applied to the operands
     */
    @Nullable
    fun deriveDecimalModType(
        typeFactory: RelDataTypeFactory,
        type1: RelDataType, type2: RelDataType
    ): RelDataType? {
        var type1: RelDataType = type1
        var type2: RelDataType = type2
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2)
        ) {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2)
            ) {
                // Java numeric will always have invalid precision/scale,
                // use its default decimal precision/scale instead.
                type1 = if (RelDataTypeFactoryImpl.isJavaType(type1)) typeFactory.decimalOf(type1) else type1
                type2 = if (RelDataTypeFactoryImpl.isJavaType(type2)) typeFactory.decimalOf(type2) else type2
                val p1: Int = type1.getPrecision()
                val p2: Int = type2.getPrecision()
                val s1: Int = type1.getScale()
                val s2: Int = type2.getScale()
                // Keep consistency with SQL standard.
                if (s1 == 0 && s2 == 0) {
                    return type2
                }
                val scale: Int = Math.max(s1, s2)
                assert(scale <= getMaxNumericScale())
                var precision: Int = Math.min(p1 - s1, p2 - s2) + Math.max(s1, s2)
                precision = Math.min(precision, getMaxNumericPrecision())
                assert(precision > 0)
                return typeFactory.createSqlType(
                    SqlTypeName.DECIMAL,
                    precision, scale
                )
            }
        }
        return null
    }

    companion object {
        /** Default type system.  */
        val DEFAULT: RelDataTypeSystem = object : RelDataTypeSystemImpl() {}
    }
}
