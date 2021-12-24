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
package org.apache.calcite.sql.type

import org.apache.calcite.rel.type.RelDataType

/**
 * SqlTypeFactoryImpl provides a default implementation of
 * [RelDataTypeFactory] which supports SQL types.
 */
class SqlTypeFactoryImpl  //~ Constructors -----------------------------------------------------------
    (typeSystem: RelDataTypeSystem?) : RelDataTypeFactoryImpl(typeSystem) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun createSqlType(typeName: SqlTypeName): RelDataType {
        if (typeName.allowsPrec()) {
            return createSqlType(typeName, typeSystem.getDefaultPrecision(typeName))
        }
        assertBasic(typeName)
        val newType: RelDataType = BasicSqlType(typeSystem, typeName)
        return canonize(newType)
    }

    @Override
    fun createSqlType(
        typeName: SqlTypeName,
        precision: Int
    ): RelDataType {
        var precision = precision
        val maxPrecision: Int = typeSystem.getMaxPrecision(typeName)
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision
        }
        if (typeName.allowsScale()) {
            return createSqlType(typeName, precision, typeName.getDefaultScale())
        }
        assertBasic(typeName)
        assert(
            precision >= 0
                    || precision == RelDataType.PRECISION_NOT_SPECIFIED
        )
        // Does not check precision when typeName is SqlTypeName#NULL.
        var newType: RelDataType =
            if (precision == RelDataType.PRECISION_NOT_SPECIFIED) BasicSqlType(typeSystem, typeName) else BasicSqlType(
                typeSystem,
                typeName,
                precision
            )
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this)
        return canonize(newType)
    }

    @Override
    fun createSqlType(
        typeName: SqlTypeName,
        precision: Int,
        scale: Int
    ): RelDataType {
        var precision = precision
        assertBasic(typeName)
        assert(
            precision >= 0
                    || precision == RelDataType.PRECISION_NOT_SPECIFIED
        )
        val maxPrecision: Int = typeSystem.getMaxPrecision(typeName)
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision
        }
        var newType: RelDataType = BasicSqlType(typeSystem, typeName, precision, scale)
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this)
        return canonize(newType)
    }

    @Override
    fun createUnknownType(): RelDataType {
        return canonize(UnknownSqlType(this))
    }

    @Override
    fun createMultisetType(
        type: RelDataType?,
        maxCardinality: Long
    ): RelDataType {
        assert(maxCardinality == -1L)
        val newType: RelDataType = MultisetSqlType(type, false)
        return canonize(newType)
    }

    @Override
    fun createArrayType(
        elementType: RelDataType?,
        maxCardinality: Long
    ): RelDataType {
        assert(maxCardinality == -1L)
        val newType = ArraySqlType(elementType, false)
        return canonize(newType)
    }

    @Override
    fun createMapType(
        keyType: RelDataType?,
        valueType: RelDataType?
    ): RelDataType {
        val newType = MapSqlType(keyType, valueType, false)
        return canonize(newType)
    }

    @Override
    fun createSqlIntervalType(
        intervalQualifier: SqlIntervalQualifier
    ): RelDataType {
        val newType: RelDataType = IntervalSqlType(typeSystem, intervalQualifier, false)
        return canonize(newType)
    }

    @Override
    fun createTypeWithCharsetAndCollation(
        type: RelDataType,
        charset: Charset?,
        collation: SqlCollation?
    ): RelDataType {
        assert(SqlTypeUtil.inCharFamily(type)) { type }
        requireNonNull(charset, "charset")
        requireNonNull(collation, "collation")
        val newType: RelDataType
        if (type is BasicSqlType) {
            newType = type.createWithCharsetAndCollation(charset, collation)
        } else if (type is JavaType) {
            val javaType: JavaType = type as JavaType
            newType = JavaType(
                javaType.getJavaClass(),
                javaType.isNullable(),
                charset,
                collation
            )
        } else {
            throw Util.needToImplement("need to implement $type")
        }
        return canonize(newType)
    }

    @Override
    @Nullable
    fun leastRestrictive(types: List<RelDataType>?): RelDataType? {
        assert(types != null)
        assert(types!!.size() >= 1)
        val type0: RelDataType = types!![0]
        if (type0.getSqlTypeName() != null) {
            val resultType: RelDataType? = leastRestrictiveSqlType(types)
            return if (resultType != null) {
                resultType
            } else leastRestrictiveByCast(types)
        }
        return super.leastRestrictive(types)
    }

    @Nullable
    private fun leastRestrictiveByCast(types: List<RelDataType>?): RelDataType? {
        var resultType: RelDataType = types!![0]
        var anyNullable: Boolean = resultType.isNullable()
        for (i in 1 until types.size()) {
            val type: RelDataType = types[i]
            if (type.getSqlTypeName() === SqlTypeName.NULL) {
                anyNullable = true
                continue
            }
            if (type.isNullable()) {
                anyNullable = true
            }
            if (SqlTypeUtil.canCastFrom(type, resultType, false)) {
                resultType = type
            } else {
                if (!SqlTypeUtil.canCastFrom(resultType, type, false)) {
                    return null
                }
            }
        }
        return if (anyNullable) {
            createTypeWithNullability(resultType, true)
        } else {
            resultType
        }
    }

    @Override
    fun createTypeWithNullability(
        type: RelDataType?,
        nullable: Boolean
    ): RelDataType {
        val newType: RelDataType
        newType = if (type is BasicSqlType) {
            (type as BasicSqlType?)!!.createWithNullability(nullable)
        } else if (type is MapSqlType) {
            copyMapType(type, nullable)
        } else if (type is ArraySqlType) {
            copyArrayType(type, nullable)
        } else if (type is MultisetSqlType) {
            copyMultisetType(type, nullable)
        } else if (type is IntervalSqlType) {
            copyIntervalType(type, nullable)
        } else if (type is ObjectSqlType) {
            copyObjectType(type, nullable)
        } else {
            return super.createTypeWithNullability(type, nullable)
        }
        return canonize(newType)
    }

    @Nullable
    private fun leastRestrictiveSqlType(types: List<RelDataType>?): RelDataType? {
        var resultType: RelDataType? = null
        var nullCount = 0
        var nullableCount = 0
        var javaCount = 0
        var anyCount = 0
        for (type in types) {
            val typeName: SqlTypeName = type.getSqlTypeName() ?: return null
            if (typeName === SqlTypeName.ANY) {
                anyCount++
            }
            if (type.isNullable()) {
                ++nullableCount
            }
            if (typeName === SqlTypeName.NULL) {
                ++nullCount
            }
            if (isJavaType(type)) {
                ++javaCount
            }
        }

        //  if any of the inputs are ANY, the output is ANY
        if (anyCount > 0) {
            return createTypeWithNullability(
                createSqlType(SqlTypeName.ANY),
                nullCount > 0 || nullableCount > 0
            )
        }
        for (i in 0 until types!!.size()) {
            var type: RelDataType = types!![i]
            val family: RelDataTypeFamily = type.getFamily()
            val typeName: SqlTypeName = type.getSqlTypeName()
            if (typeName === SqlTypeName.NULL) {
                continue
            }

            // Convert Java types; for instance, JavaType(int) becomes INTEGER.
            // Except if all types are either NULL or Java types.
            if (isJavaType(type) && javaCount + nullCount < types.size()) {
                val originalType: RelDataType = type
                type = if (typeName.allowsPrecScale(true, true)) createSqlType(
                    typeName,
                    type.getPrecision(),
                    type.getScale()
                ) else if (typeName.allowsPrecScale(true, false)) createSqlType(
                    typeName,
                    type.getPrecision()
                ) else createSqlType(typeName)
                type = createTypeWithNullability(type, originalType.isNullable())
            }
            if (resultType == null) {
                resultType = type
                val sqlTypeName: SqlTypeName = resultType.getSqlTypeName()
                if (sqlTypeName === SqlTypeName.ROW) {
                    return leastRestrictiveStructuredType(types)
                }
                if (sqlTypeName === SqlTypeName.ARRAY
                    || sqlTypeName === SqlTypeName.MULTISET
                ) {
                    return leastRestrictiveArrayMultisetType(types, sqlTypeName)
                }
                if (sqlTypeName === SqlTypeName.MAP) {
                    return leastRestrictiveMapType(types, sqlTypeName)
                }
            }
            val resultFamily: RelDataTypeFamily = resultType.getFamily()
            val resultTypeName: SqlTypeName = resultType.getSqlTypeName()
            if (resultFamily !== family) {
                return null
            }
            if (SqlTypeUtil.inCharOrBinaryFamilies(type)) {
                val charset1: Charset = type.getCharset()
                val charset2: Charset = resultType.getCharset()
                val collation1: SqlCollation = type.getCollation()
                val collation2: SqlCollation = resultType.getCollation()
                val precision: Int = SqlTypeUtil.maxPrecision(
                    resultType.getPrecision(),
                    type.getPrecision()
                )

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width.  Otherwise, result is fixed width.
                if (SqlTypeUtil.isLob(resultType)) {
                    resultType = createSqlType(resultType.getSqlTypeName())
                } else if (SqlTypeUtil.isLob(type)) {
                    resultType = createSqlType(type.getSqlTypeName())
                } else if (SqlTypeUtil.isBoundedVariableWidth(resultType)) {
                    resultType = createSqlType(
                        resultType.getSqlTypeName(),
                        precision
                    )
                } else {
                    // this catch-all case covers type variable, and both fixed
                    var newTypeName: SqlTypeName = type.getSqlTypeName()
                    if (typeSystem.shouldConvertRaggedUnionTypesToVarying()) {
                        if (resultType.getPrecision() !== type.getPrecision()) {
                            if (newTypeName === SqlTypeName.CHAR) {
                                newTypeName = SqlTypeName.VARCHAR
                            } else if (newTypeName === SqlTypeName.BINARY) {
                                newTypeName = SqlTypeName.VARBINARY
                            }
                        }
                    }
                    resultType = createSqlType(
                        newTypeName,
                        precision
                    )
                }
                var charset: Charset? = null
                // TODO:  refine collation combination rules
                val collation0: SqlCollation? =
                    if (collation1 != null && collation2 != null) SqlCollation.getCoercibilityDyadicOperator(
                        collation1,
                        collation2
                    ) else null
                var collation: SqlCollation? = null
                if (charset1 != null || charset2 != null) {
                    if (charset1 == null) {
                        charset = charset2
                        collation = collation2
                    } else if (charset2 == null) {
                        charset = charset1
                        collation = collation1
                    } else if (charset1.equals(charset2)) {
                        charset = charset1
                        collation = collation1
                    } else if (charset1.contains(charset2)) {
                        charset = charset1
                        collation = collation1
                    } else {
                        charset = charset2
                        collation = collation2
                    }
                }
                if (charset != null) {
                    resultType = createTypeWithCharsetAndCollation(
                        resultType,
                        charset,
                        if (collation0 != null) collation0 else requireNonNull(collation, "collation")
                    )
                }
            } else if (SqlTypeUtil.isExactNumeric(type)) {
                if (SqlTypeUtil.isExactNumeric(resultType)) {
                    // TODO: come up with a cleaner way to support
                    // interval + datetime = datetime
                    if (types.size() > i + 1) {
                        val type1: RelDataType = types[i + 1]
                        if (SqlTypeUtil.isDatetime(type1)) {
                            resultType = type1
                            return createTypeWithNullability(
                                resultType,
                                nullCount > 0 || nullableCount > 0
                            )
                        }
                    }
                    if (!type.equals(resultType)) {
                        if (!typeName.allowsPrec()
                            && !resultTypeName.allowsPrec()
                        ) {
                            // use the bigger primitive
                            if (type.getPrecision()
                                > resultType.getPrecision()
                            ) {
                                resultType = type
                            }
                        } else {
                            // Let the result type have precision (p), scale (s)
                            // and number of whole digits (d) as follows: d =
                            // max(p1 - s1, p2 - s2) s <= max(s1, s2) p = s + d
                            val p1: Int = resultType.getPrecision()
                            val p2: Int = type.getPrecision()
                            val s1: Int = resultType.getScale()
                            val s2: Int = type.getScale()
                            val maxPrecision: Int = typeSystem.getMaxNumericPrecision()
                            val maxScale: Int = typeSystem.getMaxNumericScale()
                            var dout: Int = Math.max(p1 - s1, p2 - s2)
                            dout = Math.min(
                                dout,
                                maxPrecision
                            )
                            var scale: Int = Math.max(s1, s2)
                            scale = Math.min(
                                scale,
                                maxPrecision - dout
                            )
                            scale = Math.min(scale, maxScale)
                            val precision = dout + scale
                            assert(precision <= maxPrecision)
                            assert(
                                precision > 0
                                        || resultType.getSqlTypeName() === SqlTypeName.DECIMAL && precision == 0 && scale == 0
                            )
                            resultType = createSqlType(
                                SqlTypeName.DECIMAL,
                                precision,
                                scale
                            )
                        }
                    }
                } else if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    // already approximate; promote to double just in case
                    // TODO:  only promote when required
                    if (SqlTypeUtil.isDecimal(type)) {
                        // Only promote to double for decimal types
                        resultType = createDoublePrecisionType()
                    }
                } else {
                    return null
                }
            } else if (SqlTypeUtil.isApproximateNumeric(type)) {
                if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    if (type.getPrecision() > resultType.getPrecision()) {
                        resultType = type
                    }
                } else if (SqlTypeUtil.isExactNumeric(resultType)) {
                    resultType = if (SqlTypeUtil.isDecimal(resultType)) {
                        createDoublePrecisionType()
                    } else {
                        type
                    }
                } else {
                    return null
                }
            } else if (SqlTypeUtil.isInterval(type)) {
                // TODO: come up with a cleaner way to support
                // interval + datetime = datetime
                if (types.size() > i + 1) {
                    val type1: RelDataType = types[i + 1]
                    if (SqlTypeUtil.isDatetime(type1)) {
                        resultType = type1
                        return createTypeWithNullability(
                            resultType,
                            nullCount > 0 || nullableCount > 0
                        )
                    }
                }
                if (!type.equals(resultType)) {
                    // TODO jvs 4-June-2005:  This shouldn't be necessary;
                    // move logic into IntervalSqlType.combine
                    val type1: Object? = resultType
                    resultType = (resultType as IntervalSqlType?)!!.combine(
                        this,
                        type as IntervalSqlType
                    )
                    resultType = (resultType as IntervalSqlType?)!!.combine(
                        this,
                        type1 as IntervalSqlType?
                    )
                }
            } else if (SqlTypeUtil.isDatetime(type)) {
                // TODO: come up with a cleaner way to support
                // datetime +/- interval (or integer) = datetime
                if (types.size() > i + 1) {
                    val type1: RelDataType = types[i + 1]
                    if (SqlTypeUtil.isInterval(type1)
                        || SqlTypeUtil.isIntType(type1)
                    ) {
                        resultType = type
                        return createTypeWithNullability(
                            resultType,
                            nullCount > 0 || nullableCount > 0
                        )
                    }
                }
            } else {
                // TODO:  datetime precision details; for now we let
                // leastRestrictiveByCast handle it
                return null
            }
        }
        if (resultType != null && nullableCount > 0) {
            resultType = createTypeWithNullability(resultType, true)
        }
        return resultType
    }

    private fun createDoublePrecisionType(): RelDataType {
        return createSqlType(SqlTypeName.DOUBLE)
    }

    private fun copyMultisetType(type: RelDataType?, nullable: Boolean): RelDataType {
        val elementType: RelDataType = copyType(type.getComponentType())
        return MultisetSqlType(elementType, nullable)
    }

    private fun copyIntervalType(type: RelDataType?, nullable: Boolean): RelDataType {
        return IntervalSqlType(
            typeSystem,
            requireNonNull(
                type.getIntervalQualifier()
            ) { "type.getIntervalQualifier() for $type" },
            nullable
        )
    }

    private fun copyArrayType(type: RelDataType?, nullable: Boolean): RelDataType {
        val elementType: RelDataType = copyType(type.getComponentType())
        return ArraySqlType(elementType, nullable)
    }

    private fun copyMapType(type: RelDataType?, nullable: Boolean): RelDataType {
        val mt: MapSqlType? = type
        val keyType: RelDataType = copyType(mt!!.getKeyType())
        val valueType: RelDataType = copyType(mt!!.getValueType())
        return MapSqlType(keyType, valueType, nullable)
    }

    // override RelDataTypeFactoryImpl
    @Override
    protected fun canonize(type: RelDataType): RelDataType {
        var type: RelDataType = type
        type = super.canonize(type)
        if (type !is ObjectSqlType) {
            return type
        }
        val objectType: ObjectSqlType = type
        if (!objectType.isNullable()) {
            objectType.setFamily(objectType)
        } else {
            objectType.setFamily(
                createTypeWithNullability(
                    objectType,
                    false
                ) as RelDataTypeFamily
            )
        }
        return type
    }

    /** The unknown type. Similar to the NULL type, but is only equal to
     * itself.  */
    private class UnknownSqlType internal constructor(typeFactory: RelDataTypeFactory) :
        BasicSqlType(typeFactory.getTypeSystem(), SqlTypeName.NULL) {
        @Override
        protected override fun generateTypeString(
            sb: StringBuilder,
            withDetail: Boolean
        ) {
            sb.append("UNKNOWN")
        }
    }

    companion object {
        private fun assertBasic(typeName: SqlTypeName?) {
            assert(typeName != null)
            assert(typeName !== SqlTypeName.MULTISET) { "use createMultisetType() instead" }
            assert(typeName !== SqlTypeName.ARRAY) { "use createArrayType() instead" }
            assert(typeName !== SqlTypeName.MAP) { "use createMapType() instead" }
            assert(typeName !== SqlTypeName.ROW) { "use createStructType() instead" }
            assert(!SqlTypeName.INTERVAL_TYPES.contains(typeName)) { "use createSqlIntervalType() instead" }
        }

        private fun copyObjectType(type: RelDataType?, nullable: Boolean): RelDataType {
            return ObjectSqlType(
                type.getSqlTypeName(),
                type.getSqlIdentifier(),
                nullable,
                type.getFieldList(),
                type.getComparability()
            )
        }
    }
}
