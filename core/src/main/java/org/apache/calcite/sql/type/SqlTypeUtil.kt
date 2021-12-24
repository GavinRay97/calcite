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
 * Contains utility methods used during SQL validation or type derivation.
 */
object SqlTypeUtil {
    //~ Methods ----------------------------------------------------------------
    /**
     * Checks whether two types or more are char comparable.
     *
     * @return Returns true if all operands are of char type and if they are
     * comparable, i.e. of the same charset and collation of same charset
     */
    fun isCharTypeComparable(argTypes: List<RelDataType?>?): Boolean {
        assert(argTypes != null)
        assert(argTypes!!.size() >= 2)

        // Filter out ANY and NULL elements.
        val argTypes2: List<RelDataType> = ArrayList()
        for (t in argTypes) {
            if (!isAny(t) && !isNull(t)) {
                argTypes2.add(t)
            }
        }
        for (pair in Pair.adjacents(argTypes2)) {
            val t0: RelDataType = pair.left
            val t1: RelDataType = pair.right
            if (!inCharFamily(t0) || !inCharFamily(t1)) {
                return false
            }
            if (!getCharset(t0).equals(getCharset(t1))) {
                return false
            }
            if (!getCollation(t0).getCharset().equals(
                    getCollation(t1).getCharset()
                )
            ) {
                return false
            }
        }
        return true
    }

    /**
     * Returns whether the operands to a call are char type-comparable.
     *
     * @param binding        Binding of call to operands
     * @param operands       Operands to check for compatibility; usually the
     * operands of the bound call, but not always
     * @param throwOnFailure Whether to throw an exception on failure
     * @return whether operands are valid
     */
    fun isCharTypeComparable(
        binding: SqlCallBinding,
        operands: List<SqlNode?>,
        throwOnFailure: Boolean
    ): Boolean {
        requireNonNull(operands, "operands")
        assert(operands.size() >= 2) { "operands.size() should be 2 or greater, actual: " + operands.size() }
        if (!isCharTypeComparable(deriveType(binding, operands))) {
            if (throwOnFailure) {
                val msg: String = String.join(", ", Util.transform(operands, String::valueOf))
                throw binding.newError(RESOURCE.operandNotComparable(msg))
            }
            return false
        }
        return true
    }

    /**
     * Iterates over all operands, derives their types, and collects them into
     * a list.
     */
    fun deriveAndCollectTypes(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operands: List<SqlNode?>
    ): List<RelDataType> {
        // NOTE: Do not use an AbstractList. Don't want to be lazy. We want
        // errors.
        val types: List<RelDataType> = ArrayList()
        for (operand in operands) {
            types.add(validator.deriveType(scope, operand))
        }
        return types
    }

    /**
     * Derives type of the call via its binding.
     * @param binding binding to derive the type from
     * @return datatype of the call
     */
    @API(since = "1.26", status = API.Status.EXPERIMENTAL)
    fun deriveType(binding: SqlCallBinding): RelDataType {
        return deriveType(binding, binding.getCall())
    }

    /**
     * Derives type of the given call under given binding.
     * @param binding binding to derive the type from
     * @param node node type to derive
     * @return datatype of the given node
     */
    @API(since = "1.26", status = API.Status.EXPERIMENTAL)
    fun deriveType(binding: SqlCallBinding, node: SqlNode?): RelDataType {
        return binding.getValidator().deriveType(
            requireNonNull(binding.getScope()) { "scope of $binding" }, node
        )
    }

    /**
     * Derives types for the list of nodes.
     * @param binding binding to derive the type from
     * @param nodes the list of nodes to derive types from
     * @return the list of types of the given nodes
     */
    @API(since = "1.26", status = API.Status.EXPERIMENTAL)
    fun deriveType(
        binding: SqlCallBinding,
        nodes: List<SqlNode?>
    ): List<RelDataType> {
        return deriveAndCollectTypes(
            binding.getValidator(),
            requireNonNull(binding.getScope()) { "scope of $binding" },
            nodes
        )
    }

    /**
     * Promotes a type to a row type (does nothing if it already is one).
     *
     * @param type      type to be promoted
     * @param fieldName name to give field in row type; null for default of
     * "ROW_VALUE"
     * @return row type
     */
    fun promoteToRowType(
        typeFactory: RelDataTypeFactory,
        type: RelDataType,
        @Nullable fieldName: String?
    ): RelDataType {
        var type: RelDataType = type
        var fieldName = fieldName
        if (!type.isStruct()) {
            if (fieldName == null) {
                fieldName = "ROW_VALUE"
            }
            type = typeFactory.builder().add(fieldName, type).build()
        }
        return type
    }

    /**
     * Recreates a given RelDataType with nullability iff any of the operands
     * of a call are nullable.
     */
    fun makeNullableIfOperandsAre(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        call: SqlCall,
        type: RelDataType?
    ): RelDataType? {
        var type: RelDataType? = type
        for (operand in call.getOperandList()) {
            val operandType: RelDataType = validator.deriveType(scope, operand)
            if (containsNullable(operandType)) {
                val typeFactory: RelDataTypeFactory = validator.getTypeFactory()
                type = typeFactory.createTypeWithNullability(type, true)
                break
            }
        }
        return type
    }

    /**
     * Recreates a given RelDataType with nullability iff any of the param
     * argTypes are nullable.
     */
    fun makeNullableIfOperandsAre(
        typeFactory: RelDataTypeFactory,
        argTypes: List<RelDataType?>?,
        type: RelDataType?
    ): RelDataType? {
        var type: RelDataType? = type
        requireNonNull(type, "type")
        if (containsNullable(argTypes)) {
            type = typeFactory.createTypeWithNullability(type, true)
        }
        return type
    }

    /**
     * Returns whether all of array of types are nullable.
     */
    fun allNullable(types: List<RelDataType?>): Boolean {
        for (type in types) {
            if (!containsNullable(type)) {
                return false
            }
        }
        return true
    }

    /**
     * Returns whether one or more of an array of types is nullable.
     */
    fun containsNullable(types: List<RelDataType?>): Boolean {
        for (type in types) {
            if (containsNullable(type)) {
                return true
            }
        }
        return false
    }

    /**
     * Determines whether a type or any of its fields (if a structured type) are
     * nullable.
     */
    fun containsNullable(type: RelDataType): Boolean {
        if (type.isNullable()) {
            return true
        }
        if (!type.isStruct()) {
            return false
        }
        for (field in type.getFieldList()) {
            if (containsNullable(field.getType())) {
                return true
            }
        }
        return false
    }

    /**
     * Returns typeName.equals(type.getSqlTypeName()). If
     * typeName.equals(SqlTypeName.Any) true is always returned.
     */
    fun isOfSameTypeName(
        typeName: SqlTypeName?,
        type: RelDataType
    ): Boolean {
        return (SqlTypeName.ANY === typeName
                || typeName === type.getSqlTypeName())
    }

    /**
     * Returns true if any element in `typeNames` matches
     * type.getSqlTypeName().
     *
     * @see .isOfSameTypeName
     */
    fun isOfSameTypeName(
        typeNames: Collection<SqlTypeName?>,
        type: RelDataType
    ): Boolean {
        for (typeName in typeNames) {
            if (isOfSameTypeName(typeName, type)) {
                return true
            }
        }
        return false
    }

    /** Returns whether a type is DATE, TIME, or TIMESTAMP.  */
    fun isDatetime(type: RelDataType): Boolean {
        return SqlTypeFamily.DATETIME.contains(type)
    }

    /** Returns whether a type is DATE.  */
    fun isDate(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return type.getSqlTypeName() === SqlTypeName.DATE
    }

    /** Returns whether a type is TIMESTAMP.  */
    fun isTimestamp(type: RelDataType): Boolean {
        return SqlTypeFamily.TIMESTAMP.contains(type)
    }

    /** Returns whether a type is some kind of INTERVAL.  */
    @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
    @EnsuresNonNullIf(expression = "#1.getIntervalQualifier()", result = true)
    fun isInterval(type: RelDataType): Boolean {
        return SqlTypeFamily.DATETIME_INTERVAL.contains(type)
    }

    /** Returns whether a type is in SqlTypeFamily.Character.  */
    @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
    @EnsuresNonNullIf(expression = "#1.getCharset()", result = true)
    @EnsuresNonNullIf(expression = "#1.getCollation()", result = true)
    fun inCharFamily(type: RelDataType): Boolean {
        return type.getFamily() === SqlTypeFamily.CHARACTER
    }

    /** Returns whether a type name is in SqlTypeFamily.Character.  */
    fun inCharFamily(typeName: SqlTypeName): Boolean {
        return typeName.getFamily() === SqlTypeFamily.CHARACTER
    }

    /** Returns whether a type is in SqlTypeFamily.Boolean.  */
    fun inBooleanFamily(type: RelDataType): Boolean {
        return type.getFamily() === SqlTypeFamily.BOOLEAN
    }

    /** Returns whether two types are in same type family.  */
    fun inSameFamily(t1: RelDataType, t2: RelDataType): Boolean {
        return t1.getFamily() === t2.getFamily()
    }

    /** Returns whether two types are in same type family, or one or the other is
     * of type [SqlTypeName.NULL].  */
    fun inSameFamilyOrNull(t1: RelDataType, t2: RelDataType): Boolean {
        return (t1.getSqlTypeName() === SqlTypeName.NULL
                || t2.getSqlTypeName() === SqlTypeName.NULL
                || t1.getFamily() === t2.getFamily())
    }

    /** Returns whether a type family is either character or binary.  */
    fun inCharOrBinaryFamilies(type: RelDataType): Boolean {
        return (type.getFamily() === SqlTypeFamily.CHARACTER
                || type.getFamily() === SqlTypeFamily.BINARY)
    }

    /** Returns whether a type is a LOB of some kind.  */
    fun isLob(type: RelDataType?): Boolean {
        // TODO jvs 9-Dec-2004:  once we support LOB types
        return false
    }

    /** Returns whether a type is variable width with bounded precision.  */
    fun isBoundedVariableWidth(type: RelDataType?): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return when (typeName) {
            VARCHAR, VARBINARY, MULTISET -> true
            else -> false
        }
    }

    /** Returns whether a type is one of the integer types.  */
    fun isIntType(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return when (typeName) {
            TINYINT, SMALLINT, INTEGER, BIGINT -> true
            else -> false
        }
    }

    /** Returns whether a type is DECIMAL.  */
    fun isDecimal(type: RelDataType?): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return typeName === SqlTypeName.DECIMAL
    }

    /** Returns whether a type is DOUBLE.  */
    fun isDouble(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return typeName === SqlTypeName.DOUBLE
    }

    /** Returns whether a type is BIGINT.  */
    fun isBigint(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return typeName === SqlTypeName.BIGINT
    }

    /** Returns whether a type is numeric with exact precision.  */
    fun isExactNumeric(type: RelDataType?): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return when (typeName) {
            TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> true
            else -> false
        }
    }

    /** Returns whether a type's scale is set.  */
    fun hasScale(type: RelDataType): Boolean {
        return type.getScale() !== Integer.MIN_VALUE
    }

    /** Returns the maximum value of an integral type, as a long value.  */
    fun maxValue(type: RelDataType): Long {
        assert(isIntType(type))
        return when (type.getSqlTypeName()) {
            TINYINT -> Byte.MAX_VALUE.toLong()
            SMALLINT -> Short.MAX_VALUE.toLong()
            INTEGER -> Integer.MAX_VALUE
            BIGINT -> Long.MAX_VALUE
            else -> throw Util.unexpected(type.getSqlTypeName())
        }
    }

    /** Returns whether a type is numeric with approximate precision.  */
    fun isApproximateNumeric(type: RelDataType?): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return when (typeName) {
            FLOAT, REAL, DOUBLE -> true
            else -> false
        }
    }

    /** Returns whether a type is numeric.  */
    fun isNumeric(type: RelDataType?): Boolean {
        return isExactNumeric(type) || isApproximateNumeric(type)
    }

    /** Returns whether a type is the NULL type.  */
    fun isNull(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return typeName === SqlTypeName.NULL
    }

    /**
     * Tests whether two types have the same name and structure, possibly with
     * differing modifiers. For example, VARCHAR(1) and VARCHAR(10) are
     * considered the same, while VARCHAR(1) and CHAR(1) are considered
     * different. Likewise, VARCHAR(1) MULTISET and VARCHAR(10) MULTISET are
     * considered the same.
     *
     * @return true if types have same name and structure
     */
    fun sameNamedType(t1: RelDataType, t2: RelDataType): Boolean {
        if (t1.isStruct() || t2.isStruct()) {
            if (!t1.isStruct() || !t2.isStruct()) {
                return false
            }
            if (t1.getFieldCount() !== t2.getFieldCount()) {
                return false
            }
            val fields1: List<RelDataTypeField> = t1.getFieldList()
            val fields2: List<RelDataTypeField> = t2.getFieldList()
            for (i in 0 until fields1.size()) {
                if (!sameNamedType(
                        fields1[i].getType(),
                        fields2[i].getType()
                    )
                ) {
                    return false
                }
            }
            return true
        }
        val comp1: RelDataType = t1.getComponentType()
        val comp2: RelDataType = t2.getComponentType()
        if (comp1 != null || comp2 != null) {
            if (comp1 == null || comp2 == null) {
                return false
            }
            if (!sameNamedType(comp1, comp2)) {
                return false
            }
        }
        return t1.getSqlTypeName() === t2.getSqlTypeName()
    }

    /**
     * Computes the maximum number of bytes required to represent a value of a
     * type having user-defined precision. This computation assumes no overhead
     * such as length indicators and NUL-terminators. Complex types for which
     * multiple representations are possible (e.g. DECIMAL or TIMESTAMP) return
     * 0.
     *
     * @param type type for which to compute storage
     * @return maximum bytes, or 0 for a fixed-width type or type with unknown
     * maximum
     */
    fun getMaxByteSize(type: RelDataType): Int {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return 0
        return when (typeName) {
            CHAR, VARCHAR -> Math.ceil(
                type.getPrecision() as Double
                        * getCharset(type).newEncoder().maxBytesPerChar()
            )
            BINARY, VARBINARY -> type.getPrecision()
            MULTISET ->
                // TODO Wael Jan-24-2005: Need a better way to tell fennel this
                // number. This a very generic place and implementation details like
                // this doesnt belong here. Waiting to change this once we have blob
                // support
                4096
            else -> 0
        }
    }

    /** Returns the minimum unscaled value of a numeric type.
     *
     * @param type a numeric type
     */
    fun getMinValue(type: RelDataType): Long {
        val typeName: SqlTypeName = type.getSqlTypeName()
        return when (typeName) {
            TINYINT -> Byte.MIN_VALUE.toLong()
            SMALLINT -> Short.MIN_VALUE.toLong()
            INTEGER -> Integer.MIN_VALUE
            BIGINT, DECIMAL -> NumberUtil.getMinUnscaled(type.getPrecision()).longValue()
            else -> throw AssertionError("getMinValue($typeName)")
        }
    }

    /** Returns the maximum unscaled value of a numeric type.
     *
     * @param type a numeric type
     */
    fun getMaxValue(type: RelDataType): Long {
        val typeName: SqlTypeName = type.getSqlTypeName()
        return when (typeName) {
            TINYINT -> Byte.MAX_VALUE.toLong()
            SMALLINT -> Short.MAX_VALUE.toLong()
            INTEGER -> Integer.MAX_VALUE
            BIGINT, DECIMAL -> NumberUtil.getMaxUnscaled(type.getPrecision()).longValue()
            else -> throw AssertionError("getMaxValue($typeName)")
        }
    }

    /** Returns whether a type has a representation as a Java primitive (ignoring
     * nullability).  */
    @Deprecated // to be removed before 2.0
    fun isJavaPrimitive(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return when (typeName) {
            BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE, SYMBOL -> true
            else -> false
        }
    }

    /** Returns the class name of the wrapper for the primitive data type.  */
    @Deprecated // to be removed before 2.0
    @Nullable
    fun getPrimitiveWrapperJavaClassName(@Nullable type: RelDataType?): String? {
        if (type == null) {
            return null
        }
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return null
        return when (typeName) {
            BOOLEAN -> "Boolean"
            else -> getNumericJavaClassName(type)
        }
    }

    /** Returns the class name of a numeric data type.  */
    @Deprecated // to be removed before 2.0
    @Nullable
    fun getNumericJavaClassName(@Nullable type: RelDataType?): String? {
        if (type == null) {
            return null
        }
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return null
        return when (typeName) {
            TINYINT -> "Byte"
            SMALLINT -> "Short"
            INTEGER -> "Integer"
            BIGINT -> "Long"
            REAL -> "Float"
            DECIMAL, FLOAT, DOUBLE -> "Double"
            else -> null
        }
    }

    private fun isAny(t: RelDataType): Boolean {
        return t.getFamily() === SqlTypeFamily.ANY
    }

    /**
     * Tests whether a value can be assigned to a site.
     *
     * @param toType   type of the target site
     * @param fromType type of the source value
     * @return true iff assignable
     */
    fun canAssignFrom(
        toType: RelDataType,
        fromType: RelDataType
    ): Boolean {
        if (isAny(toType) || isAny(fromType)) {
            return true
        }

        // TODO jvs 2-Jan-2005:  handle all the other cases like
        // rows, collections, UDT's
        if (fromType.getSqlTypeName() === SqlTypeName.NULL) {
            // REVIEW jvs 4-Dec-2008: We allow assignment from NULL to any
            // type, including NOT NULL types, since in the case where no
            // rows are actually processed, the assignment is legal
            // (FRG-365).  However, it would be better if the validator's
            // NULL type inference guaranteed that we had already
            // assigned a real (nullable) type to every NULL literal.
            return true
        }
        if (fromType.getSqlTypeName() === SqlTypeName.ARRAY) {
            return if (toType.getSqlTypeName() !== SqlTypeName.ARRAY) {
                false
            } else canAssignFrom(
                getComponentTypeOrThrow(toType),
                getComponentTypeOrThrow(fromType)
            )
        }
        return if (areCharacterSetsMismatched(toType, fromType)) {
            false
        } else toType.getFamily() === fromType.getFamily()
    }

    /**
     * Determines whether two types both have different character sets. If one
     * or the other type has no character set (e.g. in cast from INT to
     * VARCHAR), that is not a mismatch.
     *
     * @param t1 first type
     * @param t2 second type
     * @return true iff mismatched
     */
    fun areCharacterSetsMismatched(
        t1: RelDataType,
        t2: RelDataType
    ): Boolean {
        if (isAny(t1) || isAny(t2)) {
            return false
        }
        val cs1: Charset = t1.getCharset()
        val cs2: Charset = t2.getCharset()
        if (cs1 != null && cs2 != null) {
            if (!cs1.equals(cs2)) {
                return true
            }
        }
        return false
    }

    /**
     * Compares two types and returns true if fromType can be cast to toType.
     *
     *
     * REVIEW jvs 17-Dec-2004: the coerce param below shouldn't really be
     * necessary. We're using it as a hack because
     * [SqlTypeFactoryImpl.leastRestrictive] isn't complete enough
     * yet.  Once it is, this param (and the non-coerce rules of
     * [SqlTypeAssignmentRule]) should go away.
     *
     * @param toType   target of assignment
     * @param fromType source of assignment
     * @param coerce   if true, the SQL rules for CAST are used; if false, the
     * rules are similar to Java; e.g. you can't assign short x =
     * (int) y, and you can't assign int x = (String) z.
     * @return true iff cast is legal
     */
    fun canCastFrom(
        toType: RelDataType,
        fromType: RelDataType,
        coerce: Boolean
    ): Boolean {
        if (toType.equals(fromType)) {
            return true
        }
        if (isAny(toType) || isAny(fromType)) {
            return true
        }
        val fromTypeName: SqlTypeName = fromType.getSqlTypeName()
        val toTypeName: SqlTypeName = toType.getSqlTypeName()
        if (toType.isStruct() || fromType.isStruct()) {
            return if (toTypeName === SqlTypeName.DISTINCT) {
                if (fromTypeName === SqlTypeName.DISTINCT) {
                    // can't cast between different distinct types
                    false
                } else canCastFrom(
                    toType.getFieldList().get(0).getType(), fromType, coerce
                )
            } else if (fromTypeName === SqlTypeName.DISTINCT) {
                canCastFrom(
                    toType, fromType.getFieldList().get(0).getType(), coerce
                )
            } else if (toTypeName === SqlTypeName.ROW) {
                if (fromTypeName !== SqlTypeName.ROW) {
                    return fromTypeName === SqlTypeName.NULL
                }
                val n: Int = toType.getFieldCount()
                if (fromType.getFieldCount() !== n) {
                    return false
                }
                for (i in 0 until n) {
                    val toField: RelDataTypeField = toType.getFieldList().get(i)
                    val fromField: RelDataTypeField = fromType.getFieldList().get(i)
                    if (!canCastFrom(
                            toField.getType(),
                            fromField.getType(),
                            coerce
                        )
                    ) {
                        return false
                    }
                }
                true
            } else if (toTypeName === SqlTypeName.MULTISET) {
                if (!fromType.isStruct()) {
                    return false
                }
                if (fromTypeName !== SqlTypeName.MULTISET) {
                    false
                } else canCastFrom(
                    getComponentTypeOrThrow(toType),
                    getComponentTypeOrThrow(fromType),
                    coerce
                )
            } else if (fromTypeName === SqlTypeName.MULTISET) {
                false
            } else {
                toType.getFamily() === fromType.getFamily()
            }
        }
        val c1: RelDataType = toType.getComponentType()
        if (c1 != null) {
            val c2: RelDataType = fromType.getComponentType() ?: return false
            return canCastFrom(c1, c2, coerce)
        }
        if (isInterval(fromType) && isExactNumeric(toType)
            || isInterval(toType) && isExactNumeric(fromType)
        ) {
            if (!(if (isInterval(fromType)) fromType else toType).getIntervalQualifier().isSingleDatetimeField()) {
                // Casts between intervals and exact numerics must involve
                // intervals with a single datetime field.
                return false
            }
        }
        if (toTypeName == null || fromTypeName == null) {
            return false
        }

        // REVIEW jvs 9-Feb-2009: we don't impose SQL rules for character sets
        // here; instead, we do that in SqlCastFunction.  The reason is that
        // this method is called from at least one place (MedJdbcNameDirectory)
        // where internally a cast across character repertoires is OK.  Should
        // probably clean that up.
        val rules: SqlTypeMappingRule = SqlTypeMappingRules.instance(coerce)
        return rules!!.canApplyFrom(toTypeName, fromTypeName)
    }

    /**
     * Flattens a record type by recursively expanding any fields which are
     * themselves record types. For each record type, a representative null
     * value field is also prepended (with state NULL for a null value and FALSE
     * for non-null), and all component types are asserted to be nullable, since
     * SQL doesn't allow NOT NULL to be specified on attributes.
     *
     * @param typeFactory   factory which should produced flattened type
     * @param recordType    type with possible nesting
     * @param flatteningMap if non-null, receives map from unflattened ordinal
     * to flattened ordinal (must have length at least
     * recordType.getFieldList().size())
     * @return flattened equivalent
     */
    fun flattenRecordType(
        typeFactory: RelDataTypeFactory,
        recordType: RelDataType,
        flatteningMap: @Nullable IntArray?
    ): RelDataType {
        if (!recordType.isStruct()) {
            return recordType
        }
        val fieldList: List<RelDataTypeField> = ArrayList()
        val nested = flattenFields(
            typeFactory,
            recordType,
            fieldList,
            flatteningMap
        )
        if (!nested) {
            return recordType
        }
        val types: List<RelDataType> = ArrayList()
        val fieldNames: List<String> = ArrayList()
        val fieldCnt: Map<String, Long> = fieldList.stream()
            .map(RelDataTypeField::getName)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        var i = -1
        for (field in fieldList) {
            ++i
            types.add(field.getType())
            val oriFieldName: String = field.getName()
            // Patch up the field name with index if there are duplicates.
            // There is still possibility that the patched name conflicts with existing ones,
            // but that should be rare case.
            val fieldCount = fieldCnt[oriFieldName]
            val fieldName = if (fieldCount != null && fieldCount > 1) oriFieldName + "_" + i else oriFieldName
            fieldNames.add(fieldName)
        }
        return typeFactory.createStructType(types, fieldNames)
    }

    fun needsNullIndicator(recordType: RelDataType): Boolean {
        // NOTE jvs 9-Mar-2005: It would be more storage-efficient to say that
        // no null indicator is required for structured type columns declared
        // as NOT NULL.  However, the uniformity of always having a null
        // indicator makes things cleaner in many places.
        return recordType.getSqlTypeName() === SqlTypeName.STRUCTURED
    }

    private fun flattenFields(
        typeFactory: RelDataTypeFactory,
        type: RelDataType,
        list: List<RelDataTypeField>,
        flatteningMap: @Nullable IntArray?
    ): Boolean {
        var nested = false
        for (field in type.getFieldList()) {
            if (flatteningMap != null) {
                flatteningMap[field.getIndex()] = list.size()
            }
            if (field.getType().isStruct()) {
                nested = true
                flattenFields(
                    typeFactory,
                    field.getType(),
                    list,
                    null
                )
            } else if (field.getType().getComponentType() != null) {
                nested = true

                // TODO jvs 14-Feb-2005:  generalize to any kind of
                // collection type
                var flattenedCollectionType: RelDataType = typeFactory.createMultisetType(
                    flattenRecordType(
                        typeFactory,
                        getComponentTypeOrThrow(field.getType()),
                        null
                    ),
                    -1
                )
                if (field.getType() is ArraySqlType) {
                    flattenedCollectionType = typeFactory.createArrayType(
                        flattenRecordType(
                            typeFactory,
                            getComponentTypeOrThrow(field.getType()),
                            null
                        ),
                        -1
                    )
                }
                field = RelDataTypeFieldImpl(
                    field.getName(),
                    field.getIndex(),
                    flattenedCollectionType
                )
                list.add(field)
            } else {
                list.add(field)
            }
        }
        return nested
    }

    /**
     * Converts an instance of RelDataType to an instance of SqlDataTypeSpec.
     *
     * @param type         type descriptor
     * @param charSetName  charSet name
     * @param maxPrecision The max allowed precision.
     * @param maxScale     max allowed scale
     * @return corresponding parse representation
     */
    fun convertTypeToSpec(
        type: RelDataType,
        @Nullable charSetName: String?, maxPrecision: Int, maxScale: Int
    ): SqlDataTypeSpec {
        val typeName: SqlTypeName = type.getSqlTypeName()
        assert(typeName != null)
        val typeNameSpec: SqlTypeNameSpec
        if (isAtomic(type) || isNull(type)) {
            var precision = if (typeName!!.allowsPrec()) type.getPrecision() else -1
            // fix up the precision.
            if (maxPrecision > 0 && precision > maxPrecision) {
                precision = maxPrecision
            }
            var scale = if (typeName!!.allowsScale()) type.getScale() else -1
            if (maxScale > 0 && scale > maxScale) {
                scale = maxScale
            }
            typeNameSpec = SqlBasicTypeNameSpec(
                typeName,
                precision,
                scale,
                charSetName,
                SqlParserPos.ZERO
            )
        } else if (isCollection(type)) {
            typeNameSpec = SqlCollectionTypeNameSpec(
                convertTypeToSpec(getComponentTypeOrThrow(type)).getTypeNameSpec(),
                typeName,
                SqlParserPos.ZERO
            )
        } else if (isRow(type)) {
            val recordType: RelRecordType = type as RelRecordType
            val fields: List<RelDataTypeField> = recordType.getFieldList()
            val fieldNames: List<SqlIdentifier> = fields.stream()
                .map { f -> SqlIdentifier(f.getName(), SqlParserPos.ZERO) }
                .collect(Collectors.toList())
            val fieldTypes: List<SqlDataTypeSpec> = fields.stream()
                .map { f -> convertTypeToSpec(f.getType()) }
                .collect(Collectors.toList())
            typeNameSpec = SqlRowTypeNameSpec(SqlParserPos.ZERO, fieldNames, fieldTypes)
        } else {
            throw UnsupportedOperationException(
                "Unsupported type when convertTypeToSpec: $typeName"
            )
        }

        // REVIEW jvs 28-Dec-2004:  discriminate between precision/scale
        // zero and unspecified?

        // REVIEW angel 11-Jan-2006:
        // Use neg numbers to indicate unspecified precision/scale
        return SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO)
    }

    /**
     * Converts an instance of RelDataType to an instance of SqlDataTypeSpec.
     *
     * @param type type descriptor
     * @return corresponding parse representation
     */
    fun convertTypeToSpec(type: RelDataType): SqlDataTypeSpec {
        // TODO jvs 28-Dec-2004:  collation
        val charSetName: String? = if (inCharFamily(type)) type.getCharset().name() else null
        return convertTypeToSpec(type, charSetName, -1, -1)
    }

    fun createMultisetType(
        typeFactory: RelDataTypeFactory,
        type: RelDataType?,
        nullable: Boolean
    ): RelDataType {
        val ret: RelDataType = typeFactory.createMultisetType(type, -1)
        return typeFactory.createTypeWithNullability(ret, nullable)
    }

    fun createArrayType(
        typeFactory: RelDataTypeFactory,
        type: RelDataType?,
        nullable: Boolean
    ): RelDataType {
        val ret: RelDataType = typeFactory.createArrayType(type, -1)
        return typeFactory.createTypeWithNullability(ret, nullable)
    }

    fun createMapType(
        typeFactory: RelDataTypeFactory,
        keyType: RelDataType?,
        valueType: RelDataType?,
        nullable: Boolean
    ): RelDataType {
        val ret: RelDataType = typeFactory.createMapType(keyType, valueType)
        return typeFactory.createTypeWithNullability(ret, nullable)
    }

    /** Creates a MAP type from a record type. The record type must have exactly
     * two fields.  */
    fun createMapTypeFromRecord(
        typeFactory: RelDataTypeFactory, type: RelDataType
    ): RelDataType {
        Preconditions.checkArgument(
            type.getFieldCount() === 2,
            "MAP requires exactly two fields, got %s; row type %s",
            type.getFieldCount(), type
        )
        return createMapType(
            typeFactory, type.getFieldList().get(0).getType(),
            type.getFieldList().get(1).getType(), false
        )
    }

    /**
     * Adds collation and charset to a character type, returns other types
     * unchanged.
     *
     * @param type        Type
     * @param typeFactory Type factory
     * @return Type with added charset and collation, or unchanged type if it is
     * not a char type.
     */
    fun addCharsetAndCollation(
        type: RelDataType,
        typeFactory: RelDataTypeFactory
    ): RelDataType {
        var type: RelDataType = type
        if (!inCharFamily(type)) {
            return type
        }
        var charset: Charset = type.getCharset()
        if (charset == null) {
            charset = typeFactory.getDefaultCharset()
        }
        var collation: SqlCollation = type.getCollation()
        if (collation == null) {
            collation = SqlCollation.IMPLICIT
        }

        // todo: should get the implicit collation from repository
        //   instead of null
        type = typeFactory.createTypeWithCharsetAndCollation(
            type,
            charset,
            collation
        )
        SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type)
        return type
    }

    /**
     * Returns whether two types are equal, ignoring nullability.
     *
     *
     * They need not come from the same factory.
     *
     * @param factory Type factory
     * @param type1   First type
     * @param type2   Second type
     * @return whether types are equal, ignoring nullability
     */
    fun equalSansNullability(
        factory: RelDataTypeFactory,
        type1: RelDataType?,
        type2: RelDataType?
    ): Boolean {
        return if (type1.isNullable() === type2.isNullable()) {
            type1.equals(type2)
        } else type1.equals(
            factory.createTypeWithNullability(type2, type1.isNullable())
        )
    }

    /**
     * This is a poorman's
     * [.equalSansNullability].
     *
     *
     * We assume that "not null" is represented in the type's digest
     * as a trailing "NOT NULL" (case sensitive).
     *
     *
     * If you got a type factory, [.equalSansNullability]
     * is preferred.
     *
     * @param type1 First type
     * @param type2 Second type
     * @return true if the types are equal or the only difference is nullability
     */
    fun equalSansNullability(type1: RelDataType, type2: RelDataType): Boolean {
        if (type1 === type2) {
            return true
        }
        var x: String = type1.getFullTypeString()
        var y: String = type2.getFullTypeString()
        if (x.length() < y.length()) {
            val c = x
            x = y
            y = c
        }
        return (x.length() === y.length()
                || x.length() === y.length() + NON_NULLABLE_SUFFIX.length()
                && x.endsWith(NON_NULLABLE_SUFFIX)) && x.startsWith(y)
    }

    /**
     * Returns whether two collection types are equal, ignoring nullability.
     *
     *
     * They need not come from the same factory.
     *
     * @param factory       Type factory
     * @param type1         First type
     * @param type2         Second type
     * @return Whether types are equal, ignoring nullability
     */
    fun equalAsCollectionSansNullability(
        factory: RelDataTypeFactory,
        type1: RelDataType,
        type2: RelDataType
    ): Boolean {
        Preconditions.checkArgument(
            isCollection(type1),
            "Input type1 must be collection type"
        )
        Preconditions.checkArgument(
            isCollection(type2),
            "Input type2 must be collection type"
        )
        return (type1 === type2
                || (type1.getSqlTypeName() === type2.getSqlTypeName()
                && equalSansNullability(
            factory, getComponentTypeOrThrow(type1), getComponentTypeOrThrow(type2)
        )))
    }

    /**
     * Returns whether two map types are equal, ignoring nullability.
     *
     *
     * They need not come from the same factory.
     *
     * @param factory       Type factory
     * @param type1         First type
     * @param type2         Second type
     * @return Whether types are equal, ignoring nullability
     */
    fun equalAsMapSansNullability(
        factory: RelDataTypeFactory,
        type1: RelDataType,
        type2: RelDataType
    ): Boolean {
        Preconditions.checkArgument(isMap(type1), "Input type1 must be map type")
        Preconditions.checkArgument(isMap(type2), "Input type2 must be map type")
        val mType1: MapSqlType = type1
        val mType2: MapSqlType = type2
        return (type1 === type2
                || (equalSansNullability(factory, mType1.getKeyType(), mType2.getKeyType())
                && equalSansNullability(factory, mType1.getValueType(), mType2.getValueType())))
    }

    /**
     * Returns whether two struct types are equal, ignoring nullability.
     *
     *
     * They do not need to come from the same factory.
     *
     * @param factory       Type factory
     * @param type1         First type
     * @param type2         Second type
     * @param nameMatcher   Name matcher used to compare the field names, if null,
     * the field names are also ignored
     *
     * @return Whether types are equal, ignoring nullability
     */
    fun equalAsStructSansNullability(
        factory: RelDataTypeFactory,
        type1: RelDataType,
        type2: RelDataType,
        @Nullable nameMatcher: SqlNameMatcher?
    ): Boolean {
        Preconditions.checkArgument(type1.isStruct(), "Input type1 must be struct type")
        Preconditions.checkArgument(type2.isStruct(), "Input type2 must be struct type")
        if (type1 === type2) {
            return true
        }
        if (type1.getFieldCount() !== type2.getFieldCount()) {
            return false
        }
        for (pair in Pair.zip(type1.getFieldList(), type2.getFieldList())) {
            if (nameMatcher != null
                && !nameMatcher.matches(pair.left.getName(), pair.right.getName())
            ) {
                return false
            }
            if (!equalSansNullability(factory, pair.left.getType(), pair.right.getType())) {
                return false
            }
        }
        return true
    }

    /**
     * Returns the ordinal of a given field in a record type, or -1 if the field
     * is not found.
     *
     *
     * The `fieldName` is always simple, if the field is nested within a record field,
     * returns index of the outer field instead. i.g. for row type
     * (a int, b (b1 bigint, b2 varchar(20) not null)), returns 1 for both simple name "b1" and "b2".
     *
     * @param type      Record type
     * @param fieldName Name of field
     * @return Ordinal of field
     */
    fun findField(type: RelDataType, fieldName: String?): Int {
        val fields: List<RelDataTypeField> = type.getFieldList()
        for (i in 0 until fields.size()) {
            val field: RelDataTypeField = fields[i]
            if (field.getName().equals(fieldName)) {
                return i
            }
            val fieldType: RelDataType = field.getType()
            if (fieldType.isStruct() && findField(fieldType, fieldName) != -1) {
                return i
            }
        }
        return -1
    }

    /**
     * Selects data types of the specified fields from an input row type.
     * This is useful when identifying data types of a function that is going
     * to operate on inputs that are specified as field ordinals (e.g.
     * aggregate calls).
     *
     * @param rowType input row type
     * @param requiredFields ordinals of the projected fields
     * @return list of data types that are requested by requiredFields
     */
    fun projectTypes(
        rowType: RelDataType,
        requiredFields: List<Number?>
    ): List<RelDataType> {
        val fields: List<RelDataTypeField> = rowType.getFieldList()
        return object : AbstractList<RelDataType?>() {
            @Override
            operator fun get(index: Int): RelDataType {
                return fields[requiredFields[index].intValue()].getType()
            }

            @Override
            fun size(): Int {
                return requiredFields.size()
            }
        }
    }

    /**
     * Records a struct type with no fields.
     *
     * @param typeFactory Type factory
     * @return Struct type with no fields
     */
    fun createEmptyStructType(
        typeFactory: RelDataTypeFactory
    ): RelDataType {
        return typeFactory.createStructType(
            ImmutableList.of(),
            ImmutableList.of()
        )
    }

    /** Returns whether a type is flat. It is not flat if it is a record type that
     * has one or more fields that are themselves record types.  */
    fun isFlat(type: RelDataType): Boolean {
        if (type.isStruct()) {
            for (field in type.getFieldList()) {
                if (field.getType().isStruct()) {
                    return false
                }
            }
        }
        return true
    }

    /**
     * Returns whether two types are comparable. They need to be scalar types of
     * the same family, or struct types whose fields are pairwise comparable.
     *
     * @param type1 First type
     * @param type2 Second type
     * @return Whether types are comparable
     */
    fun isComparable(type1: RelDataType?, type2: RelDataType?): Boolean {
        if (type1.isStruct() !== type2.isStruct()) {
            return false
        }
        if (type1.isStruct()) {
            val n: Int = type1.getFieldCount()
            if (n != type2.getFieldCount()) {
                return false
            }
            for (pair in Pair.zip(type1.getFieldList(), type2.getFieldList())) {
                if (!isComparable(pair.left.getType(), pair.right.getType())) {
                    return false
                }
            }
            return true
        }
        val family1: RelDataTypeFamily? = family(type1)
        val family2: RelDataTypeFamily? = family(type2)
        if (family1 === family2) {
            return true
        }

        // If one of the arguments is of type 'ANY', return true.
        if (family1 === SqlTypeFamily.ANY
            || family2 === SqlTypeFamily.ANY
        ) {
            return true
        }

        // If one of the arguments is of type 'NULL', return true.
        if (family1 === SqlTypeFamily.NULL
            || family2 === SqlTypeFamily.NULL
        ) {
            return true
        }

        // We can implicitly convert from character to date
        return if (family1 === SqlTypeFamily.CHARACTER
            && canConvertStringInCompare(family2)
            || family2 === SqlTypeFamily.CHARACTER
            && canConvertStringInCompare(family1)
        ) {
            true
        } else false
    }

    /** Returns the least restrictive type T, such that a value of type T can be
     * compared with values of type `type1` and `type2` using
     * `=`.  */
    @Nullable
    fun leastRestrictiveForComparison(
        typeFactory: RelDataTypeFactory, type1: RelDataType?, type2: RelDataType?
    ): RelDataType? {
        val type: RelDataType = typeFactory.leastRestrictive(ImmutableList.of(type1, type2))
        if (type != null) {
            return type
        }
        val family1: RelDataTypeFamily? = family(type1)
        val family2: RelDataTypeFamily? = family(type2)

        // If one of the arguments is of type 'ANY', we can compare.
        if (family1 === SqlTypeFamily.ANY) {
            return type2
        }
        if (family2 === SqlTypeFamily.ANY) {
            return type1
        }

        // If one of the arguments is of type 'NULL', we can compare.
        if (family1 === SqlTypeFamily.NULL) {
            return type2
        }
        if (family2 === SqlTypeFamily.NULL) {
            return type1
        }

        // We can implicitly convert from character to date, numeric, etc.
        if (family1 === SqlTypeFamily.CHARACTER
            && canConvertStringInCompare(family2)
        ) {
            return type2
        }
        return if (family2 === SqlTypeFamily.CHARACTER
            && canConvertStringInCompare(family1)
        ) {
            type1
        } else null
    }

    internal fun family(type: RelDataType?): RelDataTypeFamily? {
        // REVIEW jvs 2-June-2005:  This is needed to keep
        // the Saffron type system happy.
        var family: RelDataTypeFamily? = null
        if (type.getSqlTypeName() != null) {
            family = type.getSqlTypeName().getFamily()
        }
        if (family == null) {
            family = type.getFamily()
        }
        return family
    }

    /**
     * Returns whether all types in a collection have the same family, as
     * determined by [.isSameFamily].
     *
     * @param types Types to check
     * @return true if all types are of the same family
     */
    fun areSameFamily(types: Iterable<RelDataType?>?): Boolean {
        val typeList: List<RelDataType> = ImmutableList.copyOf(types)
        if (Sets.newHashSet(RexUtil.families(typeList)).size() < 2) {
            return true
        }
        for (adjacent in Pair.adjacents(typeList)) {
            if (!isSameFamily(adjacent.left, adjacent.right)) {
                return false
            }
        }
        return true
    }

    /**
     * Returns whether two types are scalar types of the same family, or struct types whose fields
     * are pairwise of the same family.
     *
     * @param type1 First type
     * @param type2 Second type
     * @return Whether types have the same family
     */
    private fun isSameFamily(type1: RelDataType, type2: RelDataType): Boolean {
        if (type1.isStruct() !== type2.isStruct()) {
            return false
        }
        if (type1.isStruct()) {
            val n: Int = type1.getFieldCount()
            if (n != type2.getFieldCount()) {
                return false
            }
            for (pair in Pair.zip(type1.getFieldList(), type2.getFieldList())) {
                if (!isSameFamily(pair.left.getType(), pair.right.getType())) {
                    return false
                }
            }
            return true
        }
        val family1: RelDataTypeFamily? = family(type1)
        val family2: RelDataTypeFamily? = family(type2)
        return family1 === family2
    }

    /** Returns whether a character data type can be implicitly converted to a
     * given family in a compare operation.  */
    private fun canConvertStringInCompare(family: RelDataTypeFamily?): Boolean {
        if (family is SqlTypeFamily) {
            when (family) {
                DATE, TIME, TIMESTAMP, INTERVAL_DAY_TIME, INTERVAL_YEAR_MONTH, NUMERIC, APPROXIMATE_NUMERIC, EXACT_NUMERIC, INTEGER, BOOLEAN -> return true
                else -> {}
            }
        }
        return false
    }

    /**
     * Checks whether a type represents Unicode character data.
     *
     * @param type type to test
     * @return whether type represents Unicode character data
     */
    fun isUnicode(type: RelDataType): Boolean {
        val charset: Charset = type.getCharset() ?: return false
        return charset.name().startsWith("UTF")
    }

    /** Returns the larger of two precisions, treating
     * [RelDataType.PRECISION_NOT_SPECIFIED] as infinity.  */
    fun maxPrecision(p0: Int, p1: Int): Int {
        return if (p0 == RelDataType.PRECISION_NOT_SPECIFIED
            || p0 >= p1
            && p1 != RelDataType.PRECISION_NOT_SPECIFIED
        ) p0 else p1
    }

    /** Returns whether a precision is greater or equal than another,
     * treating [RelDataType.PRECISION_NOT_SPECIFIED] as infinity.  */
    fun comparePrecision(p0: Int, p1: Int): Int {
        if (p0 == p1) {
            return 0
        }
        if (p0 == RelDataType.PRECISION_NOT_SPECIFIED) {
            return 1
        }
        return if (p1 == RelDataType.PRECISION_NOT_SPECIFIED) {
            -1
        } else Integer.compare(p0, p1)
    }

    /** Returns whether a type is ARRAY.  */
    fun isArray(type: RelDataType): Boolean {
        return type.getSqlTypeName() === SqlTypeName.ARRAY
    }

    /** Returns whether a type is ROW.  */
    fun isRow(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return type.getSqlTypeName() === SqlTypeName.ROW
    }

    /** Returns whether a type is MAP.  */
    fun isMap(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return type.getSqlTypeName() === SqlTypeName.MAP
    }

    /** Returns whether a type is MULTISET.  */
    fun isMultiset(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return type.getSqlTypeName() === SqlTypeName.MULTISET
    }

    /** Returns whether a type is ARRAY or MULTISET.  */
    fun isCollection(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return (type.getSqlTypeName() === SqlTypeName.ARRAY
                || type.getSqlTypeName() === SqlTypeName.MULTISET)
    }

    /** Returns whether a type is CHARACTER.  */
    fun isCharacter(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return SqlTypeFamily.CHARACTER.contains(type)
    }

    /** Returns whether a type is a CHARACTER or contains a CHARACTER type.
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #hasCharacter(RelDataType)} ")
    fun hasCharactor(type: RelDataType): Boolean {
        return hasCharacter(type)
    }

    /** Returns whether a type is a CHARACTER or contains a CHARACTER type.  */
    fun hasCharacter(type: RelDataType): Boolean {
        if (isCharacter(type)) {
            return true
        }
        return if (isArray(type)) {
            hasCharacter(getComponentTypeOrThrow(type))
        } else false
    }

    /** Returns whether a type is STRING.  */
    fun isString(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return SqlTypeFamily.STRING.contains(type)
    }

    /** Returns whether a type is BOOLEAN.  */
    fun isBoolean(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return SqlTypeFamily.BOOLEAN.contains(type)
    }

    /** Returns whether a type is BINARY.  */
    fun isBinary(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return SqlTypeFamily.BINARY.contains(type)
    }

    /** Returns whether a type is atomic (datetime, numeric, string or
     * BOOLEAN).  */
    fun isAtomic(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName() ?: return false
        return (isDatetime(type)
                || isNumeric(type)
                || isString(type)
                || isBoolean(type))
    }

    /** Returns a DECIMAL type with the maximum precision for the current
     * type system.  */
    fun getMaxPrecisionScaleDecimal(factory: RelDataTypeFactory): RelDataType {
        val maxPrecision: Int = factory.getTypeSystem().getMaxNumericPrecision()
        // scale should not greater than precision.
        val scale = maxPrecision / 2
        return factory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, scale)
    }

    /**
     * Keeps only the last N fields and returns the new struct type.
     */
    fun extractLastNFields(
        typeFactory: RelDataTypeFactory,
        type: RelDataType, numToKeep: Int
    ): RelDataType {
        assert(type.isStruct())
        assert(type.getFieldCount() >= numToKeep)
        val fieldsCnt: Int = type.getFieldCount()
        return typeFactory.createStructType(
            type.getFieldList().subList(fieldsCnt - numToKeep, fieldsCnt)
        )
    }

    /**
     * Returns whether the decimal value is valid for the type. For example, 1111.11 is not
     * valid for DECIMAL(3, 1) since it overflows.
     *
     * @param value Value of literal
     * @param toType Type of the literal
     * @return whether the value is valid for the type
     */
    fun isValidDecimalValue(@Nullable value: BigDecimal?, toType: RelDataType): Boolean {
        return if (value == null) {
            true
        } else when (toType.getSqlTypeName()) {
            DECIMAL -> {
                val intDigits: Int = value.precision() - value.scale()
                val maxIntDigits: Int = toType.getPrecision() - toType.getScale()
                intDigits <= maxIntDigits
            }
            else -> true
        }
    }
}
