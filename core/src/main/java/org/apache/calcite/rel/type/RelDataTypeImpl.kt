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

import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.BasicSqlType
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.Iterables
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import java.io.Serializable
import java.nio.charset.Charset
import java.util.ArrayList
import java.util.List
import java.util.Objects
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * RelDataTypeImpl is an abstract base for implementations of
 * [RelDataType].
 *
 *
 * Identity is based upon the [.digest] field, which each derived class
 * should set during construction.
 */
abstract class RelDataTypeImpl protected constructor(@Nullable fieldList: List<RelDataTypeField?>? = null) :
    RelDataType, RelDataTypeFamily {
    //~ Instance fields --------------------------------------------------------
    @Nullable
    protected override val fieldList: List<RelDataTypeField>? = null

    @Nullable
    protected var digest: String? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RelDataTypeImpl.
     *
     * @param fieldList List of fields
     */
    /**
     * Default constructor, to allow derived classes such as
     * [BasicSqlType] to be [Serializable].
     *
     *
     * (The serialization specification says that a class can be serializable
     * even if its base class is not serializable, provided that the base class
     * has a public or protected zero-args constructor.)
     */
    init {
        if (fieldList != null) {
            // Create a defensive copy of the list.
            this.fieldList = ImmutableList.copyOf(fieldList)
        } else {
            this.fieldList = null
        }
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    fun getField(
        fieldName: String, caseSensitive: Boolean,
        elideRecord: Boolean
    ): RelDataTypeField? {
        if (fieldList == null) {
            throw IllegalStateException(
                "Trying to access field " + fieldName
                        + " in a type with no fields: " + this
            )
        }
        for (field in fieldList) {
            if (Util.matches(caseSensitive, field.getName(), fieldName)) {
                return field
            }
        }
        if (elideRecord) {
            val slots: List<Slot> = ArrayList()
            getFieldRecurse(slots, this, 0, fieldName, caseSensitive)
            loop@ for (slot in slots) {
                when (slot.count) {
                    0 -> {}
                    1 -> return slot.field
                    else -> break@loop  // duplicate fields at this depth; abandon search
                }
            }
        }
        // Extra field
        if (fieldList.size() > 0) {
            val lastField: RelDataTypeField = Iterables.getLast(fieldList)
            if (lastField.getName().equals("_extra")) {
                return RelDataTypeFieldImpl(
                    fieldName, -1, lastField.getType()
                )
            }
        }

        // a dynamic * field will match any field name.
        for (field in fieldList) {
            if (field.isDynamicStar()) {
                // the requested field could be in the unresolved star
                return field
            }
        }
        return null
    }

    @Override
    fun getFieldList(): List<RelDataTypeField>? {
        assert(fieldList != null) { "fieldList must not be null, type = $this" }
        return fieldList
    }

    @get:Override
    override val fieldNames: List<String?>?
        get() {
            assert(fieldList != null) { "fieldList must not be null, type = $this" }
            return Pair.left(fieldList)
        }

    @get:Override
    override val fieldCount: Int
        get() {
            assert(fieldList != null) { "fieldList must not be null, type = $this" }
            return fieldList!!.size()
        }

    @get:Override
    override val structKind: org.apache.calcite.rel.type.StructKind?
        get() = if (isStruct) StructKind.FULLY_QUALIFIED else StructKind.NONE

    // this is not a collection type
    @get:Nullable
    @get:Override
    override val componentType: org.apache.calcite.rel.type.RelDataType?
        get() =// this is not a collection type
            null

    // this is not a map type
    @get:Nullable
    @get:Override
    override val keyType: org.apache.calcite.rel.type.RelDataType?
        get() =// this is not a map type
            null

    // this is not a map type
    @get:Nullable
    @get:Override
    override val valueType: org.apache.calcite.rel.type.RelDataType?
        get() =// this is not a map type
            null

    @get:Override
    override val isStruct: Boolean
        get() = fieldList != null

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || obj is RelDataTypeImpl
                && Objects.equals(digest, (obj as RelDataTypeImpl).digest))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hashCode(digest)
    }

    @get:Override
    override val fullTypeString: String?
        get() = requireNonNull(digest, "digest")

    @get:Override
    override val isNullable: Boolean
        get() = false

    @get:Nullable
    @get:Override
    override val charset: Charset?
        get() = null

    @get:Nullable
    @get:Override
    override val collation: SqlCollation?
        get() = null

    @get:Nullable
    @get:Override
    override val intervalQualifier: SqlIntervalQualifier?
        get() = null

    @get:Override
    override val precision: Int
        get() = PRECISION_NOT_SPECIFIED

    @get:Override
    override val scale: Int
        get() = SCALE_NOT_SPECIFIED// The implementations must provide non-null value, however, we keep this for compatibility

    /**
     * Gets the [SqlTypeName] of this type.
     * Sub-classes must override the method to ensure the resulting value is non-nullable.
     *
     * @return SqlTypeName, never null
     */
    @get:Override
    override val sqlTypeName: SqlTypeName
        get() =// The implementations must provide non-null value, however, we keep this for compatibility
            castNonNull(null)

    @get:Nullable
    @get:Override
    override val sqlIdentifier: SqlIdentifier?
        get() {
            val typeName: SqlTypeName = sqlTypeName ?: return null
            return SqlIdentifier(
                typeName.name(),
                SqlParserPos.ZERO
            )
        }

    // by default, put each type into its own family
    @get:Override
    override val family: org.apache.calcite.rel.type.RelDataTypeFamily
        get() =// by default, put each type into its own family
            this

    /**
     * Generates a string representation of this type.
     *
     * @param sb         StringBuilder into which to generate the string
     * @param withDetail when true, all detail information needed to compute a
     * unique digest (and return from getFullTypeString) should
     * be included;
     */
    protected abstract fun generateTypeString(
        sb: StringBuilder?,
        withDetail: Boolean
    )

    /**
     * Computes the digest field. This should be called in every non-abstract
     * subclass constructor once the type is fully defined.
     */
    @SuppressWarnings("method.invocation.invalid")
    protected fun computeDigest() {
        val sb = StringBuilder()
        generateTypeString(sb, true)
        if (!isNullable) {
            sb.append(NON_NULLABLE_SUFFIX)
        }
        digest = sb.toString()
    }

    @Override
    override fun toString(): String {
        val sb = StringBuilder()
        generateTypeString(sb, false)
        return sb.toString()
    }

    // by default, make each type have a precedence list containing
    // only other types in the same family
    @get:Override
    override val precedenceList: org.apache.calcite.rel.type.RelDataTypePrecedenceList?
        get() =// by default, make each type have a precedence list containing
            // only other types in the same family
            object : RelDataTypePrecedenceList() {
                @Override
                fun containsType(type: RelDataType): Boolean {
                    return family === type.getFamily()
                }

                @Override
                fun compareTypePrecedence(
                    type1: RelDataType,
                    type2: RelDataType
                ): Int {
                    assert(containsType(type1))
                    assert(containsType(type2))
                    return 0
                }
            }

    @get:Override
    override val comparability: org.apache.calcite.rel.type.RelDataTypeComparability?
        get() = RelDataTypeComparability.ALL

    @get:Override
    override val isDynamicStruct: Boolean
        get() = false

    /** Work space for [RelDataTypeImpl.getFieldRecurse].  */
    private class Slot {
        var count = 0

        @Nullable
        var field: RelDataTypeField? = null
    }

    companion object {
        /**
         * Suffix for the digests of non-nullable types.
         */
        const val NON_NULLABLE_SUFFIX = " NOT NULL"
        private fun getFieldRecurse(
            slots: List<Slot>, type: RelDataType,
            depth: Int, fieldName: String, caseSensitive: Boolean
        ) {
            while (slots.size() <= depth) {
                slots.add(Slot())
            }
            val slot = slots[depth]
            for (field in type.getFieldList()) {
                if (Util.matches(caseSensitive, field.getName(), fieldName)) {
                    slot.count++
                    slot.field = field
                }
            }
            // No point looking to depth + 1 if there is a hit at depth.
            if (slot.count == 0) {
                for (field in type.getFieldList()) {
                    if (field.getType().isStruct()) {
                        getFieldRecurse(
                            slots, field.getType(), depth + 1,
                            fieldName, caseSensitive
                        )
                    }
                }
            }
        }

        /**
         * Returns an implementation of
         * [RelProtoDataType]
         * that copies a given type using the given type factory.
         */
        fun proto(protoType: RelDataType?): RelProtoDataType {
            assert(protoType != null)
            return RelProtoDataType { typeFactory -> typeFactory.copyType(protoType) }
        }

        /** Returns a [org.apache.calcite.rel.type.RelProtoDataType]
         * that will create a type `typeName`.
         *
         *
         * For example, `proto(SqlTypeName.DATE), false`
         * will create `DATE NOT NULL`.
         *
         * @param typeName Type name
         * @param nullable Whether nullable
         * @return Proto data type
         */
        fun proto(
            typeName: SqlTypeName?,
            nullable: Boolean
        ): RelProtoDataType {
            assert(typeName != null)
            return RelProtoDataType { typeFactory ->
                val type: RelDataType = typeFactory.createSqlType(typeName)
                typeFactory.createTypeWithNullability(type, nullable)
            }
        }

        /** Returns a [org.apache.calcite.rel.type.RelProtoDataType]
         * that will create a type `typeName(precision)`.
         *
         *
         * For example, `proto(SqlTypeName.VARCHAR, 100, false)`
         * will create `VARCHAR(100) NOT NULL`.
         *
         * @param typeName Type name
         * @param precision Precision
         * @param nullable Whether nullable
         * @return Proto data type
         */
        fun proto(
            typeName: SqlTypeName?,
            precision: Int, nullable: Boolean
        ): RelProtoDataType {
            assert(typeName != null)
            return RelProtoDataType { typeFactory ->
                val type: RelDataType = typeFactory.createSqlType(typeName, precision)
                typeFactory.createTypeWithNullability(type, nullable)
            }
        }

        /** Returns a [org.apache.calcite.rel.type.RelProtoDataType]
         * that will create a type `typeName(precision, scale)`.
         *
         *
         * For example, `proto(SqlTypeName.DECIMAL, 7, 2, false)`
         * will create `DECIMAL(7, 2) NOT NULL`.
         *
         * @param typeName Type name
         * @param precision Precision
         * @param scale Scale
         * @param nullable Whether nullable
         * @return Proto data type
         */
        fun proto(
            typeName: SqlTypeName?,
            precision: Int, scale: Int, nullable: Boolean
        ): RelProtoDataType {
            return RelProtoDataType { typeFactory ->
                val type: RelDataType = typeFactory.createSqlType(typeName, precision, scale)
                typeFactory.createTypeWithNullability(type, nullable)
            }
        }

        /**
         * Returns the "extra" field in a row type whose presence signals that
         * fields will come into existence just by asking for them.
         *
         * @param rowType Row type
         * @return The "extra" field, or null
         */
        @Nullable
        fun extra(rowType: RelDataType): RelDataTypeField {
            // Even in a case-insensitive connection, the name must be precisely
            // "_extra".
            return rowType.getField("_extra", true, false)
        }
    }
}
