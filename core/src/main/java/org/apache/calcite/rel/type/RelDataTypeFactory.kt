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

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.validate.SqlValidatorUtil
import java.nio.charset.Charset
import java.util.ArrayList
import java.util.List
import java.util.Map
import java.util.Objects

/**
 * RelDataTypeFactory is a factory for datatype descriptors. It defines methods
 * for instantiating and combining SQL, Java, and collection types. The factory
 * also provides methods for return type inference for arithmetic in cases where
 * SQL 2003 is implementation defined or impractical.
 *
 *
 * This interface is an example of the
 * [abstract factory pattern][org.apache.calcite.util.Glossary.ABSTRACT_FACTORY_PATTERN].
 * Any implementation of `RelDataTypeFactory` must ensure that type
 * objects are canonical: two types are equal if and only if they are
 * represented by the same Java object. This reduces memory consumption and
 * comparison cost.
 */
interface RelDataTypeFactory {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the type system.
     *
     * @return Type system
     */
    fun getTypeSystem(): RelDataTypeSystem

    /**
     * Creates a type that corresponds to a Java class.
     *
     * @param clazz the Java class used to define the type
     * @return canonical Java type descriptor
     */
    fun createJavaType(clazz: Class?): RelDataType?

    /**
     * Creates a cartesian product type.
     *
     * @return canonical join type descriptor
     * @param types array of types to be joined
     */
    fun createJoinType(vararg types: RelDataType?): RelDataType?

    /**
     * Creates a type that represents a structured collection of fields, given
     * lists of the names and types of the fields.
     *
     * @param  kind         Name resolution policy
     * @param typeList      types of the fields
     * @param fieldNameList names of the fields
     * @return canonical struct type descriptor
     */
    fun createStructType(
        kind: StructKind?,
        typeList: List<RelDataType?>?,
        fieldNameList: List<String?>?
    ): RelDataType?

    /** Creates a type that represents a structured collection of fields.
     * Shorthand for `createStructType(StructKind.FULLY_QUALIFIED, typeList,
     * fieldNameList)`.  */
    fun createStructType(
        typeList: List<RelDataType?>?,
        fieldNameList: List<String?>?
    ): RelDataType?

    /**
     * Creates a type that represents a structured collection of fields,
     * obtaining the field information via a callback.
     *
     * @param fieldInfo callback for field information
     * @return canonical struct type descriptor
     */
    @Deprecated
    fun  // to be removed before 2.0
            createStructType(fieldInfo: FieldInfo?): RelDataType?

    /**
     * Creates a type that represents a structured collection of fieldList,
     * obtaining the field information from a list of (name, type) pairs.
     *
     * @param fieldList List of (name, type) pairs
     * @return canonical struct type descriptor
     */
    fun createStructType(
        fieldList: List<Map.Entry<String?, RelDataType?>?>?
    ): RelDataType?

    /**
     * Creates an array type. Arrays are ordered collections of elements.
     *
     * @param elementType    type of the elements of the array
     * @param maxCardinality maximum array size, or -1 for unlimited
     * @return canonical array type descriptor
     */
    fun createArrayType(
        elementType: RelDataType?,
        maxCardinality: Long
    ): RelDataType?

    /**
     * Creates a map type. Maps are unordered collections of key/value pairs.
     *
     * @param keyType   type of the keys of the map
     * @param valueType type of the values of the map
     * @return canonical map type descriptor
     */
    fun createMapType(
        keyType: RelDataType?,
        valueType: RelDataType?
    ): RelDataType?

    /**
     * Creates a multiset type. Multisets are unordered collections of elements.
     *
     * @param elementType    type of the elements of the multiset
     * @param maxCardinality maximum collection size, or -1 for unlimited
     * @return canonical multiset type descriptor
     */
    fun createMultisetType(
        elementType: RelDataType?,
        maxCardinality: Long
    ): RelDataType?

    /**
     * Duplicates a type, making a deep copy. Normally, this is a no-op, since
     * canonical type objects are returned. However, it is useful when copying a
     * type from one factory to another.
     *
     * @param type input type
     * @return output type, a new object equivalent to input type
     */
    fun copyType(type: RelDataType?): RelDataType?

    /**
     * Creates a type that is the same as another type but with possibly
     * different nullability. The output type may be identical to the input
     * type. For type systems without a concept of nullability, the return value
     * is always the same as the input.
     *
     * @param type     input type
     * @param nullable true to request a nullable type; false to request a NOT
     * NULL type
     * @return output type, same as input type except with specified nullability
     * @throws NullPointerException if type is null
     */
    fun createTypeWithNullability(
        type: RelDataType?,
        nullable: Boolean
    ): RelDataType

    /**
     * Creates a type that is the same as another type but with possibly
     * different charset or collation. For types without a concept of charset or
     * collation this function must throw an error.
     *
     * @param type      input type
     * @param charset   charset to assign
     * @param collation collation to assign
     * @return output type, same as input type except with specified charset and
     * collation
     */
    fun createTypeWithCharsetAndCollation(
        type: RelDataType?,
        charset: Charset?,
        collation: SqlCollation?
    ): RelDataType?

    /** Returns the default [Charset] (valid if this is a string type).  */
    fun getDefaultCharset(): Charset?

    /**
     * Returns the most general of a set of types (that is, one type to which
     * they can all be cast), or null if conversion is not possible. The result
     * may be a new type that is less restrictive than any of the input types,
     * e.g. `leastRestrictive(INT, NUMERIC(3, 2))` could be
     * `NUMERIC(12, 2)`.
     *
     * @param types input types to be combined using union (not null, not empty)
     * @return canonical union type descriptor
     */
    @Nullable
    fun leastRestrictive(types: List<RelDataType?>?): RelDataType?

    /**
     * Creates a SQL type with no precision or scale.
     *
     * @param typeName Name of the type, for example [SqlTypeName.BOOLEAN],
     * never null
     * @return canonical type descriptor
     */
    fun createSqlType(typeName: SqlTypeName?): RelDataType?

    /**
     * Creates a SQL type that represents the "unknown" type.
     * It is only equal to itself, and is distinct from the NULL type.
     *
     * @return unknown type
     */
    fun createUnknownType(): RelDataType?

    /**
     * Creates a SQL type with length (precision) but no scale.
     *
     * @param typeName  Name of the type, for example [SqlTypeName.VARCHAR].
     * Never null.
     * @param precision Maximum length of the value (non-numeric types) or the
     * precision of the value (numeric/datetime types).
     * Must be non-negative or
     * [RelDataType.PRECISION_NOT_SPECIFIED].
     * @return canonical type descriptor
     */
    fun createSqlType(
        typeName: SqlTypeName?,
        precision: Int
    ): RelDataType?

    /**
     * Creates a SQL type with precision and scale.
     *
     * @param typeName  Name of the type, for example [SqlTypeName.DECIMAL].
     * Never null.
     * @param precision Precision of the value.
     * Must be non-negative or
     * [RelDataType.PRECISION_NOT_SPECIFIED].
     * @param scale     scale of the values, i.e. the number of decimal places to
     * shift the value. For example, a NUMBER(10,3) value of
     * "123.45" is represented "123450" (that is, multiplied by
     * 10^3). A negative scale *is* valid.
     * @return canonical type descriptor
     */
    fun createSqlType(
        typeName: SqlTypeName?,
        precision: Int,
        scale: Int
    ): RelDataType?

    /**
     * Creates a SQL interval type.
     *
     * @param intervalQualifier contains information if it is a year-month or a
     * day-time interval along with precision information
     * @return canonical type descriptor
     */
    fun createSqlIntervalType(
        intervalQualifier: SqlIntervalQualifier?
    ): RelDataType?

    /**
     * Infers the return type of a decimal multiplication. Decimal
     * multiplication involves at least one decimal operand and requires both
     * operands to have exact numeric types.
     *
     * @param type1 type of the first operand
     * @param type2 type of the second operand
     * @return the result type for a decimal multiplication, or null if decimal
     * multiplication should not be applied to the operands.
     */
    @Deprecated // to be removed before 2.0
    @Nullable
    @Deprecated(
        """Use
    {@link RelDataTypeSystem#deriveDecimalMultiplyType(RelDataTypeFactory, RelDataType, RelDataType)}"""
    )
    fun createDecimalProduct(
        type1: RelDataType?,
        type2: RelDataType?
    ): RelDataType?

    /**
     * Returns whether a decimal multiplication should be implemented by casting
     * arguments to double values.
     *
     *
     * Pre-condition: `createDecimalProduct(type1, type2) != null`
     *
     */
    @Deprecated
    @Deprecated(
        """Use
    {@link RelDataTypeSystem#shouldUseDoubleMultiplication(RelDataTypeFactory, RelDataType, RelDataType)}"""
    )
    fun  // to be removed before 2.0
            useDoubleMultiplication(
        type1: RelDataType?,
        type2: RelDataType?
    ): Boolean

    /**
     * Infers the return type of a decimal division. Decimal division involves
     * at least one decimal operand and requires both operands to have exact
     * numeric types.
     *
     * @param type1 type of the first operand
     * @param type2 type of the second operand
     * @return the result type for a decimal division, or null if decimal
     * division should not be applied to the operands.
     *
     */
    @Deprecated // to be removed before 2.0
    @Nullable
    @Deprecated(
        """Use
    {@link RelDataTypeSystem#deriveDecimalDivideType(RelDataTypeFactory, RelDataType, RelDataType)}"""
    )
    fun createDecimalQuotient(
        type1: RelDataType?,
        type2: RelDataType?
    ): RelDataType?

    /**
     * Create a decimal type equivalent to the numeric `type`,
     * this is related to specific system implementation,
     * you can override this logic if it is required.
     *
     * @param type the numeric type to create decimal type with
     * @return decimal equivalence of the numeric type.
     */
    fun decimalOf(type: RelDataType?): RelDataType

    /**
     * Creates a
     * [org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder].
     * But since `FieldInfoBuilder` is deprecated, we recommend that you use
     * its base class [Builder], which is not deprecated.
     */
    @SuppressWarnings("deprecation")
    fun builder(): FieldInfoBuilder?
    //~ Inner Interfaces -------------------------------------------------------
    /**
     * Callback that provides enough information to create fields.
     */
    @Deprecated
    interface FieldInfo {
        /**
         * Returns the number of fields.
         *
         * @return number of fields
         */
        fun getFieldCount(): Int

        /**
         * Returns the name of a given field.
         *
         * @param index Ordinal of field
         * @return Name of given field
         */
        fun getFieldName(index: Int): String

        /**
         * Returns the type of a given field.
         *
         * @param index Ordinal of field
         * @return Type of given field
         */
        fun getFieldType(index: Int): RelDataType
    }

    /**
     * Implementation of [FieldInfo] that provides a fluid API to build
     * a list of fields.
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    class FieldInfoBuilder(typeFactory: RelDataTypeFactory?) : Builder(typeFactory), FieldInfo {
        @Override
        override fun add(name: String?, type: RelDataType?): Builder {
            return super.add(name, type) as FieldInfoBuilder
        }

        @Override
        override fun add(name: String?, typeName: SqlTypeName?): Builder {
            return super.add(name, typeName) as FieldInfoBuilder
        }

        @Override
        override fun add(
            name: String?, typeName: SqlTypeName?,
            precision: Int
        ): FieldInfoBuilder {
            return super.add(name, typeName, precision) as FieldInfoBuilder
        }

        @Override
        override fun add(
            name: String?, typeName: SqlTypeName?,
            precision: Int, scale: Int
        ): FieldInfoBuilder {
            return super.add(name, typeName, precision, scale) as FieldInfoBuilder
        }

        @Override
        override fun add(
            name: String?, startUnit: TimeUnit?,
            startPrecision: Int, endUnit: TimeUnit?, fractionalSecondPrecision: Int
        ): FieldInfoBuilder {
            return super.add(
                name, startUnit, startPrecision,
                endUnit, fractionalSecondPrecision
            ) as FieldInfoBuilder
        }

        @Override
        override fun nullable(nullable: Boolean): FieldInfoBuilder {
            return super.nullable(nullable) as FieldInfoBuilder
        }

        @Override
        override fun add(field: RelDataTypeField): FieldInfoBuilder {
            return super.add(field) as FieldInfoBuilder
        }

        @Override
        override fun addAll(
            fields: Iterable<Map.Entry<String?, RelDataType?>?>
        ): FieldInfoBuilder {
            return super.addAll(fields) as FieldInfoBuilder
        }

        @Override
        override fun kind(kind: StructKind?): FieldInfoBuilder {
            return super.kind(kind) as FieldInfoBuilder
        }

        @Override
        override fun uniquify(): FieldInfoBuilder {
            return super.uniquify() as FieldInfoBuilder
        }
    }

    /** Fluid API to build a list of fields.  */
    class Builder(typeFactory: RelDataTypeFactory?) {
        private val names: List<String> = ArrayList()
        private val types: List<RelDataType> = ArrayList()
        private var kind: StructKind? = StructKind.FULLY_QUALIFIED
        private val typeFactory: RelDataTypeFactory
        private var nullableRecord = false

        /**
         * Creates a Builder with the given type factory.
         */
        init {
            this.typeFactory = Objects.requireNonNull(typeFactory, "typeFactory")
        }

        /**
         * Returns the number of fields.
         *
         * @return number of fields
         */
        fun getFieldCount(): Int {
            return names.size()
        }

        /**
         * Returns the name of a given field.
         *
         * @param index Ordinal of field
         * @return Name of given field
         */
        fun getFieldName(index: Int): String {
            return names[index]
        }

        /**
         * Returns the type of a given field.
         *
         * @param index Ordinal of field
         * @return Type of given field
         */
        fun getFieldType(index: Int): RelDataType {
            return types[index]
        }

        /**
         * Adds a field with given name and type.
         */
        fun add(name: String?, type: RelDataType?): Builder {
            names.add(name)
            types.add(type)
            return this
        }

        /**
         * Adds a field with a type created using
         * [org.apache.calcite.rel.type.RelDataTypeFactory.createSqlType].
         */
        fun add(name: String?, typeName: SqlTypeName?): Builder {
            add(name, typeFactory.createSqlType(typeName))
            return this
        }

        /**
         * Adds a field with a type created using
         * [org.apache.calcite.rel.type.RelDataTypeFactory.createSqlType].
         */
        fun add(name: String?, typeName: SqlTypeName?, precision: Int): Builder {
            add(name, typeFactory.createSqlType(typeName, precision))
            return this
        }

        /**
         * Adds a field with a type created using
         * [org.apache.calcite.rel.type.RelDataTypeFactory.createSqlType].
         */
        fun add(
            name: String?, typeName: SqlTypeName?, precision: Int,
            scale: Int
        ): Builder {
            add(name, typeFactory.createSqlType(typeName, precision, scale))
            return this
        }

        /**
         * Adds a field with an interval type.
         */
        fun add(
            name: String?, startUnit: TimeUnit?, startPrecision: Int,
            endUnit: TimeUnit?, fractionalSecondPrecision: Int
        ): Builder {
            val q = SqlIntervalQualifier(
                startUnit, startPrecision, endUnit,
                fractionalSecondPrecision, SqlParserPos.ZERO
            )
            add(name, typeFactory.createSqlIntervalType(q))
            return this
        }

        /**
         * Changes the nullability of the last field added.
         *
         * @throws java.lang.IndexOutOfBoundsException if no fields have been
         * added
         */
        fun nullable(nullable: Boolean): Builder {
            val lastType: RelDataType = types[types.size() - 1]
            if (lastType.isNullable() !== nullable) {
                val type: RelDataType = typeFactory.createTypeWithNullability(lastType, nullable)
                types.set(types.size() - 1, type)
            }
            return this
        }

        /**
         * Adds a field. Field's ordinal is ignored.
         */
        fun add(field: RelDataTypeField): Builder {
            add(field.getName(), field.getType())
            return this
        }

        /**
         * Adds all fields in a collection.
         */
        fun addAll(
            fields: Iterable<Map.Entry<String?, RelDataType?>?>
        ): Builder {
            for (field in fields) {
                add(field.getKey(), field.getValue())
            }
            return this
        }

        fun kind(kind: StructKind?): Builder {
            this.kind = kind
            return this
        }

        /** Sets whether the record type will be nullable.  */
        fun nullableRecord(nullableRecord: Boolean): Builder {
            this.nullableRecord = nullableRecord
            return this
        }

        /**
         * Makes sure that field names are unique.
         */
        fun uniquify(): Builder {
            val uniqueNames: List<String> = SqlValidatorUtil.uniquify(
                names,
                typeFactory.getTypeSystem().isSchemaCaseSensitive()
            )
            if (uniqueNames !== names) {
                names.clear()
                names.addAll(uniqueNames)
            }
            return this
        }

        /**
         * Creates a struct type with the current contents of this builder.
         */
        fun build(): RelDataType {
            return typeFactory.createTypeWithNullability(
                typeFactory.createStructType(kind, types, names),
                nullableRecord
            )
        }

        /** Creates a dynamic struct type with the current contents of this
         * builder.  */
        fun buildDynamic(): RelDataType {
            val dynamicType: RelDataType = DynamicRecordTypeImpl(typeFactory)
            val type: RelDataType = build()
            dynamicType.getFieldList().addAll(type.getFieldList())
            return dynamicType
        }

        /** Returns whether a field exists with the given name.  */
        fun nameExists(name: String): Boolean {
            return names.contains(name)
        }
    }
}
