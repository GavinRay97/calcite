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

import org.apache.calcite.linq4j.tree.Primitive
import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.type.ArraySqlType
import org.apache.calcite.sql.type.JavaToSqlTypeConversionRules
import org.apache.calcite.sql.type.MapSqlType
import org.apache.calcite.sql.type.MultisetSqlType
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.util.Util
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Interner
import com.google.common.collect.Interners
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.nio.charset.Charset
import java.sql.Time
import java.sql.Timestamp
import java.util.AbstractList
import java.util.ArrayList
import java.util.List
import java.util.Map
import java.util.Objects

/**
 * Abstract base for implementations of [RelDataTypeFactory].
 */
abstract class RelDataTypeFactoryImpl protected constructor(typeSystem: RelDataTypeSystem?) : RelDataTypeFactory {
    protected val typeSystem: RelDataTypeSystem
    //~ Constructors -----------------------------------------------------------
    /** Creates a type factory.  */
    init {
        this.typeSystem = Objects.requireNonNull(typeSystem, "typeSystem")
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun getTypeSystem(): RelDataTypeSystem {
        return typeSystem
    }

    // implement RelDataTypeFactory
    @Override
    override fun createJavaType(clazz: Class): RelDataType {
        val javaType: JavaType = if (clazz === String::class.java) JavaType(
            clazz, true, getDefaultCharset(),
            SqlCollation.IMPLICIT
        ) else JavaType(clazz)
        return canonize(javaType)
    }

    // implement RelDataTypeFactory
    @Override
    override fun createJoinType(vararg types: RelDataType?): RelDataType {
        assert(types != null)
        assert(types.size >= 1)
        val flattenedTypes: List<RelDataType> = ArrayList()
        getTypeList(ImmutableList.copyOf(types), flattenedTypes)
        return canonize(
            RelCrossType(flattenedTypes, getFieldList(flattenedTypes))
        )
    }

    @Override
    fun createStructType(
        typeList: List<RelDataType?>,
        fieldNameList: List<String?>
    ): RelDataType {
        return createStructType(
            StructKind.FULLY_QUALIFIED, typeList,
            fieldNameList
        )
    }

    @Override
    fun createStructType(
        kind: StructKind,
        typeList: List<RelDataType?>,
        fieldNameList: List<String?>
    ): RelDataType {
        return createStructType(
            kind, typeList,
            fieldNameList, false
        )
    }

    private fun createStructType(
        kind: StructKind,
        typeList: List<RelDataType?>,
        fieldNameList: List<String?>,
        nullable: Boolean
    ): RelDataType {
        assert(typeList.size() === fieldNameList.size())
        return canonize(kind, fieldNameList, typeList, nullable)
    }

    @SuppressWarnings("deprecation")
    @Override
    fun createStructType(
        fieldInfo: RelDataTypeFactory.FieldInfo
    ): RelDataType {
        return canonize(StructKind.FULLY_QUALIFIED,
            object : AbstractList<String?>() {
                @Override
                operator fun get(index: Int): String {
                    return fieldInfo.getFieldName(index)
                }

                @Override
                fun size(): Int {
                    return fieldInfo.getFieldCount()
                }
            },
            object : AbstractList<RelDataType?>() {
                @Override
                operator fun get(index: Int): RelDataType {
                    return fieldInfo.getFieldType(index)
                }

                @Override
                fun size(): Int {
                    return fieldInfo.getFieldCount()
                }
            })
    }

    @Override
    fun createStructType(
        fieldList: List<Map.Entry<String?, RelDataType?>?>
    ): RelDataType {
        return createStructType(fieldList, false)
    }

    private fun createStructType(
        fieldList: List<Map.Entry<String?, RelDataType?>?>, nullable: Boolean
    ): RelDataType {
        return canonize(StructKind.FULLY_QUALIFIED,
            object : AbstractList<String?>() {
                @Override
                operator fun get(index: Int): String {
                    return fieldList[index].getKey()
                }

                @Override
                fun size(): Int {
                    return fieldList.size()
                }
            },
            object : AbstractList<RelDataType?>() {
                @Override
                operator fun get(index: Int): RelDataType {
                    return fieldList[index].getValue()
                }

                @Override
                fun size(): Int {
                    return fieldList.size()
                }
            }, nullable
        )
    }

    @Override
    @Nullable
    fun leastRestrictive(types: List<RelDataType>?): RelDataType? {
        assert(types != null)
        assert(types!!.size() >= 1)
        val type0: RelDataType = types!![0]
        return if (type0.isStruct()) {
            leastRestrictiveStructuredType(types)
        } else null
    }

    @Nullable
    protected fun leastRestrictiveStructuredType(
        types: List<RelDataType>?
    ): RelDataType? {
        val type0: RelDataType = types!![0]
        // precheck that fieldCount is present
        if (!type0.isStruct()) {
            return null
        }
        val fieldCount: Int = type0.getFieldCount()

        // precheck that all types are structs with same number of fields
        // and register desired nullability for the result
        var isNullable = false
        for (type in types) {
            if (!type.isStruct()) {
                return null
            }
            if (type.getFieldList().size() !== fieldCount) {
                return null
            }
            isNullable = isNullable or type.isNullable()
        }

        // recursively compute column-wise least restrictive
        val builder: Builder = builder()
        for (j in 0 until fieldCount) {
            // REVIEW jvs 22-Jan-2004:  Always use the field name from the
            // first type?
            val k: Int = j
            val type: RelDataType = leastRestrictive(
                Util.transform(types) { t -> t.getFieldList().get(k).getType() }
            ) ?: return null
            builder.add(
                type0.getFieldList().get(j).getName(),
                type
            )
        }
        return createTypeWithNullability(builder.build(), isNullable)
    }

    @Nullable
    protected fun leastRestrictiveArrayMultisetType(
        types: List<RelDataType>, sqlTypeName: SqlTypeName
    ): RelDataType? {
        assert(sqlTypeName === SqlTypeName.ARRAY || sqlTypeName === SqlTypeName.MULTISET)
        var isNullable = false
        for (type in types) {
            if (type.getComponentType() == null) {
                return null
            }
            isNullable = isNullable or type.isNullable()
        }
        val type: RelDataType = leastRestrictive(
            Util.transform(
                types
            ) { t -> if (t is ArraySqlType) (t as ArraySqlType).getComponentType() else (t as MultisetSqlType).getComponentType() })
            ?: return null
        return if (sqlTypeName === SqlTypeName.ARRAY) ArraySqlType(type, isNullable) else MultisetSqlType(
            type,
            isNullable
        )
    }

    @Nullable
    protected fun leastRestrictiveMapType(
        types: List<RelDataType>, sqlTypeName: SqlTypeName
    ): RelDataType? {
        assert(sqlTypeName === SqlTypeName.MAP)
        var isNullable = false
        for (type in types) {
            if (type !is MapSqlType) {
                return null
            }
            isNullable = isNullable or type.isNullable()
        }
        val keyType: RelDataType = leastRestrictive(
            Util.transform(types) { t -> (t as MapSqlType).getKeyType() }) ?: return null
        val valueType: RelDataType = leastRestrictive(
            Util.transform(types) { t -> (t as MapSqlType).getValueType() }) ?: return null
        return MapSqlType(keyType, valueType, isNullable)
    }

    // copy a non-record type, setting nullability
    private fun copySimpleType(
        type: RelDataType,
        nullable: Boolean
    ): RelDataType {
        return if (type is JavaType) {
            val javaType = type
            if (SqlTypeUtil.inCharFamily(javaType)) {
                JavaType(
                    javaType.clazz,
                    nullable,
                    javaType.charset,
                    javaType.collation
                )
            } else {
                JavaType(
                    if (nullable) Primitive.box(javaType.clazz) else Primitive.unbox(javaType.clazz),
                    nullable
                )
            }
        } else {
            // REVIEW: RelCrossType if it stays around; otherwise get rid of
            // this comment
            type
        }
    }

    // recursively copy a record type
    private fun copyRecordType(
        type: RelRecordType,
        ignoreNullable: Boolean,
        nullable: Boolean
    ): RelDataType {
        // For flattening and outer joins, it is desirable to change
        // the nullability of the individual fields.
        return createStructType(
            type.getStructKind(),
            object : AbstractList<RelDataType?>() {
                @Override
                operator fun get(index: Int): RelDataType {
                    val fieldType: RelDataType = type.getFieldList()!!.get(index).getType()
                    return if (ignoreNullable) {
                        copyType(fieldType)
                    } else {
                        createTypeWithNullability(fieldType, nullable)
                    }
                }

                @Override
                fun size(): Int {
                    return type.getFieldCount()
                }
            },
            type.getFieldNames(), nullable
        )
    }

    // implement RelDataTypeFactory
    @Override
    fun copyType(type: RelDataType): RelDataType {
        return createTypeWithNullability(type, type.isNullable())
    }

    // implement RelDataTypeFactory
    @Override
    fun createTypeWithNullability(
        type: RelDataType,
        nullable: Boolean
    ): RelDataType {
        Objects.requireNonNull(type, "type")
        val newType: RelDataType
        newType = if (type.isNullable() === nullable) {
            type
        } else if (type is RelRecordType) {
            // REVIEW: angel 18-Aug-2005 dtbug 336 workaround
            // Changed to ignore nullable parameter if nullable is false since
            // copyRecordType implementation is doubtful
            // - If nullable -> Do a deep copy, setting all fields of the record type
            // to be nullable regardless of initial nullability.
            // - If not nullable -> Do a deep copy, setting not nullable at top RelRecordType
            // level only, keeping its fields' nullability as before.
            // According to the SQL standard, nullability for struct types can be defined only for
            // columns, which translates to top level structs. Nested struct attributes are always
            // nullable, so in principle we could always set the nested attributes to be nullable.
            // However, this might create regressions so we will not do it and we will keep previous
            // behavior.
            copyRecordType(type, !nullable, nullable)
        } else {
            copySimpleType(type, nullable)
        }
        return canonize(newType)
    }

    /**
     * Registers a type, or returns the existing type if it is already
     * registered.
     *
     * @throws NullPointerException if type is null
     */
    @SuppressWarnings("BetaApi")
    protected fun canonize(type: RelDataType?): RelDataType {
        return DATATYPE_CACHE.intern(type)
    }

    /**
     * Looks up a type using a temporary key, and if not present, creates
     * a permanent key and type.
     *
     *
     * This approach allows us to use a cheap temporary key. A permanent
     * key is more expensive, because it must be immutable and not hold
     * references into other data structures.
     */
    protected fun canonize(
        kind: StructKind,
        names: List<String?>,
        types: List<RelDataType?>,
        nullable: Boolean
    ): RelDataType {
        val type: RelDataType = KEY2TYPE_CACHE.getIfPresent(
            Key(kind, names, types, nullable)
        )
        if (type != null) {
            return type
        }
        val names2: ImmutableList<String?> = ImmutableList.copyOf(names)
        val types2: ImmutableList<RelDataType?> = ImmutableList.copyOf(types)
        return KEY2TYPE_CACHE.getUnchecked(Key(kind, names2, types2, nullable))
    }

    protected fun canonize(
        kind: StructKind,
        names: List<String?>,
        types: List<RelDataType?>
    ): RelDataType {
        return canonize(kind, names, types, false)
    }

    @Nullable
    private fun fieldsOf(clazz: Class): List<RelDataTypeFieldImpl>? {
        val list: List<RelDataTypeFieldImpl> = ArrayList()
        for (field in clazz.getFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue
            }
            list.add(
                RelDataTypeFieldImpl(
                    field.getName(),
                    list.size(),
                    createJavaType(field.getType())
                )
            )
        }
        return if (list.isEmpty()) {
            null
        } else list
    }

    /**
     * Delegates to
     * [RelDataTypeSystem.deriveDecimalMultiplyType]
     * to get the return type for the operation.
     */
    @Deprecated
    @Override
    @Nullable
    fun createDecimalProduct(
        type1: RelDataType,
        type2: RelDataType
    ): RelDataType? {
        return typeSystem.deriveDecimalMultiplyType(this, type1, type2)
    }

    /**
     * Delegates to
     * [RelDataTypeSystem.shouldUseDoubleMultiplication]
     * to get if double should be used for multiplication.
     */
    @Deprecated
    @Override
    fun useDoubleMultiplication(
        type1: RelDataType,
        type2: RelDataType
    ): Boolean {
        return typeSystem.shouldUseDoubleMultiplication(this, type1, type2)
    }

    /**
     * Delegates to
     * [RelDataTypeSystem.deriveDecimalDivideType]
     * to get the return type for the operation.
     */
    @Deprecated
    @Override
    @Nullable
    fun createDecimalQuotient(
        type1: RelDataType,
        type2: RelDataType
    ): RelDataType? {
        return typeSystem.deriveDecimalDivideType(this, type1, type2)
    }

    @Override
    fun decimalOf(type: RelDataType): RelDataType {
        // create decimal type and sync nullability
        return createTypeWithNullability(decimalOf2(type), type.isNullable())
    }

    /** Create decimal type equivalent with the given `type` while sans nullability.  */
    private fun decimalOf2(type: RelDataType): RelDataType {
        assert(SqlTypeUtil.isNumeric(type) || SqlTypeUtil.isNull(type))
        val typeName: SqlTypeName = type.getSqlTypeName()
        assert(typeName != null)
        return when (typeName) {
            DECIMAL ->       // Fix the precision when the type is JavaType.
                if (isJavaType(type)) SqlTypeUtil.getMaxPrecisionScaleDecimal(this) else type
            TINYINT -> createSqlType(SqlTypeName.DECIMAL, 3, 0)!!
            SMALLINT -> createSqlType(SqlTypeName.DECIMAL, 5, 0)!!
            INTEGER -> createSqlType(SqlTypeName.DECIMAL, 10, 0)!!
            BIGINT ->       // the default max precision is 19, so this is actually DECIMAL(19, 0)
                // but derived system can override the max precision/scale.
                createSqlType(SqlTypeName.DECIMAL, 38, 0)!!
            REAL -> createSqlType(SqlTypeName.DECIMAL, 14, 7)!!
            FLOAT -> createSqlType(SqlTypeName.DECIMAL, 14, 7)!!
            DOUBLE ->       // the default max precision is 19, so this is actually DECIMAL(19, 15)
                // but derived system can override the max precision/scale.
                createSqlType(SqlTypeName.DECIMAL, 30, 15)!!
            else ->       // default precision and scale.
                createSqlType(SqlTypeName.DECIMAL)!!
        }
    }

    @Override
    override fun getDefaultCharset(): Charset {
        return Util.getDefaultCharset()
    }

    @SuppressWarnings("deprecation")
    @Override
    override fun builder(): FieldInfoBuilder {
        return FieldInfoBuilder(this)
    }
    //~ Inner Classes ----------------------------------------------------------
    // TODO jvs 13-Dec-2004:  move to OJTypeFactoryImpl?
    /**
     * Type which is based upon a Java class.
     */
    inner class JavaType @SuppressWarnings("argument.type.incompatible") constructor(
        clazz: Class,
        nullable: Boolean,
        @Nullable charset: Charset?,
        @Nullable collation: SqlCollation?
    ) : RelDataTypeImpl(fieldsOf(clazz)) {
        val clazz: Class
        private val nullable: Boolean

        @Nullable
        override val collation: SqlCollation?

        @Nullable
        override val charset: Charset?

        constructor(clazz: Class) : this(clazz, !clazz.isPrimitive()) {}
        constructor(
            clazz: Class,
            nullable: Boolean
        ) : this(clazz, nullable, null, null) {
        }

        init {
            this.clazz = clazz
            this.nullable = nullable
            assert(charset != null == SqlTypeUtil.inCharFamily(this)) { "Need to be a chartype" }
            this.charset = charset
            this.collation = collation
            computeDigest()
        }

        fun getJavaClass(): Class {
            return clazz
        }

        @Override
        fun isNullable(): Boolean {
            return nullable
        }

        @Override
        fun getFamily(): RelDataTypeFamily {
            val family: RelDataTypeFamily? = CLASS_FAMILIES[clazz]
            return if (family != null) family else this
        }

        @Override
        protected override fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
            sb.append("JavaType(")
            sb.append(clazz)
            sb.append(")")
        }

        @Override
        @Nullable
        fun getComponentType(): RelDataType? {
            val componentType: Class = clazz.getComponentType()
            return if (componentType == null) {
                null
            } else {
                createJavaType(componentType)
            }
        }

        /**
         * For [JavaType] created with [Map] class,
         * we cannot get the key type. Use ANY as key type.
         */
        @Override
        @Nullable
        fun getKeyType(): RelDataType? {
            return if (Map::class.java.isAssignableFrom(clazz)) {
                // Need to return a SQL type because the type inference needs SqlTypeName.
                createSqlType(SqlTypeName.ANY)
            } else {
                null
            }
        }

        /**
         * For [JavaType] created with [Map] class,
         * we cannot get the value type. Use ANY as value type.
         */
        @Override
        @Nullable
        fun getValueType(): RelDataType? {
            return if (Map::class.java.isAssignableFrom(clazz)) {
                // Need to return a SQL type because the type inference needs SqlTypeName.
                createSqlType(SqlTypeName.ANY)
            } else {
                null
            }
        }

        @Override
        @Nullable
        fun getCharset(): Charset? {
            return charset
        }

        @Override
        @Nullable
        fun getCollation(): SqlCollation? {
            return collation
        }

        @Override
        fun getSqlTypeName(): SqlTypeName {
            return JavaToSqlTypeConversionRules.instance().lookup(clazz) ?: return SqlTypeName.OTHER
        }
    }

    /** Key to the data type cache.  */
    private class Key internal constructor(
        kind: StructKind,
        names: List<String?>,
        types: List<RelDataType?>,
        nullable: Boolean
    ) {
        val kind: StructKind
        val names: List<String?>
        val types: List<RelDataType?>
        val nullable: Boolean

        init {
            this.kind = kind
            this.names = names
            this.types = types
            this.nullable = nullable
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(kind, names, types, nullable)
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || (obj is Key
                    && kind === (obj as Key).kind && names.equals((obj as Key).names)
                    && types.equals((obj as Key).types)
                    && nullable == (obj as Key).nullable))
        }
    }

    companion object {
        //~ Instance fields --------------------------------------------------------
        /**
         * Global cache for Key to RelDataType. Uses soft values to allow GC.
         */
        private val KEY2TYPE_CACHE: LoadingCache<Key, RelDataType> = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from { key: Key -> keyToType(key) })

        /**
         * Global cache for RelDataType.
         */
        @SuppressWarnings("BetaApi")
        private val DATATYPE_CACHE: Interner<RelDataType> = Interners.newWeakInterner()
        private fun keyToType(key: Key): RelDataType {
            val list: ImmutableList.Builder<RelDataTypeField> = ImmutableList.builder()
            for (i in 0 until key.names.size()) {
                list.add(
                    RelDataTypeFieldImpl(
                        key.names[i], i, key.types[i]
                    )
                )
            }
            return RelRecordType(key.kind, list.build(), key.nullable)
        }

        private val CLASS_FAMILIES: Map<Class, RelDataTypeFamily> =
            ImmutableMap.< Class, RelDataTypeFamily>builder<Class?, RelDataTypeFamily?>()
        .put(String::
        class.java, SqlTypeFamily.CHARACTER)
        .put(kotlin.ByteArray::
        class.java, SqlTypeFamily.BINARY)
        .put(Boolean::
        class.javaPrimitiveType, SqlTypeFamily.BOOLEAN)
        .put(Boolean::
        class.java, SqlTypeFamily.BOOLEAN)
        .put(kotlin.Char::
        class.javaPrimitiveType, SqlTypeFamily.NUMERIC)
        .put(Character::
        class.java, SqlTypeFamily.NUMERIC)
        .put(Short::
        class.javaPrimitiveType, SqlTypeFamily.NUMERIC)
        .put(Short::
        class.java, SqlTypeFamily.NUMERIC)
        .put(Int::
        class.javaPrimitiveType, SqlTypeFamily.NUMERIC)
        .put(Integer::
        class.java, SqlTypeFamily.NUMERIC)
        .put(Long::
        class.javaPrimitiveType, SqlTypeFamily.NUMERIC)
        .put(Long::
        class.java, SqlTypeFamily.NUMERIC)
        .put(kotlin.Float::
        class.javaPrimitiveType, SqlTypeFamily.APPROXIMATE_NUMERIC)
        .put(Float::
        class.java, SqlTypeFamily.APPROXIMATE_NUMERIC)
        .put(kotlin.Double::
        class.javaPrimitiveType, SqlTypeFamily.APPROXIMATE_NUMERIC)
        .put(Double::
        class.java, SqlTypeFamily.APPROXIMATE_NUMERIC)
        .put(java.sql.Date::
        class.java, SqlTypeFamily.DATE)
        .put(Time::
        class.java, SqlTypeFamily.TIME)
        .put(Timestamp::
        class.java, SqlTypeFamily.TIMESTAMP)
        .build()
        /**
         * Returns a list of the fields in a list of types.
         */
        private fun getFieldList(types: List<RelDataType>): List<RelDataTypeField> {
            val fieldList: List<RelDataTypeField> = ArrayList()
            for (type in types) {
                addFields(type, fieldList)
            }
            return fieldList
        }

        /**
         * Returns a list of all atomic types in a list.
         */
        private fun getTypeList(
            inTypes: ImmutableList<RelDataType>,
            flatTypes: List<RelDataType>
        ) {
            for (inType in inTypes) {
                if (inType is RelCrossType) {
                    getTypeList((inType as RelCrossType).types, flatTypes)
                } else {
                    flatTypes.add(inType)
                }
            }
        }

        /**
         * Adds all fields in `type` to `fieldList`,
         * renumbering the fields (if necessary) to ensure that their index
         * matches their position in the list.
         */
        private fun addFields(
            type: RelDataType,
            fieldList: List<RelDataTypeField>
        ) {
            if (type is RelCrossType) {
                for (type1 in type.types) {
                    addFields(type1, fieldList)
                }
            } else {
                val fields: List<RelDataTypeField> = type.getFieldList()
                for (field in fields) {
                    if (field.getIndex() !== fieldList.size()) {
                        field = RelDataTypeFieldImpl(
                            field.getName(), fieldList.size(),
                            field.getType()
                        )
                    }
                    fieldList.add(field)
                }
            }
        }

        fun isJavaType(t: RelDataType?): Boolean {
            return t is JavaType
        }
    }
}
