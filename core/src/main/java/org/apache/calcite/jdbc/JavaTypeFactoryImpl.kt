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
package org.apache.calcite.jdbc

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Implementation of [JavaTypeFactory].
 *
 *
 * **NOTE: This class is experimental and subject to
 * change/removal without notice**.
 */
class JavaTypeFactoryImpl @JvmOverloads constructor(typeSystem: RelDataTypeSystem? = RelDataTypeSystem.DEFAULT) :
    SqlTypeFactoryImpl(typeSystem), JavaTypeFactory {
    private val syntheticTypes: Map<List<Pair<Type, Boolean>>, SyntheticRecordType> = HashMap()
    @Override
    fun createStructType(type: Class): RelDataType {
        val list: List<RelDataTypeField> = ArrayList()
        for (field in type.getFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                // FIXME: watch out for recursion
                val fieldType: Type = fieldType(field)
                list.add(
                    RelDataTypeFieldImpl(
                        field.getName(),
                        list.size(),
                        createType(fieldType)
                    )
                )
            }
        }
        return canonize(JavaRecordType(list, type))
    }

    @Override
    fun createType(type: Type): RelDataType {
        if (type is RelDataType) {
            return type as RelDataType
        }
        if (type is SyntheticRecordType) {
            val syntheticRecordType = type as SyntheticRecordType
            return requireNonNull(
                syntheticRecordType.relType
            ) { "relType for $syntheticRecordType" }
        }
        if (type is Types.ArrayType) {
            val arrayType: Types.ArrayType = type as Types.ArrayType
            val componentRelType: RelDataType = createType(arrayType.getComponentType())
            return createArrayType(
                createTypeWithNullability(
                    componentRelType,
                    arrayType.componentIsNullable()
                ), arrayType.maximumCardinality()
            )
        }
        if (type is Types.MapType) {
            val mapType: Types.MapType = type as Types.MapType
            val keyRelType: RelDataType = createType(mapType.getKeyType())
            val valueRelType: RelDataType = createType(mapType.getValueType())
            return createMapType(
                createTypeWithNullability(keyRelType, mapType.keyIsNullable()),
                createTypeWithNullability(valueRelType, mapType.valueIsNullable())
            )
        }
        if (type !is Class) {
            throw UnsupportedOperationException("TODO: implement $type")
        }
        val clazz: Class = type as Class
        when (Primitive.flavor(clazz)) {
            PRIMITIVE -> return createJavaType(clazz)
            BOX -> return createJavaType(Primitive.box(clazz))
            else -> {}
        }
        return if (JavaToSqlTypeConversionRules.instance().lookup(clazz) != null) {
            createJavaType(clazz)
        } else if (clazz.isArray()) {
            createMultisetType(
                createType(clazz.getComponentType()), -1
            )
        } else if (List::class.java.isAssignableFrom(clazz)) {
            createArrayType(
                createTypeWithNullability(createSqlType(SqlTypeName.ANY), true), -1
            )
        } else if (Map::class.java.isAssignableFrom(clazz)) {
            createMapType(
                createTypeWithNullability(createSqlType(SqlTypeName.ANY), true),
                createTypeWithNullability(createSqlType(SqlTypeName.ANY), true)
            )
        } else {
            createStructType(clazz)
        }
    }

    @Override
    fun getJavaClass(type: RelDataType): Type {
        if (type is JavaType) {
            val javaType: JavaType = type as JavaType
            return javaType.getJavaClass()
        }
        if (type is BasicSqlType || type is IntervalSqlType) {
            when (type.getSqlTypeName()) {
                VARCHAR, CHAR -> return String::class.java
                DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, INTEGER, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> return if (type.isNullable()) Integer::class.java else Int::class.javaPrimitiveType
                TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, BIGINT, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> return if (type.isNullable()) Long::class.java else Long::class.javaPrimitiveType
                SMALLINT -> return if (type.isNullable()) Short::class.java else Short::class.javaPrimitiveType
                TINYINT -> return if (type.isNullable()) Byte::class.java else Byte::class.javaPrimitiveType
                DECIMAL -> return BigDecimal::class.java
                BOOLEAN -> return if (type.isNullable()) Boolean::class.java else Boolean::class.javaPrimitiveType
                DOUBLE, FLOAT -> return if (type.isNullable()) Double::class.java else Double::class.javaPrimitiveType
                REAL -> return if (type.isNullable()) Float::class.java else Float::class.javaPrimitiveType
                BINARY, VARBINARY -> return ByteString::class.java
                GEOMETRY -> return Geometries.Geom::class.java
                SYMBOL -> return Enum::class.java
                ANY -> return Object::class.java
                NULL -> return Void::class.java
                else -> {}
            }
        }
        when (type.getSqlTypeName()) {
            ROW -> {
                assert(type is RelRecordType)
                return if (type is JavaRecordType) {
                    (type as JavaRecordType).clazz
                } else {
                    createSyntheticType(type as RelRecordType)
                }
            }
            MAP -> return Map::class.java
            ARRAY, MULTISET -> return List::class.java
            else -> {}
        }
        return Object::class.java
    }

    @Override
    fun toSql(type: RelDataType): RelDataType {
        return toSql(this, type)
    }

    @Override
    fun createSyntheticType(types: List<Type?>): Type {
        if (types.isEmpty()) {
            // Unit is a pre-defined synthetic type to be used when there are 0
            // fields. Because all instances are the same, we use a singleton.
            return Unit::class.java
        }
        val name = "Record" + types.size().toString() + "_" + syntheticTypes.size()
        val syntheticType = SyntheticRecordType(null, name)
        for (ord in Ord.zip(types)) {
            syntheticType.fields.add(
                RecordFieldImpl(
                    syntheticType,
                    "f" + ord.i,
                    ord.e,
                    !Primitive.`is`(ord.e),
                    Modifier.PUBLIC
                )
            )
        }
        return register(syntheticType)
    }

    private fun register(
        syntheticType: SyntheticRecordType
    ): SyntheticRecordType {
        val key: List<Pair<Type, Boolean>> = object : AbstractList<Pair<Type?, Boolean?>?>() {
            @Override
            operator fun get(index: Int): Pair<Type, Boolean> {
                val field: Types.RecordField = syntheticType.recordFields[index]
                return Pair.of(field.getType(), field.nullable())
            }

            @Override
            fun size(): Int {
                return syntheticType.recordFields.size()
            }
        }
        val syntheticType2 = syntheticTypes[key]
        return if (syntheticType2 == null) {
            syntheticTypes.put(key, syntheticType)
            syntheticType
        } else {
            syntheticType2
        }
    }

    /** Creates a synthetic Java class whose fields have the same names and
     * relational types.  */
    private fun createSyntheticType(type: RelRecordType): Type {
        val name = "Record" + type.getFieldCount().toString() + "_" + syntheticTypes.size()
        val syntheticType = SyntheticRecordType(type, name)
        for (recordField in type.getFieldList()) {
            val javaClass: Type = getJavaClass(recordField.getType())
            syntheticType.fields.add(
                RecordFieldImpl(
                    syntheticType,
                    recordField.getName(),
                    javaClass, recordField.getType().isNullable()
                            && !Primitive.`is`(javaClass),
                    Modifier.PUBLIC
                )
            )
        }
        return register(syntheticType)
    }

    /** Synthetic record type.  */
    class SyntheticRecordType(@Nullable relType: RelDataType?, name: String) : Types.RecordType {
        val fields: List<Types.RecordField> = ArrayList()

        @Nullable
        val relType: RelDataType?

        @get:Override
        val name: String

        init {
            this.relType = relType
            this.name = name
            assert(
                relType == null
                        || Util.isDistinct(relType.getFieldNames())
            ) { "field names not distinct: $relType" }
        }

        @get:Override
        val recordFields: List<Any>
            get() = fields

        @Override
        override fun toString(): String {
            return name
        }
    }

    /** Implementation of a field.  */
    private class RecordFieldImpl internal constructor(
        syntheticType: SyntheticRecordType?,
        name: String?,
        type: Type,
        nullable: Boolean,
        modifiers: Int
    ) : Types.RecordField {
        private val syntheticType: SyntheticRecordType

        @get:Override
        val name: String
        private val type: Type
        private val nullable: Boolean

        @get:Override
        val modifiers: Int

        init {
            this.syntheticType = requireNonNull(syntheticType, "syntheticType")
            this.name = requireNonNull(name, "name")
            this.type = requireNonNull(type, "type")
            this.nullable = nullable
            this.modifiers = modifiers
            assert(!(nullable && Primitive.`is`(type))) { "type [$type] can never be null" }
        }

        @Override
        fun getType(): Type {
            return type
        }

        @Override
        fun nullable(): Boolean {
            return nullable
        }

        @Override
        @Nullable
        operator fun get(@Nullable o: Object?): Object {
            throw UnsupportedOperationException()
        }

        @get:Override
        val declaringClass: Type
            get() = syntheticType
    }

    companion object {
        /** Returns the type of a field.
         *
         *
         * Takes into account [org.apache.calcite.adapter.java.Array]
         * annotations if present.
         */
        private fun fieldType(field: Field): Type {
            val klass: Class<*> = field.getType()
            val array: org.apache.calcite.adapter.java.Array =
                field.getAnnotation(org.apache.calcite.adapter.java.Array::class.java)
            if (array != null) {
                return ArrayType(
                    array.component(), array.componentIsNullable(),
                    array.maximumCardinality()
                )
            }
            val map: org.apache.calcite.adapter.java.Map =
                field.getAnnotation(org.apache.calcite.adapter.java.Map::class.java)
            return if (map != null) {
                MapType(
                    map.key(), map.keyIsNullable(), map.value(),
                    map.valueIsNullable()
                )
            } else klass
        }

        /** Converts a type in Java format to a SQL-oriented type.  */
        fun toSql(
            typeFactory: RelDataTypeFactory,
            type: RelDataType
        ): RelDataType {
            if (type is RelRecordType) {
                return typeFactory.createTypeWithNullability(
                    typeFactory.createStructType(
                        type.getFieldList()
                            .stream()
                            .map { field -> toSql(typeFactory, field.getType()) }
                            .collect(Collectors.toList()),
                        type.getFieldNames()),
                    type.isNullable())
            } else if (type is JavaType) {
                val sqlTypeName: SqlTypeName = type.getSqlTypeName()
                val relDataType: RelDataType
                relDataType = if (SqlTypeUtil.isArray(type)) {
                    // Transform to sql type, take care for two cases:
                    // 1. type.getJavaClass() is collection with erased generic type
                    // 2. ElementType returned by JavaType is also of JavaType,
                    // and needs conversion using typeFactory
                    val elementType: RelDataType = toSqlTypeWithNullToAny(
                        typeFactory, type.getComponentType()
                    )
                    typeFactory.createArrayType(elementType, -1)
                } else if (SqlTypeUtil.isMap(type)) {
                    val keyType: RelDataType = toSqlTypeWithNullToAny(
                        typeFactory, type.getKeyType()
                    )
                    val valueType: RelDataType = toSqlTypeWithNullToAny(
                        typeFactory, type.getValueType()
                    )
                    typeFactory.createMapType(keyType, valueType)
                } else {
                    typeFactory.createSqlType(sqlTypeName)
                }
                return typeFactory.createTypeWithNullability(relDataType, type.isNullable())
            }
            return type
        }

        private fun toSqlTypeWithNullToAny(
            typeFactory: RelDataTypeFactory, @Nullable type: RelDataType?
        ): RelDataType {
            return if (type == null) {
                typeFactory.createSqlType(SqlTypeName.ANY)
            } else toSql(typeFactory, type)
        }
    }
}
