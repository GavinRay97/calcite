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
package org.apache.calcite.adapter.java

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable.EnumUtils
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.linq4j.function.Function1
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.linq4j.tree.Primitive
import org.apache.calcite.rel.RelReferentialConstraint
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.Function
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.SchemaFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Schemas
import org.apache.calcite.schema.Statistic
import org.apache.calcite.schema.Statistics
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.TableMacro
import org.apache.calcite.schema.TranslatableTable
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.schema.impl.AbstractTableQueryable
import org.apache.calcite.schema.impl.ReflectiveFunctionBase
import org.apache.calcite.util.BuiltInMethod
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableMultimap
import com.google.common.collect.Iterables
import com.google.common.collect.Multimap
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import java.lang.reflect.Constructor
import java.lang.reflect.Field
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Type
import java.util.Collections
import java.util.List
import java.util.Map
import java.util.Objects.requireNonNull

/**
 * Implementation of [org.apache.calcite.schema.Schema] that exposes the
 * public fields and methods in a Java object.
 */
class ReflectiveSchema(target: Object) : AbstractSchema() {
    private val clazz: Class
    private val target: Object

    @get:Override
    @get:SuppressWarnings(["rawtypes", "unchecked"])
    @MonotonicNonNull
    protected var tableMap: Map<String, Table>? = null
        protected get() {
            if (field == null) {
                field = createTableMap()
            }
            return field
        }
        private set

    @MonotonicNonNull
    private var functionMap: Multimap<String, Function>? = null

    /**
     * Creates a ReflectiveSchema.
     *
     * @param target Object whose fields will be sub-objects of the schema
     */
    init {
        clazz = target.getClass()
        this.target = target
    }

    @Override
    override fun toString(): String {
        return "ReflectiveSchema(target=$target)"
    }

    /** Returns the wrapped object.
     *
     *
     * May not appear to be used, but is used in generated code via
     * [org.apache.calcite.util.BuiltInMethod.REFLECTIVE_SCHEMA_GET_TARGET].
     */
    fun getTarget(): Object {
        return target
    }

    private fun createTableMap(): Map<String, Table> {
        val builder: ImmutableMap.Builder<String, Table> = ImmutableMap.builder()
        for (field in clazz.getFields()) {
            val fieldName: String = field.getName()
            val table: Table = fieldRelation<Any>(field) ?: continue
            builder.put(fieldName, table)
        }
        val tableMap: Map<String, Table> = builder.build()
        // Unique-Key - Foreign-Key
        for (field in clazz.getFields()) {
            if (RelReferentialConstraint::class.java.isAssignableFrom(field.getType())) {
                var rc: RelReferentialConstraint
                rc = try {
                    field.get(target) as RelReferentialConstraint
                } catch (e: IllegalAccessException) {
                    throw RuntimeException(
                        "Error while accessing field $field", e
                    )
                }
                requireNonNull(rc) { "field must not be null: $field" }
                val table = tableMap[Util.last(rc.getSourceQualifiedName())] as FieldTable<*>?
                assert(table != null)
                var referentialConstraints: List<RelReferentialConstraint?> =
                    table!!.getStatistic().getReferentialConstraints()
                if (referentialConstraints == null) {
                    // This enables to keep the same Statistics.of below
                    referentialConstraints = ImmutableList.of()
                }
                table.statistic = Statistics.of(
                    ImmutableList.copyOf(
                        Iterables.concat(
                            referentialConstraints,
                            Collections.singleton(rc)
                        )
                    )
                )
            }
        }
        return tableMap
    }

    @get:Override
    protected val functionMultimap: Multimap<String, Function>?
        protected get() {
            if (functionMap == null) {
                functionMap = createFunctionMap()
            }
            return functionMap
        }

    private fun createFunctionMap(): Multimap<String, Function> {
        val builder: ImmutableMultimap.Builder<String, Function> = ImmutableMultimap.builder()
        for (method in clazz.getMethods()) {
            val methodName: String = method.getName()
            if (method.getDeclaringClass() === Object::class.java
                || methodName.equals("toString")
            ) {
                continue
            }
            if (TranslatableTable::class.java.isAssignableFrom(method.getReturnType())) {
                val tableMacro: TableMacro = MethodTableMacro(this, method)
                builder.put(methodName, tableMacro)
            }
        }
        return builder.build()
    }

    /** Returns an expression for the object wrapped by this schema (not the
     * schema itself).  */
    fun getTargetExpression(@Nullable parentSchema: SchemaPlus?, name: String?): Expression {
        return EnumUtils.convert(
            Expressions.call(
                Schemas.unwrap(
                    getExpression(parentSchema, name),
                    ReflectiveSchema::class.java
                ),
                BuiltInMethod.REFLECTIVE_SCHEMA_GET_TARGET.method
            ),
            target.getClass()
        )
    }

    /** Returns a table based on a particular field of this schema. If the
     * field is not of the right type to be a relation, returns null.  */
    private fun <T> fieldRelation(field: Field): @Nullable Table? {
        val elementType: Type = getElementType(field.getType())
            ?: return null
        val o: Object
        o = try {
            field.get(target)
        } catch (e: IllegalAccessException) {
            throw RuntimeException(
                "Error while accessing field $field", e
            )
        }
        requireNonNull(o) { "field $field is null for $target" }
        @SuppressWarnings("unchecked") val enumerable: Enumerable<T> = toEnumerable(o)
        return FieldTable<Any>(field, elementType, enumerable)
    }

    /** Table that is implemented by reading from a Java object.  */
    private class ReflectiveTable internal constructor(elementType: Type?, enumerable: Enumerable) :
        AbstractQueryableTable(elementType), Table, ScannableTable {
        private val enumerable: Enumerable

        init {
            this.enumerable = enumerable
        }

        @Override
        fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
            return (typeFactory as JavaTypeFactory).createType(elementType)
        }

        @get:Override
        val statistic: Statistic
            get() = Statistics.UNKNOWN

        @Override
        fun scan(root: DataContext?): Enumerable<Array<Object>> {
            return if (elementType === kotlin.Array<Object>::class.java) {
                enumerable
            } else {
                enumerable.select(FieldSelector(elementType as Class))
            }
        }

        @Override
        fun <T> asQueryable(
            queryProvider: QueryProvider?,
            schema: SchemaPlus?, tableName: String?
        ): Queryable<T> {
            return object : AbstractTableQueryable<T>(
                queryProvider, schema, this,
                tableName
            ) {
                @SuppressWarnings("unchecked")
                @Override
                fun enumerator(): Enumerator<T> {
                    return enumerable.enumerator() as Enumerator<T>
                }
            }
        }
    }

    /** Factory that creates a schema by instantiating an object and looking at
     * its public fields.
     *
     *
     * The following example instantiates a `FoodMart` object as a schema
     * that contains tables called `EMPS` and `DEPTS` based on the
     * object's fields.
     *
     * <blockquote><pre>
     * schemas: [
     * {
     * name: "foodmart",
     * type: "custom",
     * factory: "org.apache.calcite.adapter.java.ReflectiveSchema$Factory",
     * operand: {
     * class: "com.acme.FoodMart",
     * staticMethod: "instance"
     * }
     * }
     * ]
     * &nbsp;
     * class FoodMart {
     * public static final FoodMart instance() {
     * return new FoodMart();
     * }
     * &nbsp;
     * Employee[] EMPS;
     * Department[] DEPTS;
     * }</pre></blockquote>
     */
    class Factory : SchemaFactory {
        @Override
        fun create(
            parentSchema: SchemaPlus?, name: String?,
            operand: Map<String?, Object?>
        ): Schema {
            val clazz: Class<*>
            val target: Object
            val className: Object? = operand["class"]
            clazz = if (className != null) {
                try {
                    Class.forName(className as String?)
                } catch (e: ClassNotFoundException) {
                    throw RuntimeException("Error loading class $className", e)
                }
            } else {
                throw RuntimeException("Operand 'class' is required")
            }
            val methodName: Object? = operand["staticMethod"]
            if (methodName != null) {
                try {
                    val method: Method = clazz.getMethod(methodName as String?)
                    target = method.invoke(null)
                    requireNonNull(target) { "method $method returns null" }
                } catch (e: Exception) {
                    throw RuntimeException("Error invoking method $methodName", e)
                }
            } else {
                target = try {
                    val constructor: Constructor<*> = clazz.getConstructor()
                    constructor.newInstance()
                } catch (e: Exception) {
                    throw RuntimeException(
                        "Error instantiating class $className",
                        e
                    )
                }
            }
            return ReflectiveSchema(target)
        }
    }

    /** Table macro based on a Java method.  */
    private class MethodTableMacro internal constructor(private val schema: ReflectiveSchema, method: Method) :
        ReflectiveFunctionBase(method), TableMacro {
        init {
            assert(TranslatableTable::class.java.isAssignableFrom(method.getReturnType())) {
                ("Method should return TranslatableTable so the macro can be "
                        + "expanded")
            }
        }

        @Override
        override fun toString(): String {
            return "Member {method=" + method.toString() + "}"
        }

        @Override
        fun apply(arguments: List<Object?>): TranslatableTable {
            return try {
                val o: Object = requireNonNull(
                    method.invoke(schema.getTarget(), arguments.toArray())
                ) { "method " + method.toString() + " returned null for arguments " + arguments }
                o as TranslatableTable
            } catch (e: IllegalAccessException) {
                throw RuntimeException(e)
            } catch (e: InvocationTargetException) {
                throw RuntimeException(e)
            }
        }
    }

    /** Table based on a Java field.
     *
     * @param <T> element type
    </T> */
    private class FieldTable<T> internal constructor(
        field: Field, elementType: Type?, enumerable: Enumerable<T>,
        statistic: Statistic
    ) : ReflectiveTable(elementType, enumerable) {
        private val field: Field
        override val statistic: Statistic

        internal constructor(field: Field, elementType: Type?, enumerable: Enumerable<T>) : this(
            field,
            elementType,
            enumerable,
            Statistics.UNKNOWN
        ) {
        }

        init {
            this.field = field
            this.statistic = statistic
        }

        @Override
        override fun toString(): String {
            return "Relation {field=" + field.getName().toString() + "}"
        }

        @Override
        override fun getStatistic(): Statistic {
            return statistic
        }

        @Override
        override fun getExpression(
            schema: SchemaPlus,
            tableName: String?, clazz: Class?
        ): Expression {
            val reflectiveSchema: ReflectiveSchema = requireNonNull(
                schema.unwrap(ReflectiveSchema::class.java)
            ) { "schema.unwrap(ReflectiveSchema.class) for $schema" }
            return Expressions.field(
                reflectiveSchema.getTargetExpression(
                    schema.getParentSchema(), schema.getName()
                ), field
            )
        }
    }

    /** Function that returns an array of a given object's field values.  */
    private class FieldSelector internal constructor(elementType: Class) : Function1<Object?, Array<Object?>?> {
        private val fields: Array<Field>

        init {
            fields = elementType.getFields()
        }

        @Override
        @Nullable
        fun apply(o: Object?): Array<Object?> {
            return try {
                @Nullable val objects: Array<Object?> = arrayOfNulls<Object>(fields.size)
                for (i in fields.indices) {
                    objects[i] = fields[i].get(o)
                }
                objects
            } catch (e: IllegalAccessException) {
                throw RuntimeException(e)
            }
        }
    }

    companion object {
        /** Deduces the element type of a collection;
         * same logic as [.toEnumerable].  */
        @Nullable
        private fun getElementType(clazz: Class): Type? {
            if (clazz.isArray()) {
                return clazz.getComponentType()
            }
            return if (Iterable::class.java.isAssignableFrom(clazz)) {
                Object::class.java
            } else null
            // not a collection/array/iterable
        }

        private fun toEnumerable(o: Object): Enumerable {
            if (o.getClass().isArray()) {
                return if (o is Array<Object>) {
                    Linq4j.asEnumerable(o as Array<Object?>)
                } else {
                    Linq4j.asEnumerable(Primitive.asList(o))
                }
            }
            if (o is Iterable) {
                return Linq4j.asEnumerable(o as Iterable)
            }
            throw RuntimeException("Cannot convert " + o.getClass().toString() + " into a Enumerable")
        }
    }
}
