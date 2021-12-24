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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * How a row is represented as a Java value.
 */
enum class JavaRowFormat {
    CUSTOM {
        @Override
        override fun javaRowClass(
            typeFactory: JavaTypeFactory,
            type: RelDataType
        ): Type {
            assert(type.getFieldCount() > 1)
            return typeFactory.getJavaClass(type)
        }

        @Override
        override fun javaFieldClass(
            typeFactory: JavaTypeFactory, type: RelDataType,
            index: Int
        ): Type {
            return typeFactory.getJavaClass(type.getFieldList().get(index).getType())
        }

        @Override
        override fun record(
            javaRowClass: Type, expressions: List<Expression?>
        ): Expression {
            return when (expressions.size()) {
                0 -> {
                    assert(javaRowClass === Unit::class.java)
                    Expressions.field(null, javaRowClass, "INSTANCE")
                }
                else -> Expressions.new_(javaRowClass, expressions)
            }
        }

        @Override
        override fun field(
            expression: Expression, field: Int,
            @Nullable fromType: Type?, fieldType: Type?
        ): MemberExpression {
            val type: Type = expression.getType()
            return if (type is Types.RecordType) {
                val recordType: Types.RecordType = type as Types.RecordType
                val recordField: Types.RecordField = recordType.getRecordFields().get(field)
                Expressions.field(
                    expression, recordField.getDeclaringClass(),
                    recordField.getName()
                )
            } else {
                Expressions.field(expression, Types.nthField(field, type))
            }
        }
    },
    SCALAR {
        @Override
        override fun javaRowClass(
            typeFactory: JavaTypeFactory,
            type: RelDataType
        ): Type {
            assert(type.getFieldCount() === 1)
            return typeFactory.getJavaClass(
                type.getFieldList().get(0).getType()
            )
        }

        @Override
        override fun javaFieldClass(
            typeFactory: JavaTypeFactory, type: RelDataType,
            index: Int
        ): Type {
            return javaRowClass(typeFactory, type)
        }

        @Override
        override fun record(javaRowClass: Type?, expressions: List<Expression?>): Expression? {
            assert(expressions.size() === 1)
            return expressions[0]
        }

        @Override
        override fun field(
            expression: Expression, field: Int, @Nullable fromType: Type?,
            fieldType: Type?
        ): Expression {
            assert(field == 0)
            return expression
        }
    },

    /** A list that is comparable and immutable. Useful for records with 0 fields
     * (empty list is a good singleton) but sometimes also for records with 2 or
     * more fields that you need to be comparable, say as a key in a lookup.  */
    LIST {
        @Override
        override fun javaRowClass(
            typeFactory: JavaTypeFactory?,
            type: RelDataType?
        ): Type {
            return FlatLists.ComparableList::class.java
        }

        @Override
        override fun javaFieldClass(
            typeFactory: JavaTypeFactory?, type: RelDataType?,
            index: Int
        ): Type {
            return Object::class.java
        }

        @Override
        override fun record(
            javaRowClass: Type?, expressions: List<Expression?>
        ): Expression {
            return when (expressions.size()) {
                0 -> Expressions.field(
                    null,
                    FlatLists::class.java,
                    "COMPARABLE_EMPTY_LIST"
                )
                2 -> Expressions.convert_(
                    Expressions.call(
                        List::class.java,
                        null,
                        BuiltInMethod.LIST2.method,
                        expressions
                    ),
                    List::class.java
                )
                3 -> Expressions.convert_(
                    Expressions.call(
                        List::class.java,
                        null,
                        BuiltInMethod.LIST3.method,
                        expressions
                    ),
                    List::class.java
                )
                4 -> Expressions.convert_(
                    Expressions.call(
                        List::class.java,
                        null,
                        BuiltInMethod.LIST4.method,
                        expressions
                    ),
                    List::class.java
                )
                5 -> Expressions.convert_(
                    Expressions.call(
                        List::class.java,
                        null,
                        BuiltInMethod.LIST5.method,
                        expressions
                    ),
                    List::class.java
                )
                6 -> Expressions.convert_(
                    Expressions.call(
                        List::class.java,
                        null,
                        BuiltInMethod.LIST6.method,
                        expressions
                    ),
                    List::class.java
                )
                else -> Expressions.convert_(
                    Expressions.call(
                        List::class.java,
                        null,
                        BuiltInMethod.LIST_N.method,
                        Expressions.newArrayInit(
                            Comparable::class.java,
                            expressions
                        )
                    ),
                    List::class.java
                )
            }
        }

        @Override
        override fun field(
            expression: Expression?, field: Int, @Nullable fromType: Type?,
            fieldType: Type?
        ): Expression? {
            var fromType: Type? = fromType
            val e: MethodCallExpression = Expressions.call(
                expression,
                BuiltInMethod.LIST_GET.method, Expressions.constant(field)
            )
            if (fromType == null) {
                fromType = e.getType()
            }
            return EnumUtils.convert(e, fromType, fieldType)
        }
    },

    /**
     * See [org.apache.calcite.interpreter.Row].
     */
    ROW {
        @Override
        override fun javaRowClass(typeFactory: JavaTypeFactory?, type: RelDataType?): Type {
            return Row::class.java
        }

        @Override
        override fun javaFieldClass(
            typeFactory: JavaTypeFactory?, type: RelDataType?,
            index: Int
        ): Type {
            return Object::class.java
        }

        @Override
        override fun record(
            javaRowClass: Type?,
            expressions: List<Expression?>?
        ): Expression {
            return Expressions.call(BuiltInMethod.ROW_AS_COPY.method, expressions)
        }

        @Override
        override fun field(
            expression: Expression?, field: Int, @Nullable fromType: Type?,
            fieldType: Type?
        ): Expression? {
            var fromType: Type? = fromType
            val e: Expression = Expressions.call(
                expression,
                BuiltInMethod.ROW_VALUE.method, Expressions.constant(field)
            )
            if (fromType == null) {
                fromType = e.getType()
            }
            return EnumUtils.convert(e, fromType, fieldType)
        }
    },
    ARRAY {
        @Override
        override fun javaRowClass(
            typeFactory: JavaTypeFactory?,
            type: RelDataType?
        ): Type {
            return Array<Object>::class.java
        }

        @Override
        override fun javaFieldClass(
            typeFactory: JavaTypeFactory?, type: RelDataType?,
            index: Int
        ): Type {
            return Object::class.java
        }

        @Override
        override fun record(javaRowClass: Type?, expressions: List<Expression?>?): Expression {
            return Expressions.newArrayInit(Object::class.java, expressions)
        }

        @Override
        override fun comparer(): Expression {
            return Expressions.call(BuiltInMethod.ARRAY_COMPARER.method)
        }

        @Override
        override fun field(
            expression: Expression?, field: Int, @Nullable fromType: Type?,
            fieldType: Type?
        ): Expression? {
            var fromType: Type? = fromType
            val e: IndexExpression = Expressions.arrayIndex(
                expression,
                Expressions.constant(field)
            )
            if (fromType == null) {
                fromType = e.getType()
            }
            return EnumUtils.convert(e, fromType, fieldType)
        }
    };

    fun optimize(rowType: RelDataType): JavaRowFormat {
        return when (rowType.getFieldCount()) {
            0 -> LIST
            1 -> SCALAR
            else -> {
                if (this === SCALAR) {
                    LIST
                } else this
            }
        }
    }

    abstract fun javaRowClass(typeFactory: JavaTypeFactory?, type: RelDataType?): Type

    /**
     * Returns the java class that is used to physically store the given field.
     * For instance, a non-null int field can still be stored in a field of type
     * `Object.class` in [JavaRowFormat.ARRAY] case.
     *
     * @param typeFactory type factory to resolve java types
     * @param type row type
     * @param index field index
     * @return java type used to store the field
     */
    abstract fun javaFieldClass(
        typeFactory: JavaTypeFactory?, type: RelDataType?,
        index: Int
    ): Type

    abstract fun record(
        javaRowClass: Type?, expressions: List<Expression?>?
    ): Expression

    @Nullable
    open fun comparer(): Expression? {
        return null
    }

    /** Returns a reference to a particular field.
     *
     *
     * `fromType` may be null; if null, uses the natural type of the
     * field.
     */
    abstract fun field(
        expression: Expression?, field: Int,
        @Nullable fromType: Type?, fieldType: Type?
    ): Expression
}
