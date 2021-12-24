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
package org.apache.calcite.schema.impl

import org.apache.calcite.adapter.enumerable.CallImplementor

/**
 * Implementation of [org.apache.calcite.schema.TableFunction] based on a
 * method.
 */
class TableFunctionImpl private constructor(method: Method, implementor: CallImplementor) :
    ReflectiveFunctionBase(method), TableFunction, ImplementableFunction {
    private val implementor: CallImplementor

    /** Private constructor; use [.create].  */
    init {
        this.implementor = implementor
    }

    @Override
    fun getRowType(
        typeFactory: RelDataTypeFactory?,
        arguments: List<Object?>
    ): RelDataType {
        return apply(arguments).getRowType(typeFactory)
    }

    @Override
    fun getElementType(arguments: List<Object?>): Type {
        val table: Table = apply(arguments)
        if (table is QueryableTable) {
            val queryableTable: QueryableTable = table as QueryableTable
            return queryableTable.getElementType()
        } else if (table is ScannableTable) {
            return Array<Object>::class.java
        }
        throw AssertionError(
            "Invalid table class: " + table + " "
                    + table.getClass()
        )
    }

    @Override
    fun getImplementor(): CallImplementor {
        return implementor
    }

    private fun apply(arguments: List<Object?>): Table {
        return try {
            var o: Object? = null
            if (!Modifier.isStatic(method.getModifiers())) {
                val constructor: Constructor<*> = method.getDeclaringClass().getConstructor()
                o = constructor.newInstance()
            }
            val table: Object = method.invoke(o, arguments.toArray())
            requireNonNull(
                table
            ) { "got null from " + method.toString() + " with arguments " + arguments } as Table
        } catch (e: IllegalArgumentException) {
            throw RESOURCE.illegalArgumentForTableFunctionCall(
                method.toString(),
                Arrays.toString(method.getParameterTypes()),
                arguments.toString()
            ).ex(e)
        } catch (e: IllegalAccessException) {
            throw RuntimeException(e)
        } catch (e: InvocationTargetException) {
            throw RuntimeException(e)
        } catch (e: InstantiationException) {
            throw RuntimeException(e)
        } catch (e: NoSuchMethodException) {
            throw RuntimeException(e)
        }
    }

    companion object {
        /** Creates a [TableFunctionImpl] from a class, looking for an "eval"
         * method. Returns null if there is no such method.  */
        @Nullable
        fun create(clazz: Class<*>): TableFunction? {
            return create(clazz, "eval")
        }

        /** Creates a [TableFunctionImpl] from a class, looking for a method
         * with a given name. Returns null if there is no such method.  */
        @Nullable
        fun create(clazz: Class<*>, methodName: String?): TableFunction? {
            val method: Method = findMethod(clazz, methodName) ?: return null
            return create(method)
        }

        /** Creates a [TableFunctionImpl] from a method.  */
        @Nullable
        fun create(method: Method): TableFunction? {
            if (!Modifier.isStatic(method.getModifiers())) {
                val clazz: Class = method.getDeclaringClass()
                if (!classHasPublicZeroArgsConstructor(clazz)) {
                    throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex()
                }
            }
            val returnType: Class<*> = method.getReturnType()
            if (!QueryableTable::class.java.isAssignableFrom(returnType)
                && !ScannableTable::class.java.isAssignableFrom(returnType)
            ) {
                return null
            }
            val implementor: CallImplementor = createImplementor(method)
            return TableFunctionImpl(method, implementor)
        }

        private fun createImplementor(method: Method): CallImplementor {
            return RexImpTable.createImplementor(
                object : ReflectiveCallNotNullImplementor(method) {
                    @Override
                    fun implement(
                        translator: RexToLixTranslator,
                        call: RexCall, translatedOperands: List<Expression?>?
                    ): Expression {
                        var expr: Expression = super.implement(
                            translator, call,
                            translatedOperands
                        )
                        val returnType: Class<*> = method.getReturnType()
                        expr = if (QueryableTable::class.java.isAssignableFrom(returnType)) {
                            val queryable: Expression = Expressions.call(
                                Expressions.convert_(expr, QueryableTable::class.java),
                                BuiltInMethod.QUERYABLE_TABLE_AS_QUERYABLE.method,
                                Expressions.call(
                                    translator.getRoot(),
                                    BuiltInMethod.DATA_CONTEXT_GET_QUERY_PROVIDER.method
                                ),
                                Expressions.constant(null, SchemaPlus::class.java),
                                Expressions.constant(call.getOperator().getName(), String::class.java)
                            )
                            Expressions.call(
                                queryable,
                                BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method
                            )
                        } else {
                            Expressions.call(
                                expr,
                                BuiltInMethod.SCANNABLE_TABLE_SCAN.method,
                                translator.getRoot()
                            )
                        }
                        return expr
                    }
                }, NullPolicy.NONE, false
            )
        }
    }
}
