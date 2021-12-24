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
 * Implementation of [org.apache.calcite.schema.ScalarFunction].
 */
class ScalarFunctionImpl private constructor(method: Method, implementor: CallImplementor) :
    ReflectiveFunctionBase(method), ScalarFunction, ImplementableFunction {
    private val implementor: CallImplementor

    /** Private constructor.  */
    init {
        this.implementor = implementor
    }

    @Override
    fun getReturnType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.createJavaType(method.getReturnType())
    }

    @Override
    fun getImplementor(): CallImplementor {
        return implementor
    }

    fun getReturnType(
        typeFactory: RelDataTypeFactory,
        opBinding: SqlOperatorBinding
    ): RelDataType {
        // Strict and semi-strict functions can return null even if their Java
        // functions return a primitive type. Because when one of their arguments
        // is null, they won't even be called.
        val returnType: RelDataType = getReturnType(typeFactory)
        when (getNullPolicy(method)) {
            STRICT -> for (type in opBinding.collectOperandTypes()) {
                if (type.isNullable()) {
                    return typeFactory.createTypeWithNullability(returnType, true)
                }
            }
            SEMI_STRICT -> return typeFactory.createTypeWithNullability(returnType, true)
            else -> {}
        }
        return returnType
    }

    companion object {
        /**
         * Creates [org.apache.calcite.schema.ScalarFunction] for each method in
         * a given class.
         */
        @Deprecated // to be removed before 2.0
        fun createAll(
            clazz: Class<*>
        ): ImmutableMultimap<String, ScalarFunction> {
            val builder: ImmutableMultimap.Builder<String, ScalarFunction> = ImmutableMultimap.builder()
            for (method in clazz.getMethods()) {
                if (method.getDeclaringClass() === Object::class.java) {
                    continue
                }
                if (!Modifier.isStatic(method.getModifiers())
                    && !classHasPublicZeroArgsConstructor(clazz)
                ) {
                    continue
                }
                val function: ScalarFunction = create(method)
                builder.put(method.getName(), function)
            }
            return builder.build()
        }

        /**
         * Returns a map of all functions based on the methods in a given class.
         * It is keyed by method names and maps to both
         * [org.apache.calcite.schema.ScalarFunction]
         * and [org.apache.calcite.schema.TableFunction].
         */
        fun functions(clazz: Class<*>): ImmutableMultimap<String, Function> {
            val builder: ImmutableMultimap.Builder<String, Function> = ImmutableMultimap.builder()
            for (method in clazz.getMethods()) {
                if (method.getDeclaringClass() === Object::class.java) {
                    continue
                }
                if (!Modifier.isStatic(method.getModifiers())
                    && !classHasPublicZeroArgsConstructor(clazz)
                ) {
                    continue
                }
                val tableFunction: TableFunction = TableFunctionImpl.create(method)
                if (tableFunction != null) {
                    builder.put(method.getName(), tableFunction)
                } else {
                    val function: ScalarFunction = create(method)
                    builder.put(method.getName(), function)
                }
            }
            return builder.build()
        }

        /**
         * Creates [org.apache.calcite.schema.ScalarFunction] from given class.
         *
         *
         * If a method of the given name is not found or it does not suit,
         * returns `null`.
         *
         * @param clazz class that is used to implement the function
         * @param methodName Method name (typically "eval")
         * @return created [ScalarFunction] or null
         */
        @Nullable
        fun create(clazz: Class<*>, methodName: String?): ScalarFunction? {
            val method: Method = findMethod(clazz, methodName) ?: return null
            return create(method)
        }

        /**
         * Creates [org.apache.calcite.schema.ScalarFunction] from given method.
         * When `eval` method does not suit, `null` is returned.
         *
         * @param method method that is used to implement the function
         * @return created [ScalarFunction] or null
         */
        fun create(method: Method): ScalarFunction {
            if (!Modifier.isStatic(method.getModifiers())) {
                val clazz: Class<*> = method.getDeclaringClass()
                if (!classHasPublicZeroArgsConstructor(clazz)
                    && !classHasPublicFunctionContextConstructor(clazz)
                ) {
                    throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex()
                }
            }
            val implementor: CallImplementor = createImplementor(method)
            return ScalarFunctionImpl(method, implementor)
        }

        /**
         * Creates unsafe version of [ScalarFunction] from any method. The method
         * does not need to be static or belong to a class with default constructor. It is
         * the responsibility of the underlying engine to initialize the UDF object that
         * contain the method.
         *
         * @param method method that is used to implement the function
         */
        fun createUnsafe(method: Method): ScalarFunction {
            val implementor: CallImplementor = createImplementor(method)
            return ScalarFunctionImpl(method, implementor)
        }

        private fun createImplementor(method: Method): CallImplementor {
            val nullPolicy: NullPolicy = getNullPolicy(method)
            return RexImpTable.createImplementor(
                ReflectiveCallNotNullImplementor(method), nullPolicy, false
            )
        }

        private fun getNullPolicy(m: Method): NullPolicy {
            return if (m.getAnnotation(Strict::class.java) != null) {
                NullPolicy.STRICT
            } else if (m.getAnnotation(SemiStrict::class.java) != null) {
                NullPolicy.SEMI_STRICT
            } else if (m.getDeclaringClass().getAnnotation(Strict::class.java) != null) {
                NullPolicy.STRICT
            } else if (m.getDeclaringClass().getAnnotation(SemiStrict::class.java) != null) {
                NullPolicy.SEMI_STRICT
            } else {
                NullPolicy.NONE
            }
        }
    }
}
