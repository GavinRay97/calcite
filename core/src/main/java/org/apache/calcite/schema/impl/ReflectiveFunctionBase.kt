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

import org.apache.calcite.rel.type.RelDataType

/**
 * Implementation of a function that is based on a method.
 * This class mainly solves conversion of method parameter types to `List<FunctionParameter>` form.
 */
abstract class ReflectiveFunctionBase protected constructor(method: Method) : Function {
    /** Method that implements the function.  */
    val method: Method

    /** Types of parameter for the function call.  */
    val parameters: List<FunctionParameter>

    /**
     * Creates a ReflectiveFunctionBase.
     *
     * @param method Method that is used to get type information from
     */
    init {
        this.method = method
        parameters = builder().addMethodParameters(method).build()
    }

    /**
     * Returns the parameters of this function.
     *
     * @return Parameters; never null
     */
    @Override
    fun getParameters(): List<FunctionParameter> {
        return parameters
    }

    /** Helps build lists of
     * [org.apache.calcite.schema.FunctionParameter].  */
    class ParameterListBuilder {
        val builder: List<FunctionParameter> = ArrayList()
        fun build(): ImmutableList<FunctionParameter> {
            return ImmutableList.copyOf(builder)
        }

        fun add(type: Class<*>, name: String): ParameterListBuilder {
            return add(type, name, false)
        }

        fun add(
            type: Class<*>, name: String,
            optional: Boolean
        ): ParameterListBuilder {
            val ordinal: Int = builder.size()
            builder.add(
                object : FunctionParameter() {
                    @Override
                    override fun toString(): String {
                        return (ordinal.toString() + ": " + name + " " + type.getSimpleName()
                                + if (optional) "?" else "")
                    }

                    @get:Override
                    val ordinal: Int
                        get() = ordinal

                    @get:Override
                    val name: String
                        get() = name

                    @Override
                    fun getType(typeFactory: RelDataTypeFactory): RelDataType {
                        return typeFactory.createJavaType(type)
                    }

                    @get:Override
                    val isOptional: Boolean
                        get() = optional
                })
            return this
        }

        fun addMethodParameters(method: Method): ParameterListBuilder {
            val types: Array<Class<*>> = method.getParameterTypes()
            for (i in types.indices) {
                add(
                    types[i], ReflectUtil.getParameterName(method, i),
                    ReflectUtil.isParameterOptional(method, i)
                )
            }
            return this
        }
    }

    companion object {
        /**
         * Returns whether a class has a public constructor with zero arguments.
         *
         * @param clazz Class to verify
         * @return whether class has a public constructor with zero arguments
         */
        fun classHasPublicZeroArgsConstructor(clazz: Class<*>): Boolean {
            for (constructor in clazz.getConstructors()) {
                if (constructor.getParameterTypes().length === 0
                    && Modifier.isPublic(constructor.getModifiers())
                ) {
                    return true
                }
            }
            return false
        }

        /**
         * Returns whether a class has a public constructor with one argument
         * of type [FunctionContext].
         *
         * @param clazz Class to verify
         * @return whether class has a public constructor with one FunctionContext
         * argument
         */
        fun classHasPublicFunctionContextConstructor(clazz: Class<*>): Boolean {
            for (constructor in clazz.getConstructors()) {
                if (constructor.getParameterTypes().length === 1 && constructor.getParameterTypes()
                        .get(0) === FunctionContext::class.java && Modifier.isPublic(constructor.getModifiers())
                ) {
                    return true
                }
            }
            return false
        }

        /**
         * Finds a method in a given class by name.
         * @param clazz class to search method in
         * @param name name of the method to find
         * @return the first method with matching name or null when no method found
         */
        @Nullable
        fun findMethod(clazz: Class<*>, name: String?): Method? {
            for (method in clazz.getMethods()) {
                if (method.getName().equals(name) && !method.isBridge()) {
                    return method
                }
            }
            return null
        }

        /** Creates a ParameterListBuilder.  */
        fun builder(): ParameterListBuilder {
            return ParameterListBuilder()
        }
    }
}
