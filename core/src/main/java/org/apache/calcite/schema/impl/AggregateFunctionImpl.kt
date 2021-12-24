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

import org.apache.calcite.adapter.enumerable.AggImplementor

/**
 * Implementation of [AggregateFunction] via user-defined class.
 * The class should implement `A init()`, `A add(A, V)`, and
 * `R result(A)` methods.
 * All the methods should be either static or instance.
 * Bonus point: when using non-static implementation, the aggregate object is
 * reused through the calculation, thus it can have aggregation-related state.
 */
class AggregateFunctionImpl private constructor(
    declaringClass: Class<*>,
    params: List<FunctionParameter>,
    valueTypes: List<Class<*>>,
    accumulatorType: Class<*>,
    resultType: Class<*>,
    initMethod: Method?,
    addMethod: Method?,
    @Nullable mergeMethod: Method?,
    @Nullable resultMethod: Method?
) : AggregateFunction, ImplementableAggFunction {
    val isStatic: Boolean
    val initMethod: Method
    val addMethod: Method

    @Nullable
    val mergeMethod: Method?

    @Nullable
    val resultMethod // may be null
            : Method?
    val valueTypes: ImmutableList<Class<*>>
    private val parameters: List<FunctionParameter>
    val accumulatorType: Class<*>
    val resultType: Class<*>
    val declaringClass: Class<*>

    /** Private constructor; use [.create].  */
    init {
        this.declaringClass = declaringClass
        this.valueTypes = ImmutableList.copyOf(valueTypes)
        parameters = params
        this.accumulatorType = accumulatorType
        this.resultType = resultType
        this.initMethod = Objects.requireNonNull(initMethod, "initMethod")
        this.addMethod = Objects.requireNonNull(addMethod, "addMethod")
        this.mergeMethod = mergeMethod
        this.resultMethod = resultMethod
        isStatic = Modifier.isStatic(initMethod.getModifiers())
        assert(resultMethod != null || accumulatorType === resultType)
    }

    @Override
    fun getParameters(): List<FunctionParameter> {
        return parameters
    }

    @Override
    fun getReturnType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.createJavaType(resultType)
    }

    @Override
    fun getImplementor(windowContext: Boolean): AggImplementor {
        return UserDefinedAggReflectiveImplementor(this)
    }

    companion object {
        /** Creates an aggregate function, or returns null.  */
        @Nullable
        fun create(clazz: Class<*>): AggregateFunctionImpl? {
            val initMethod: Method = ReflectiveFunctionBase.findMethod(clazz, "init")
            val addMethod: Method = ReflectiveFunctionBase.findMethod(clazz, "add")
            val mergeMethod: Method? = null // TODO:
            val resultMethod: Method = ReflectiveFunctionBase.findMethod(
                clazz, "result"
            )
            if (initMethod != null && addMethod != null) {
                // A is return type of init by definition
                val accumulatorType: Class<*> = initMethod.getReturnType()

                // R is return type of result by definition
                val resultType: Class<*> = if (resultMethod != null) resultMethod.getReturnType() else accumulatorType

                // V is remaining args of add by definition
                val addParamTypes: List<Class> = ImmutableList.copyOf(addMethod.getParameterTypes() as Array<Class?>)
                if (addParamTypes.isEmpty() || addParamTypes[0] !== accumulatorType) {
                    throw RESOURCE.firstParameterOfAdd(clazz.getName()).ex()
                }
                val params: ReflectiveFunctionBase.ParameterListBuilder = ReflectiveFunctionBase.builder()
                val valueTypes: ImmutableList.Builder<Class<*>> = ImmutableList.builder()
                for (i in 1 until addParamTypes.size()) {
                    val type: Class = addParamTypes[i]
                    val name: String = ReflectUtil.getParameterName(addMethod, i)
                    val optional: Boolean = ReflectUtil.isParameterOptional(addMethod, i)
                    params.add(type, name, optional)
                    valueTypes.add(type)
                }

                // A init()
                // A add(A, V)
                // A merge(A, A)
                // R result(A)

                // TODO: check add returns A
                // TODO: check merge returns A
                // TODO: check merge args are (A, A)
                // TODO: check result args are (A)
                return AggregateFunctionImpl(
                    clazz, params.build(),
                    valueTypes.build(), accumulatorType, resultType, initMethod,
                    addMethod, mergeMethod, resultMethod
                )
            }
            return null
        }
    }
}
