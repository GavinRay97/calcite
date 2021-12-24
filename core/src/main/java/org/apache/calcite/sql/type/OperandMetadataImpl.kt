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
package org.apache.calcite.sql.type

import org.apache.calcite.linq4j.function.Functions

/**
 * Operand type-checking strategy user-defined functions (including user-defined
 * aggregate functions, table functions, and table macros).
 *
 *
 * UDFs have a fixed number of parameters is fixed. Per
 * [SqlOperandMetadata], this interface provides the name and types of
 * each parameter.
 *
 * @see OperandTypes.operandMetadata
 */
class OperandMetadataImpl internal constructor(
    families: List<SqlTypeFamily?>?,
    paramTypesFactory: Function<RelDataTypeFactory?, List<RelDataType?>?>?,
    paramNameFn: IntFunction<String?>, optional: Predicate<Integer?>
) : FamilyOperandTypeChecker(families, optional), SqlOperandMetadata {
    private val paramTypesFactory: Function<RelDataTypeFactory, List<RelDataType>>
    private val paramNameFn: IntFunction<String>
    //~ Constructors -----------------------------------------------------------
    /** Package private. Create using [OperandTypes.operandMetadata].  */
    init {
        this.paramTypesFactory = Objects.requireNonNull(paramTypesFactory, "paramTypesFactory")
        this.paramNameFn = paramNameFn
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val isFixedParameters: Boolean
        get() = true

    @Override
    override fun paramTypes(typeFactory: RelDataTypeFactory?): List<RelDataType> {
        return paramTypesFactory.apply(typeFactory)
    }

    @Override
    override fun paramNames(): List<String> {
        return Functions.generate(families.size(), paramNameFn)
    }
}
