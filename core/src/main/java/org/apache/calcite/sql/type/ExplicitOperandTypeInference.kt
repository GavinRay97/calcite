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

import org.apache.calcite.rel.type.RelDataType

/**
 * ExplicitOperandTypeInferences implements [SqlOperandTypeInference] by
 * explicitly supplying a type for each parameter.
 */
class ExplicitOperandTypeInference internal constructor(paramTypes: ImmutableList<RelDataType?>) :
    SqlOperandTypeInference {
    //~ Instance fields --------------------------------------------------------
    private val paramTypes: ImmutableList<RelDataType>
    //~ Constructors -----------------------------------------------------------
    /** Use
     * [org.apache.calcite.sql.type.InferTypes.explicit].  */
    init {
        this.paramTypes = paramTypes
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun inferOperandTypes(
        callBinding: SqlCallBinding?,
        returnType: RelDataType?,
        operandTypes: Array<RelDataType?>
    ) {
        if (operandTypes.size != paramTypes.size()) {
            // This call does not match the inference strategy.
            // It's likely that we're just about to give a validation error.
            // Don't make a fuss, just give up.
            return
        }
        @SuppressWarnings("all") val unused: Array<RelDataType> = paramTypes.toArray(operandTypes)
    }
}
