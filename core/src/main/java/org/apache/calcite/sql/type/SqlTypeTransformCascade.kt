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
 * Strategy to infer the type of an operator call from the type of the operands
 * by using one [SqlReturnTypeInference] rule and a combination of
 * [SqlTypeTransform]s.
 */
class SqlTypeTransformCascade(
    rule: SqlReturnTypeInference?,
    vararg transforms: SqlTypeTransform?
) : SqlReturnTypeInference {
    //~ Instance fields --------------------------------------------------------
    private val rule: SqlReturnTypeInference
    private val transforms: ImmutableList<SqlTypeTransform>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlTypeTransformCascade from a rule and an array of one or more
     * transforms.
     */
    init {
        Preconditions.checkArgument(transforms.size > 0)
        this.rule = Objects.requireNonNull(rule, "rule")
        this.transforms = ImmutableList.copyOf(transforms)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    override fun inferReturnType(
        opBinding: SqlOperatorBinding?
    ): RelDataType? {
        var ret: RelDataType = rule.inferReturnType(opBinding)
            ?: // inferReturnType may return null; transformType does not accept or
            // return null types
            return null
        for (transform in transforms) {
            ret = transform.transformType(opBinding, ret)
        }
        return ret
    }
}
