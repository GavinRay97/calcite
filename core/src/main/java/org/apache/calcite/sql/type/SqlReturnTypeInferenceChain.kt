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
 * by using a series of [SqlReturnTypeInference] rules in a given order.
 * If a rule fails to find a return type (by returning NULL), next rule is tried
 * until there are no more rules in which case NULL will be returned.
 */
class SqlReturnTypeInferenceChain internal constructor(vararg rules: SqlReturnTypeInference?) : SqlReturnTypeInference {
    //~ Instance fields --------------------------------------------------------
    private val rules: ImmutableList<SqlReturnTypeInference>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlReturnTypeInferenceChain from an array of rules.
     *
     *
     * Package-protected.
     * Use [org.apache.calcite.sql.type.ReturnTypes.chain].
     */
    init {
        Preconditions.checkArgument(rules.size > 1)
        this.rules = ImmutableList.copyOf(rules)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    override fun inferReturnType(opBinding: SqlOperatorBinding?): RelDataType? {
        for (rule in rules) {
            val ret: RelDataType = rule.inferReturnType(opBinding)
            if (ret != null) {
                return ret
            }
        }
        return null
    }
}
