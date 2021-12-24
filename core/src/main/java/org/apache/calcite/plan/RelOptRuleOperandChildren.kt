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
package org.apache.calcite.plan

import com.google.common.collect.ImmutableList
import java.util.List

/**
 * Children of a [org.apache.calcite.plan.RelOptRuleOperand] and the
 * policy for matching them.
 *
 *
 * Often created by calling one of the following methods:
 * [RelOptRule.some],
 * [RelOptRule.none],
 * [RelOptRule.any],
 * [RelOptRule.unordered].
 *
 */
@Deprecated // to be removed before 2.0
@Deprecated("Use {@link RelRule.OperandBuilder}")
class RelOptRuleOperandChildren(
    policy: RelOptRuleOperandChildPolicy,
    operands: List<RelOptRuleOperand?>?
) {
    val policy: RelOptRuleOperandChildPolicy
    val operands: ImmutableList<RelOptRuleOperand>

    init {
        this.policy = policy
        this.operands = ImmutableList.copyOf(operands)
    }

    companion object {
        val ANY_CHILDREN = RelOptRuleOperandChildren(
            RelOptRuleOperandChildPolicy.ANY,
            ImmutableList.of()
        )
        val LEAF_CHILDREN = RelOptRuleOperandChildren(
            RelOptRuleOperandChildPolicy.LEAF,
            ImmutableList.of()
        )
    }
}
