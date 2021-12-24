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
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.tools.RelBuilder

/**
 * Rule to convert an
 * [inner join][org.apache.calcite.rel.core.Join] to a
 * [filter][org.apache.calcite.rel.core.Filter] on top of a
 * [cartesian inner join][org.apache.calcite.rel.core.Join].
 *
 *
 * One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join. It also makes
 * the `FennelCartesianJoinRule` applicable.
 *
 *
 * The constructor is parameterized to allow any sub-class of
 * [org.apache.calcite.rel.core.Join].
 */
abstract class AbstractJoinExtractFilterRule
/** Creates an AbstractJoinExtractFilterRule.  */
protected constructor(config: Config?) : RelRule<AbstractJoinExtractFilterRule.Config?>(config), TransformationRule {
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)
        if (join.getJoinType() !== JoinRelType.INNER) {
            return
        }
        if (join.getCondition().isAlwaysTrue()) {
            return
        }
        if (!join.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return
        }
        val builder: RelBuilder = call.builder()

        // NOTE jvs 14-Mar-2006:  See JoinCommuteRule for why we
        // preserve attribute semiJoinDone here.
        val cartesianJoin: RelNode = join.copy(
            join.getTraitSet(),
            builder.literal(true),
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone()
        )
        builder.push(cartesianJoin)
            .filter(join.getCondition())
        call.transformTo(builder.build())
    }

    /** Rule configuration.  */
    interface Config : RelRule.Config
}
