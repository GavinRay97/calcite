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

/**
 * Rule to add a semi-join into a join. Transformation is as follows:
 *
 *
 * LogicalJoin(X, Y)  LogicalJoin(SemiJoin(X, Y), Y)
 *
 *
 * Can be configured to match any sub-class of
 * [org.apache.calcite.rel.core.Join], not just
 * [org.apache.calcite.rel.logical.LogicalJoin].
 *
 * @see CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN
 */
@Value.Enclosing
class JoinAddRedundantSemiJoinRule
/** Creates a JoinAddRedundantSemiJoinRule.  */
protected constructor(config: Config?) : RelRule<JoinAddRedundantSemiJoinRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out Join?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(clazz)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val origJoinRel: Join = call.rel(0)
        if (origJoinRel.isSemiJoinDone()) {
            return
        }

        // can't process outer joins using semi-joins
        if (origJoinRel.getJoinType() !== JoinRelType.INNER) {
            return
        }

        // determine if we have a valid join condition
        val joinInfo: JoinInfo = origJoinRel.analyzeCondition()
        if (joinInfo.leftKeys.size() === 0) {
            return
        }
        val semiJoin: RelNode = LogicalJoin.create(
            origJoinRel.getLeft(),
            origJoinRel.getRight(),
            ImmutableList.of(),
            origJoinRel.getCondition(),
            ImmutableSet.of(),
            JoinRelType.SEMI
        )
        val newJoinRel: RelNode = origJoinRel.copy(
            origJoinRel.getTraitSet(),
            origJoinRel.getCondition(),
            semiJoin,
            origJoinRel.getRight(),
            JoinRelType.INNER,
            true
        )
        call.transformTo(newJoinRel)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinAddRedundantSemiJoinRule? {
            return JoinAddRedundantSemiJoinRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b -> b.operand(joinClass).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableJoinAddRedundantSemiJoinRule.Config.of()
                .withOperandFor(LogicalJoin::class.java)
        }
    }
}
