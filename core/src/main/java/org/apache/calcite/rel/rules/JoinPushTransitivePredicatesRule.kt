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

import org.apache.calcite.plan.Contexts

/**
 * Planner rule that infers predicates from on a
 * [org.apache.calcite.rel.core.Join] and creates
 * [org.apache.calcite.rel.core.Filter]s if those predicates can be pushed
 * to its inputs.
 *
 *
 * Uses [org.apache.calcite.rel.metadata.RelMdPredicates] to infer
 * the predicates,
 * returns them in a [org.apache.calcite.plan.RelOptPredicateList]
 * and applies them appropriately.
 *
 * @see CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES
 */
@Value.Enclosing
class JoinPushTransitivePredicatesRule
/** Creates a JoinPushTransitivePredicatesRule.  */
protected constructor(config: Config?) : RelRule<JoinPushTransitivePredicatesRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        joinClass: Class<out Join?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(joinClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        joinClass: Class<out Join?>?,
        filterFactory: FilterFactory?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(RelBuilder.proto(Contexts.of(filterFactory)))
            .`as`(Config::class.java)
            .withOperandFor(joinClass)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val preds: RelOptPredicateList = mq.getPulledUpPredicates(join)
        if (preds.leftInferredPredicates.isEmpty()
            && preds.rightInferredPredicates.isEmpty()
        ) {
            return
        }
        val relBuilder: RelBuilder = call.builder()
        var left: RelNode = join.getLeft()
        if (preds.leftInferredPredicates.size() > 0) {
            val curr: RelNode = left
            left = relBuilder.push(left)
                .filter(preds.leftInferredPredicates).build()
            call.getPlanner().onCopy(curr, left)
        }
        var right: RelNode = join.getRight()
        if (preds.rightInferredPredicates.size() > 0) {
            val curr: RelNode = right
            right = relBuilder.push(right)
                .filter(preds.rightInferredPredicates).build()
            call.getPlanner().onCopy(curr, right)
        }
        val newRel: RelNode = join.copy(
            join.getTraitSet(), join.getCondition(),
            left, right, join.getJoinType(), join.isSemiJoinDone()
        )
        call.getPlanner().onCopy(join, newRel)
        call.transformTo(newRel)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinPushTransitivePredicatesRule? {
            return JoinPushTransitivePredicatesRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b -> b.operand(joinClass).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableJoinPushTransitivePredicatesRule.Config.of()
                .withOperandFor(Join::class.java)
        }
    }
}
