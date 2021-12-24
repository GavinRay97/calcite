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
 * Planner rule that replaces `IS NOT DISTINCT FROM`
 * in a [Filter] with logically equivalent operations.
 *
 * @see org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
 *
 * @see CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM
 *
 * @see RelBuilder.isDistinctFrom
 *
 * @see RelBuilder.isNotDistinctFrom
 */
@Value.Enclosing
class FilterRemoveIsNotDistinctFromRule
/** Creates a FilterRemoveIsNotDistinctFromRule.  */
internal constructor(config: Config?) : RelRule<FilterRemoveIsNotDistinctFromRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val oldFilter: Filter = call.rel(0)
        val oldFilterCond: RexNode = oldFilter.getCondition()
        if (RexUtil.findOperatorCall(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                oldFilterCond
            )
            == null
        ) {
            // no longer contains isNotDistinctFromOperator
            return
        }

        // Now replace all the "a isNotDistinctFrom b"
        // with the RexNode given by RelOptUtil.isDistinctFrom() method
        val rewriteShuttle = RemoveIsNotDistinctFromRexShuttle(
            oldFilter.getCluster().getRexBuilder()
        )
        val relBuilder: RelBuilder = call.builder()
        val newFilterRel: RelNode = relBuilder
            .push(oldFilter.getInput())
            .filter(oldFilterCond.accept(rewriteShuttle))
            .build()
        call.transformTo(newFilterRel)
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Shuttle that removes 'x IS NOT DISTINCT FROM y' and converts it
     * to 'CASE WHEN x IS NULL THEN y IS NULL WHEN y IS NULL THEN x IS
     * NULL ELSE x = y END'.  */
    private class RemoveIsNotDistinctFromRexShuttle internal constructor(
        rexBuilder: RexBuilder
    ) : RexShuttle() {
        val rexBuilder: RexBuilder

        init {
            this.rexBuilder = rexBuilder
        }

        @Override
        fun visitCall(call: RexCall): RexNode {
            var newCall: RexNode = super.visitCall(call)
            if (call.getOperator()
                === SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
            ) {
                val tmpCall: RexCall = newCall as RexCall
                newCall = RelOptUtil.isDistinctFrom(
                    rexBuilder,
                    tmpCall.operands.get(0),
                    tmpCall.operands.get(1),
                    true
                )
            }
            return newCall
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterRemoveIsNotDistinctFromRule? {
            return FilterRemoveIsNotDistinctFromRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterRemoveIsNotDistinctFromRule.Config.of()
                .withOperandSupplier { b -> b.operand(Filter::class.java).anyInputs() }
        }
    }
}
