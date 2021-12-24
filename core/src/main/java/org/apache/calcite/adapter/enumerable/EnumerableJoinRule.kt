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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.plan.Convention

/** Planner rule that converts a
 * [LogicalJoin] relational expression
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].
 * You may provide a custom config to convert other nodes that extend [Join].
 *
 * @see EnumerableRules.ENUMERABLE_JOIN_RULE
 */
internal class EnumerableJoinRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val join: Join = rel as Join
        val newInputs: List<RelNode> = ArrayList()
        for (input in join.getInputs()) {
            if (input.getConvention() !is EnumerableConvention) {
                input = convert(
                    input,
                    input.getTraitSet()
                        .replace(EnumerableConvention.INSTANCE)
                )
            }
            newInputs.add(input)
        }
        val rexBuilder: RexBuilder = join.getCluster().getRexBuilder()
        val left: RelNode = newInputs[0]
        val right: RelNode = newInputs[1]
        val info: JoinInfo = join.analyzeCondition()

        // If the join has equiKeys (i.e. complete or partial equi-join),
        // create an EnumerableHashJoin, which supports all types of joins,
        // even if the join condition contains partial non-equi sub-conditions;
        // otherwise (complete non-equi-join), create an EnumerableNestedLoopJoin,
        // since a hash join strategy in this case would not be beneficial.
        val hasEquiKeys = (!info.leftKeys.isEmpty()
                && !info.rightKeys.isEmpty())
        if (hasEquiKeys) {
            // Re-arrange condition: first the equi-join elements, then the non-equi-join ones (if any);
            // this is not strictly necessary but it will be useful to avoid spurious errors in the
            // unit tests when verifying the plan.
            val equi: RexNode = info.getEquiCondition(left, right, rexBuilder)
            val condition: RexNode
            condition = if (info.isEqui()) {
                equi
            } else {
                val nonEqui: RexNode = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions)
                RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui))
            }
            return EnumerableHashJoin.create(
                left,
                right,
                condition,
                join.getVariablesSet(),
                join.getJoinType()
            )
        }
        return EnumerableNestedLoopJoin.create(
            left,
            right,
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalJoin::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableJoinRule"
            )
            .withRuleFactory { config: Config? -> EnumerableJoinRule(config) }
    }
}
