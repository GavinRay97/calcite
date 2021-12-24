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

import org.apache.calcite.linq4j.Ord

/** Planner rule that converts a
 * [LogicalJoin] relational expression
 * [enumerable calling convention][EnumerableConvention].
 * You may provide a custom config to convert other nodes that extend [Join].
 *
 * @see EnumerableJoinRule
 *
 * @see EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE
 */
internal class EnumerableMergeJoinRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    @Nullable
    fun convert(rel: RelNode): RelNode? {
        val join: Join = rel as Join
        val info: JoinInfo = join.analyzeCondition()
        if (!EnumerableMergeJoin.isMergeJoinSupported(join.getJoinType())) {
            // EnumerableMergeJoin only supports certain join types.
            return null
        }
        if (info.pairs().size() === 0) {
            // EnumerableMergeJoin CAN support cartesian join, but disable it for now.
            return null
        }
        val newInputs: List<RelNode> = ArrayList()
        val collations: List<RelCollation> = ArrayList()
        var offset = 0
        for (ord in Ord.zip(join.getInputs())) {
            var traits: RelTraitSet = ord.e.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
            if (!info.pairs().isEmpty()) {
                val fieldCollations: List<RelFieldCollation> = ArrayList()
                for (key in info.keys().get(ord.i)) {
                    fieldCollations.add(
                        RelFieldCollation(
                            key, RelFieldCollation.Direction.ASCENDING,
                            RelFieldCollation.NullDirection.LAST
                        )
                    )
                }
                val collation: RelCollation = RelCollations.of(fieldCollations)
                collations.add(RelCollations.shift(collation, offset))
                traits = traits.replace(collation)
            }
            newInputs.add(convert(ord.e, traits))
            offset += ord.e.getRowType().getFieldCount()
        }
        val left: RelNode = newInputs[0]
        val right: RelNode = newInputs[1]
        val cluster: RelOptCluster = join.getCluster()
        val newRel: RelNode
        var traitSet: RelTraitSet = join.getTraitSet()
            .replace(EnumerableConvention.INSTANCE)
        if (!collations.isEmpty()) {
            traitSet = traitSet.replace(collations)
        }
        // Re-arrange condition: first the equi-join elements, then the non-equi-join ones (if any);
        // this is not strictly necessary but it will be useful to avoid spurious errors in the
        // unit tests when verifying the plan.
        val rexBuilder: RexBuilder = join.getCluster().getRexBuilder()
        val equi: RexNode = info.getEquiCondition(left, right, rexBuilder)
        val condition: RexNode
        condition = if (info.isEqui()) {
            equi
        } else {
            val nonEqui: RexNode = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions)
            RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui))
        }
        newRel = EnumerableMergeJoin(
            cluster,
            traitSet,
            left,
            right,
            condition,
            join.getVariablesSet(),
            join.getJoinType()
        )
        return newRel
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalJoin::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableMergeJoinRule"
            )
            .withRuleFactory { config: Config? -> EnumerableMergeJoinRule(config) }
    }
}
