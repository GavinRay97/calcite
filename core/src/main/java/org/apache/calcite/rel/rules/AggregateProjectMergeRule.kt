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
 * Planner rule that recognizes a [org.apache.calcite.rel.core.Aggregate]
 * on top of a [org.apache.calcite.rel.core.Project] and if possible
 * aggregate through the project or removes the project.
 *
 *
 * This is only possible when the grouping expressions and arguments to
 * the aggregate functions are field references (i.e. not expressions).
 *
 *
 * In some cases, this rule has the effect of trimming: the aggregate will
 * use fewer columns than the project did.
 *
 * @see CoreRules.AGGREGATE_PROJECT_MERGE
 */
@Value.Enclosing
class AggregateProjectMergeRule
/** Creates an AggregateProjectMergeRule.  */
protected constructor(config: Config?) : RelRule<AggregateProjectMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        projectClass: Class<out Project?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        CoreRules.AGGREGATE_PROJECT_MERGE.config
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(aggregateClass, projectClass)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val project: Project = call.rel(1)
        val x: RelNode? = apply(call, aggregate, project)
        if (x != null) {
            call.transformTo(x)
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateProjectMergeRule? {
            return AggregateProjectMergeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            projectClass: Class<out Project?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass).oneInput { b1 -> b1.operand(projectClass).anyInputs() }
            }.`as`(
                Config::class.java
            )
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateProjectMergeRule.Config.of()
                .withOperandFor(Aggregate::class.java, Project::class.java)
        }
    }

    companion object {
        @Nullable
        fun apply(
            call: RelOptRuleCall, aggregate: Aggregate,
            project: Project
        ): RelNode? {
            // Find all fields which we need to be straightforward field projections.
            val interestingFields: Set<Integer> = RelOptUtil.getAllFields(aggregate)

            // Build the map from old to new; abort if any entry is not a
            // straightforward field projection.
            val map: Map<Integer, Integer> = HashMap()
            for (source in interestingFields) {
                val rex: RexNode = project.getProjects().get(source) as? RexInputRef ?: return null
                map.put(source, (rex as RexInputRef).getIndex())
            }
            val newGroupSet: ImmutableBitSet = aggregate.getGroupSet().permute(map)
            var newGroupingSets: ImmutableList<ImmutableBitSet?>? = null
            if (aggregate.getGroupType() !== Group.SIMPLE) {
                newGroupingSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
                    ImmutableBitSet.permute(aggregate.getGroupSets(), map)
                )
            }
            val aggCalls: ImmutableList.Builder<AggregateCall> = ImmutableList.builder()
            val sourceCount: Int = aggregate.getInput().getRowType().getFieldCount()
            val targetCount: Int = project.getInput().getRowType().getFieldCount()
            val targetMapping: Mappings.TargetMapping = Mappings.target(map, sourceCount, targetCount)
            for (aggregateCall in aggregate.getAggCallList()) {
                aggCalls.add(aggregateCall.transform(targetMapping))
            }
            val newAggregate: Aggregate = aggregate.copy(
                aggregate.getTraitSet(), project.getInput(),
                newGroupSet, newGroupingSets, aggCalls.build()
            )

            // Add a project if the group set is not in the same order or
            // contains duplicates.
            val relBuilder: RelBuilder = call.builder()
            relBuilder.push(newAggregate)
            val newKeys: List<Integer> = Util.transform(
                aggregate.getGroupSet().asList()
            ) { key ->
                requireNonNull(
                    map[key]
                ) { "no value found for key $key in $map" }
            }
            if (!newKeys.equals(newGroupSet.asList())) {
                val posList: List<Integer> = ArrayList()
                for (newKey in newKeys) {
                    posList.add(newGroupSet.indexOf(newKey))
                }
                for (i in newAggregate.getGroupCount() until newAggregate.getRowType().getFieldCount()) {
                    posList.add(i)
                }
                relBuilder.project(relBuilder.fields(posList))
            }
            return relBuilder.build()
        }
    }
}
