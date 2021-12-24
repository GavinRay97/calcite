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
 * Planner rule that pushes a [org.apache.calcite.rel.core.Filter]
 * past a [org.apache.calcite.rel.core.Aggregate].
 *
 * @see org.apache.calcite.rel.rules.AggregateFilterTransposeRule
 *
 * @see CoreRules.FILTER_AGGREGATE_TRANSPOSE
 */
@Value.Enclosing
class FilterAggregateTransposeRule
/** Creates a FilterAggregateTransposeRule.  */
protected constructor(config: Config?) : RelRule<FilterAggregateTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        filterClass: Class<out Filter?>?,
        relBuilderFactory: RelBuilderFactory?,
        aggregateClass: Class<out Aggregate?>?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(filterClass, aggregateClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        filterClass: Class<out Filter?>?,
        filterFactory: FilterFactory?,
        aggregateClass: Class<out Aggregate?>?
    ) : this(
        filterClass, RelBuilder.proto(Contexts.of(filterFactory)),
        aggregateClass
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filterRel: Filter = call.rel(0)
        val aggRel: Aggregate = call.rel(1)
        val conditions: List<RexNode> = RelOptUtil.conjunctions(filterRel.getCondition())
        val rexBuilder: RexBuilder = filterRel.getCluster().getRexBuilder()
        val origFields: List<RelDataTypeField> = aggRel.getRowType().getFieldList()
        val adjustments = IntArray(origFields.size())
        var j = 0
        for (i in aggRel.getGroupSet()) {
            adjustments[j] = i - j
            j++
        }
        val pushedConditions: List<RexNode> = ArrayList()
        val remainingConditions: List<RexNode> = ArrayList()
        for (condition in conditions) {
            val rCols: ImmutableBitSet = RelOptUtil.InputFinder.bits(condition)
            if (canPush(aggRel, rCols)) {
                pushedConditions.add(
                    condition.accept(
                        RexInputConverter(
                            rexBuilder, origFields,
                            aggRel.getInput(0).getRowType().getFieldList(),
                            adjustments
                        )
                    )
                )
            } else {
                remainingConditions.add(condition)
            }
        }
        val builder: RelBuilder = call.builder()
        var rel: RelNode = builder.push(aggRel.getInput()).filter(pushedConditions).build()
        if (rel === aggRel.getInput(0)) {
            return
        }
        rel = aggRel.copy(aggRel.getTraitSet(), ImmutableList.of(rel))
        rel = builder.push(rel).filter(remainingConditions).build()
        call.transformTo(rel)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterAggregateTransposeRule {
            return FilterAggregateTransposeRule(this)
        }

        /** Defines an operand tree for the given 2 classes.  */
        fun withOperandFor(
            filterClass: Class<out Filter?>?,
            aggregateClass: Class<out Aggregate?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass).oneInput { b1 -> b1.operand(aggregateClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        /** Defines an operand tree for the given 3 classes.  */
        fun withOperandFor(
            filterClass: Class<out Filter?>?,
            aggregateClass: Class<out Aggregate?>?,
            relClass: Class<out RelNode?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass)
                    .oneInput { b1 -> b1.operand(aggregateClass).oneInput { b2 -> b2.operand(relClass).anyInputs() } }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterAggregateTransposeRule.Config.of()
                .withOperandFor(Filter::class.java, Aggregate::class.java)
        }
    }

    companion object {
        private fun canPush(aggregate: Aggregate, rCols: ImmutableBitSet): Boolean {
            // If the filter references columns not in the group key, we cannot push
            val groupKeys: ImmutableBitSet = ImmutableBitSet.range(0, aggregate.getGroupSet().cardinality())
            if (!groupKeys.contains(rCols)) {
                return false
            }
            if (aggregate.getGroupType() !== Group.SIMPLE) {
                // If grouping sets are used, the filter can be pushed if
                // the columns referenced in the predicate are present in
                // all the grouping sets.
                for (groupingSet in aggregate.getGroupSets()) {
                    if (!groupingSet.contains(rCols)) {
                        return false
                    }
                }
            }
            return true
        }
    }
}
