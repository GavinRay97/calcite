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

import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Pair
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List
import java.util.NavigableMap
import java.util.TreeMap

/**
 * Planner rule that removes constant keys from an
 * [org.apache.calcite.rel.core.Aggregate].
 *
 *
 * Constant fields are deduced using
 * [RelMetadataQuery.getPulledUpPredicates]; the input does not
 * need to be a [org.apache.calcite.rel.core.Project].
 *
 *
 * This rule never removes the last column, because `Aggregate([])`
 * returns 1 row even if its input is empty.
 *
 *
 * Since the transformed relational expression has to match the original
 * relational expression, the constants are placed in a projection above the
 * reduced aggregate. If those constants are not used, another rule will remove
 * them from the project.
 */
@Value.Enclosing
class AggregateProjectPullUpConstantsRule
/** Creates an AggregateProjectPullUpConstantsRule.  */
protected constructor(config: Config?) : RelRule<AggregateProjectPullUpConstantsRule.Config?>(config),
    TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        inputClass: Class<out RelNode?>?,
        relBuilderFactory: RelBuilderFactory?, description: String?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .withDescription(description)
            .`as`(Config::class.java)
            .withOperandFor(aggregateClass, inputClass)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val input: RelNode = call.rel(1)
        val groupCount: Int = aggregate.getGroupCount()
        if (groupCount == 1) {
            // No room for optimization since we cannot convert from non-empty
            // GROUP BY list to the empty one.
            return
        }
        val rexBuilder: RexBuilder = aggregate.getCluster().getRexBuilder()
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val predicates: RelOptPredicateList = mq.getPulledUpPredicates(aggregate.getInput())
        if (RelOptPredicateList.isEmpty(predicates)) {
            return
        }
        val map: NavigableMap<Integer, RexNode> = TreeMap()
        for (key in aggregate.getGroupSet()) {
            val ref: RexInputRef = rexBuilder.makeInputRef(aggregate.getInput(), key)
            if (predicates.constantMap.containsKey(ref)) {
                map.put(key, predicates.constantMap.get(ref))
            }
        }

        // None of the group expressions are constant. Nothing to do.
        if (map.isEmpty()) {
            return
        }
        if (groupCount == map.size()) {
            // At least a single item in group by is required.
            // Otherwise "GROUP BY 1, 2" might be altered to "GROUP BY ()".
            // Removing of the first element is not optimal here,
            // however it will allow us to use fast path below (just trim
            // groupCount).
            map.remove(map.navigableKeySet().first())
        }
        var newGroupSet: ImmutableBitSet = aggregate.getGroupSet()
        for (key in map.keySet()) {
            newGroupSet = newGroupSet.clear(key)
        }
        val newGroupCount: Int = newGroupSet.cardinality()

        // If the constants are on the trailing edge of the group list, we just
        // reduce the group count.
        val relBuilder: RelBuilder = call.builder()
        relBuilder.push(input)

        // Clone aggregate calls.
        val newAggCalls: List<AggregateCall> = ArrayList()
        for (aggCall in aggregate.getAggCallList()) {
            newAggCalls.add(
                aggCall.adaptTo(
                    input, aggCall.getArgList(), aggCall.filterArg,
                    groupCount, newGroupCount
                )
            )
        }
        relBuilder.aggregate(relBuilder.groupKey(newGroupSet), newAggCalls)

        // Create a projection back again.
        val projects: List<Pair<RexNode, String>> = ArrayList()
        var source = 0
        for (field in aggregate.getRowType().getFieldList()) {
            var expr: RexNode
            val i: Int = field.getIndex()
            if (i >= groupCount) {
                // Aggregate expressions' names and positions are unchanged.
                expr = relBuilder.field(i - map.size())
            } else {
                val pos: Int = aggregate.getGroupSet().nth(i)
                val rexNode: RexNode = map.get(pos)
                if (rexNode != null) {
                    // Re-generate the constant expression in the project.
                    val originalType: RelDataType = aggregate.getRowType().getFieldList().get(projects.size()).getType()
                    expr = if (!originalType.equals(rexNode.getType())) {
                        rexBuilder.makeCast(originalType, rexNode, true)
                    } else {
                        rexNode
                    }
                } else {
                    // Project the aggregation expression, in its original
                    // position.
                    expr = relBuilder.field(source)
                    ++source
                }
            }
            projects.add(Pair.of(expr, field.getName()))
        }
        relBuilder.project(Pair.left(projects), Pair.right(projects)) // inverse
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateProjectPullUpConstantsRule? {
            return AggregateProjectPullUpConstantsRule(this)
        }

        @Override
        @Value.Default
        fun operandSupplier(): OperandTransform? {
            return OperandTransform { b0 ->
                b0.operand(LogicalAggregate::class.java)
                    .predicate(Aggregate::isSimple)
                    .oneInput { b1 -> b1.operand(LogicalProject::class.java).anyInputs() }
            }
        }

        /** Defines an operand tree for the given classes.
         *
         * @param aggregateClass Aggregate class
         * @param inputClass Input class, such as [LogicalProject]
         */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            inputClass: Class<out RelNode?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass)
                    .predicate(Aggregate::isSimple)
                    .oneInput { b1 -> b1.operand(inputClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateProjectPullUpConstantsRule.Config.of()
        }
    }
}
