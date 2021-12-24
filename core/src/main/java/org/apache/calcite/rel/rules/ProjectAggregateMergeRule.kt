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

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexPermuteInputsShuttle
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexVisitorImpl
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.MappingType
import org.apache.calcite.util.mapping.Mappings
import org.immutables.value.Value
import java.math.BigDecimal
import java.util.ArrayList
import java.util.List
import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger

/**
 * Planner rule that matches a [Project] on a [Aggregate]
 * and projects away aggregate calls that are not used.
 *
 *
 * Also converts `COALESCE(SUM(x), 0)` to `SUM0(x)`.
 * This transformation is useful because there are cases where
 * [AggregateMergeRule] can merge `SUM0` but not `SUM`.
 *
 * @see CoreRules.PROJECT_AGGREGATE_MERGE
 */
@Value.Enclosing
class ProjectAggregateMergeRule
/** Creates a ProjectAggregateMergeRule.  */
protected constructor(config: Config?) : RelRule<ProjectAggregateMergeRule.Config?>(config), TransformationRule {
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: Project = call.rel(0)
        val aggregate: Aggregate = call.rel(1)
        val cluster: RelOptCluster = aggregate.getCluster()

        // Do a quick check. If all aggregate calls are used, and there are no CASE
        // expressions, there is nothing to do.
        val bits: ImmutableBitSet = RelOptUtil.InputFinder.bits(project.getProjects(), null)
        if (bits.contains(
                ImmutableBitSet.range(
                    aggregate.getGroupCount(),
                    aggregate.getRowType().getFieldCount()
                )
            )
            && kindCount(project.getProjects(), SqlKind.CASE) == 0
        ) {
            return
        }

        // Replace 'COALESCE(SUM(x), 0)' with 'SUM0(x)' wherever it occurs.
        // Add 'SUM0(x)' to the aggregate call list, if necessary.
        val aggCallList: List<AggregateCall> = ArrayList(aggregate.getAggCallList())
        val shuttle: RexShuttle = object : RexShuttle() {
            @Override
            fun visitCall(call: RexCall): RexNode {
                when (call.getKind()) {
                    CASE -> {
                        // Do we have "CASE(IS NOT NULL($0), CAST($0):INTEGER NOT NULL, 0)"?
                        val operands: List<RexNode> = call.operands
                        if (operands.size() === 3 && operands[0].getKind() === SqlKind.IS_NOT_NULL && ((operands[0] as RexCall).operands.get(
                                0
                            ).getKind()
                                    === SqlKind.INPUT_REF) && operands[1].getKind() === SqlKind.CAST && ((operands[1] as RexCall).operands.get(
                                0
                            ).getKind()
                                    === SqlKind.INPUT_REF) && operands[2].getKind() === SqlKind.LITERAL
                        ) {
                            val isNotNull: RexCall = operands[0] as RexCall
                            val ref0: RexInputRef = isNotNull.operands.get(0) as RexInputRef
                            val cast: RexCall = operands[1] as RexCall
                            val ref1: RexInputRef = cast.operands.get(0) as RexInputRef
                            val literal: RexLiteral = operands[2] as RexLiteral
                            if (ref0.getIndex() === ref1.getIndex()
                                && Objects.equals(literal.getValueAs(BigDecimal::class.java), BigDecimal.ZERO)
                            ) {
                                val aggCallIndex: Int = ref1.getIndex() - aggregate.getGroupCount()
                                if (aggCallIndex >= 0) {
                                    val aggCall: AggregateCall = aggregate.getAggCallList().get(aggCallIndex)
                                    if (aggCall.getAggregation().getKind() === SqlKind.SUM) {
                                        val j = findSum0(cluster.getTypeFactory(), aggCall, aggCallList)
                                        return cluster.getRexBuilder().makeInputRef(call.type, j)
                                    }
                                }
                            }
                        }
                    }
                    else -> {}
                }
                return super.visitCall(call)
            }
        }
        val projects2: List<RexNode> = shuttle.visitList(project.getProjects())
        val bits2: ImmutableBitSet = RelOptUtil.InputFinder.bits(projects2, null)

        // Build the mapping that we will apply to the project expressions.
        val mapping: Mappings.TargetMapping = Mappings.create(
            MappingType.FUNCTION,
            aggregate.getGroupCount() + aggCallList.size(), -1
        )
        var j = 0
        for (i in 0 until mapping.getSourceCount()) {
            if (i < aggregate.getGroupCount()) {
                // Field is a group key. All group keys are retained.
                mapping.set(i, j++)
            } else if (bits2.get(i)) {
                // Field is an aggregate call. It is used.
                mapping.set(i, j++)
            } else {
                // Field is an aggregate call. It is not used. Remove it.
                aggCallList.remove(j - aggregate.getGroupCount())
            }
        }
        val builder: RelBuilder = call.builder()
        builder.push(aggregate.getInput())
        builder.aggregate(
            builder.groupKey(aggregate.getGroupSet(), aggregate.groupSets), aggCallList
        )
        builder.project(
            RexPermuteInputsShuttle.of(mapping).visitList(projects2)
        )
        call.transformTo(builder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectAggregateMergeRule? {
            return ProjectAggregateMergeRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectAggregateMergeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Project::class.java)
                        .oneInput { b1 -> b1.operand(Aggregate::class.java).anyInputs() }
                }
        }
    }

    companion object {
        /** Given a call to SUM, finds a call to SUM0 with identical arguments,
         * or creates one and adds it to the list. Returns the index.  */
        private fun findSum0(
            typeFactory: RelDataTypeFactory, sum: AggregateCall,
            aggCallList: List<AggregateCall>
        ): Int {
            val sum0: AggregateCall = AggregateCall.create(
                SqlStdOperatorTable.SUM0, sum.isDistinct(),
                sum.isApproximate(), sum.ignoreNulls(), sum.getArgList(),
                sum.filterArg, sum.distinctKeys, sum.collation,
                typeFactory.createTypeWithNullability(sum.type, false), null
            )
            val i = aggCallList.indexOf(sum0)
            if (i >= 0) {
                return i
            }
            aggCallList.add(sum0)
            return aggCallList.size() - 1
        }

        /** Returns the number of calls of a given kind in a list of expressions.  */
        private fun kindCount(
            nodes: Iterable<RexNode?>,
            kind: SqlKind
        ): Int {
            val kindCount = AtomicInteger(0)
            object : RexVisitorImpl<Void?>(true) {
                @Override
                fun visitCall(call: RexCall): Void {
                    if (call.getKind() === kind) {
                        kindCount.incrementAndGet()
                    }
                    return super.visitCall(call)
                }
            }.visitEach(nodes)
            return kindCount.get()
        }
    }
}
