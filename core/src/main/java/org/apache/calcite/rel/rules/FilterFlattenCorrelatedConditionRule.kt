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
 * Planner rule that matches a [Filter] expression with correlated variables, and rewrites the
 * condition in a simpler form that is more convenient for the decorrelation logic.
 *
 *
 * Uncorrelated calls below a comparison operator are turned into input references by extracting
 * the computation in a [org.apache.calcite.rel.core.Project] expression. An additional
 * projection may be added on top of the new filter to retain expression equivalence.
 *
 *
 * Sub-plan before
 * <pre>
 * LogicalProject($f0=[true])
 * LogicalFilter(condition=[=($cor0.DEPTNO, +($7, 30))])
 * LogicalTableScan(table=[[CATALOG, SALES, EMP]])
</pre> *
 *
 *
 * Sub-plan after
 * <pre>
 * LogicalProject($f0=[true])
 * LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2],..., COMM=[$6], DEPTNO=[$7], SLACKER=[$8])
 * LogicalFilter(condition=[=($cor0.DEPTNO, $9)])
 * LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2],..., SLACKER=[$8], $f9=[+($7, 30)])
 * LogicalTableScan(table=[[CATALOG, SALES, EMP]])
</pre> *
 *
 *
 * The rule should be used in conjunction with other rules and transformations to have a positive
 * impact on the plan. At the moment it is tightly connected with the decorrelation logic and may
 * not be useful in a broader context. Projects may implement decorrelation differently so they may
 * choose to use this rule or not.
 */
@API(since = "1.27", status = API.Status.EXPERIMENTAL)
@Value.Enclosing
class FilterFlattenCorrelatedConditionRule(config: Config?) :
    RelRule<FilterFlattenCorrelatedConditionRule.Config?>(config) {
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val filter: Filter = call.rel(0)
        return RexUtil.containsCorrelation(filter.getCondition())
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filter: Filter = call.rel(0)
        val b: RelBuilder = call.builder()
        b.push(filter.getInput())
        val proj: Int = b.fields().size()
        val projOperands: List<RexNode> = ArrayList()
        // Visitor logic strongly dependent on RelDecorrelator#findCorrelationEquivalent
        // Handling more kinds of expressions may be useless if the respective logic cannot exploit them
        val newCondition: RexNode = filter.getCondition().accept(object : RexShuttle() {
            @Override
            fun visitCall(call: RexCall): RexNode {
                return when (call.getKind()) {
                    EQUALS, NOT_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM -> {
                        val op0: RexNode = call.operands.get(0)
                        val op1: RexNode = call.operands.get(1)
                        val replaceIndex: Int
                        replaceIndex = if (RexUtil.containsCorrelation(op1) && isUncorrelatedCall(op0)) {
                            0
                        } else if (RexUtil.containsCorrelation(op0) && isUncorrelatedCall(op1)) {
                            1
                        } else {
                            // Structure does not match, do not replace
                            -1
                        }
                        if (replaceIndex != -1) {
                            val copyOperands: List<RexNode> = ArrayList(call.operands)
                            val oldOp: RexNode = call.operands.get(replaceIndex)
                            val newOp: RexNode = b.getRexBuilder()
                                .makeInputRef(oldOp.getType(), proj + projOperands.size())
                            projOperands.add(oldOp)
                            copyOperands.set(replaceIndex, newOp)
                            return call.clone(call.type, copyOperands)
                        }
                        call
                    }
                    AND, OR -> super.visitCall(call)
                    else -> call
                }
            }
        })
        if (newCondition.equals(filter.getCondition())) {
            return
        }
        b.projectPlus(projOperands)
        b.filter(newCondition)
        b.project(b.fields(ImmutableBitSet.range(proj).asList()))
        call.transformTo(b.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterFlattenCorrelatedConditionRule? {
            return FilterFlattenCorrelatedConditionRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterFlattenCorrelatedConditionRule.Config.of()
                .withOperandSupplier { op -> op.operand(Filter::class.java).anyInputs() }
        }
    }

    companion object {
        private fun isUncorrelatedCall(node: RexNode): Boolean {
            return node is RexCall && !RexUtil.containsCorrelation(node)
        }
    }
}
