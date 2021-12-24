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
 * MultiJoinProjectTransposeRule implements the rule for pulling
 * [org.apache.calcite.rel.logical.LogicalProject]s that are on top of a
 * [MultiJoin] and beneath a
 * [org.apache.calcite.rel.logical.LogicalJoin] so the
 * [org.apache.calcite.rel.logical.LogicalProject] appears above the
 * [org.apache.calcite.rel.logical.LogicalJoin].
 *
 *
 * In the process of doing
 * so, also save away information about the respective fields that are
 * referenced in the expressions in the
 * [org.apache.calcite.rel.logical.LogicalProject] we're pulling up, as
 * well as the join condition, in the resultant [MultiJoin]s
 *
 *
 * For example, if we have the following sub-query:
 *
 * <blockquote><pre>
 * (select X.x1, Y.y1 from X, Y
 * where X.x2 = Y.y2 and X.x3 = 1 and Y.y3 = 2)</pre></blockquote>
 *
 *
 * The [MultiJoin] associated with (X, Y) associates x1 with X and
 * y1 with Y. Although x3 and y3 need to be read due to the filters, they are
 * not required after the row scan has completed and therefore are not saved.
 * The join fields, x2 and y2, are also tracked separately.
 *
 *
 * Note that by only pulling up projects that are on top of
 * [MultiJoin]s, we preserve projections on top of row scans.
 *
 *
 * See the superclass for details on restrictions regarding which
 * [org.apache.calcite.rel.logical.LogicalProject]s cannot be pulled.
 *
 * @see CoreRules.MULTI_JOIN_BOTH_PROJECT
 *
 * @see CoreRules.MULTI_JOIN_LEFT_PROJECT
 *
 * @see CoreRules.MULTI_JOIN_RIGHT_PROJECT
 */
@Value.Enclosing
class MultiJoinProjectTransposeRule
/** Creates a MultiJoinProjectTransposeRule.  */
protected constructor(config: Config?) : JoinProjectTransposeRule(config) {
    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        description: String?
    ) : this(ImmutableMultiJoinProjectTransposeRule.Config.of().withDescription(description)
        .withOperandSupplier { b -> b.exactly(operand) }) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?,
        description: String?
    ) : this(ImmutableMultiJoinProjectTransposeRule.Config.of().withDescription(description)
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.exactly(operand) }) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    protected fun hasLeftChild(call: RelOptRuleCall): Boolean {
        return call.rels.length !== 4
    }

    @Override
    protected fun hasRightChild(call: RelOptRuleCall): Boolean {
        return call.rels.length > 3
    }

    @Override
    protected fun getRightChild(call: RelOptRuleCall): Project {
        return if (call.rels.length === 4) {
            call.rel(2)
        } else {
            call.rel(3)
        }
    }

    @Override
    protected fun getProjectChild(
        call: RelOptRuleCall,
        project: Project?,
        leftChild: Boolean
    ): RelNode {
        // locate the appropriate MultiJoin based on which rule was fired
        // and which projection we're dealing with
        val multiJoin: MultiJoin
        multiJoin = if (leftChild) {
            call.rel(2)
        } else if (call.rels.length === 4) {
            call.rel(3)
        } else {
            call.rel(4)
        }

        // create a new MultiJoin that reflects the columns in the projection
        // above the MultiJoin
        return RelOptUtil.projectMultiJoin(multiJoin, project)
    }

    /** Rule configuration.  */
    @Value.Immutable
    @SuppressWarnings("immutables:subtype")
    interface Config : JoinProjectTransposeRule.Config {
        @Override
        fun toRule(): MultiJoinProjectTransposeRule? {
            return MultiJoinProjectTransposeRule(this)
        }

        companion object {
            val BOTH_PROJECT: Config = ImmutableMultiJoinProjectTransposeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalJoin::class.java).inputs(
                        { b1 ->
                            b1.operand(LogicalProject::class.java)
                                .oneInput { b2 -> b2.operand(MultiJoin::class.java).anyInputs() }
                        }
                    ) { b3 ->
                        b3.operand(LogicalProject::class.java)
                            .oneInput { b4 -> b4.operand(MultiJoin::class.java).anyInputs() }
                    }
                }
                .withDescription(
                    "MultiJoinProjectTransposeRule: with two LogicalProject children"
                )
            val LEFT_PROJECT: Config = ImmutableMultiJoinProjectTransposeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalJoin::class.java).inputs { b1 ->
                        b1.operand(LogicalProject::class.java).oneInput { b2 ->
                            b2.operand(
                                MultiJoin::class.java
                            ).anyInputs()
                        }
                    }
                }
                .withDescription(
                    "MultiJoinProjectTransposeRule: with LogicalProject on left"
                )
            val RIGHT_PROJECT: Config = ImmutableMultiJoinProjectTransposeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalJoin::class.java).inputs(
                        { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                    ) { b2 ->
                        b2.operand(LogicalProject::class.java)
                            .oneInput { b3 -> b3.operand(MultiJoin::class.java).anyInputs() }
                    }
                }
                .withDescription(
                    "MultiJoinProjectTransposeRule: with LogicalProject on right"
                )
        }
    }
}
