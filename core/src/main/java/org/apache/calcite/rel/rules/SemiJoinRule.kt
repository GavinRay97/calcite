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

/**
 * Planner rule that creates a `SemiJoin` from a
 * [org.apache.calcite.rel.core.Join] on top of a
 * [org.apache.calcite.rel.logical.LogicalAggregate].
 */
abstract class SemiJoinRule
/** Creates a SemiJoinRule.  */
protected constructor(config: Config?) : RelRule<SemiJoinRule.Config?>(config), TransformationRule {
    protected fun perform(
        call: RelOptRuleCall, @Nullable project: Project?,
        join: Join, left: RelNode, aggregate: Aggregate
    ) {
        val cluster: RelOptCluster = join.getCluster()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        if (project != null) {
            val bits: ImmutableBitSet = RelOptUtil.InputFinder.bits(project.getProjects(), null)
            val rightBits: ImmutableBitSet = ImmutableBitSet.range(
                left.getRowType().getFieldCount(),
                join.getRowType().getFieldCount()
            )
            if (bits.intersects(rightBits)) {
                return
            }
        } else {
            if (join.getJoinType().projectsRight()
                && !isEmptyAggregate(aggregate)
            ) {
                return
            }
        }
        val joinInfo: JoinInfo = join.analyzeCondition()
        if (!joinInfo.rightSet().equals(
                ImmutableBitSet.range(aggregate.getGroupCount())
            )
        ) {
            // Rule requires that aggregate key to be the same as the join key.
            // By the way, neither a super-set nor a sub-set would work.
            return
        }
        if (!joinInfo.isEqui()) {
            return
        }
        val relBuilder: RelBuilder = call.builder()
        relBuilder.push(left)
        when (join.getJoinType()) {
            SEMI, INNER -> {
                val newRightKeyBuilder: List<Integer> = ArrayList()
                val aggregateKeys: List<Integer> = aggregate.getGroupSet().asList()
                for (key in joinInfo.rightKeys) {
                    newRightKeyBuilder.add(aggregateKeys[key])
                }
                val newRightKeys: ImmutableIntList = ImmutableIntList.copyOf(newRightKeyBuilder)
                relBuilder.push(aggregate.getInput())
                val newCondition: RexNode = RelOptUtil.createEquiJoinCondition(
                    relBuilder.peek(2, 0),
                    joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
                    rexBuilder
                )
                relBuilder.semiJoin(newCondition).hints(join.getHints())
            }
            LEFT -> {}
            else -> throw AssertionError(join.getJoinType())
        }
        if (project != null) {
            relBuilder.project(project.getProjects(), project.getRowType().getFieldNames())
        }
        val relNode: RelNode = relBuilder.build()
        call.transformTo(relNode)
    }

    /** SemiJoinRule that matches a Project on top of a Join with an Aggregate
     * as its right child.
     *
     * @see CoreRules.PROJECT_TO_SEMI_JOIN
     */
    class ProjectToSemiJoinRule
    /** Creates a ProjectToSemiJoinRule.  */
    protected constructor(config: ProjectToSemiJoinRuleConfig?) : SemiJoinRule(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            projectClass: Class<Project?>?,
            joinClass: Class<Join?>?, aggregateClass: Class<Aggregate?>?,
            relBuilderFactory: RelBuilderFactory?, description: String?
        ) : this(
            ProjectToSemiJoinRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .withDescription(description)
                .`as`(ProjectToSemiJoinRuleConfig::class.java)
                .withOperandFor(projectClass, joinClass, aggregateClass)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val project: Project = call.rel(0)
            val join: Join = call.rel(1)
            val left: RelNode = call.rel(2)
            val aggregate: Aggregate = call.rel(3)
            perform(call, project, join, left, aggregate)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface ProjectToSemiJoinRuleConfig : Config {
            @Override
            override fun toRule(): ProjectToSemiJoinRule {
                return ProjectToSemiJoinRule(this)
            }

            /** Defines an operand tree for the given classes.  */
            fun withOperandFor(
                projectClass: Class<out Project?>?,
                joinClass: Class<out Join?>?,
                aggregateClass: Class<out Aggregate?>?
            ): ProjectToSemiJoinRuleConfig? {
                return withOperandSupplier { b ->
                    b.operand(projectClass).oneInput { b2 ->
                        b2.operand(joinClass)
                            .predicate { join: Join -> isJoinTypeSupported(join) }
                            .inputs(
                                { b3 -> b3.operand(RelNode::class.java).anyInputs() }
                            ) { b4 -> b4.operand(aggregateClass).anyInputs() }
                    }
                }
                    .`as`(ProjectToSemiJoinRuleConfig::class.java)
            }

            companion object {
                val DEFAULT: ProjectToSemiJoinRuleConfig = ImmutableProjectToSemiJoinRuleConfig.of()
                    .withDescription("SemiJoinRule:project")
                    .withOperandFor(Project::class.java, Join::class.java, Aggregate::class.java)
            }
        }
    }

    /** SemiJoinRule that matches a Join with an empty Aggregate as its right
     * input.
     *
     * @see CoreRules.JOIN_TO_SEMI_JOIN
     */
    class JoinToSemiJoinRule
    /** Creates a JoinToSemiJoinRule.  */
    protected constructor(config: JoinToSemiJoinRuleConfig?) : SemiJoinRule(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            joinClass: Class<Join?>?, aggregateClass: Class<Aggregate?>?,
            relBuilderFactory: RelBuilderFactory?, description: String?
        ) : this(
            JoinToSemiJoinRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .withDescription(description)
                .`as`(JoinToSemiJoinRuleConfig::class.java)
                .withOperandFor(joinClass, aggregateClass)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val join: Join = call.rel(0)
            val left: RelNode = call.rel(1)
            val aggregate: Aggregate = call.rel(2)
            perform(call, null, join, left, aggregate)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface JoinToSemiJoinRuleConfig : Config {
            @Override
            override fun toRule(): JoinToSemiJoinRule {
                return JoinToSemiJoinRule(this)
            }

            /** Defines an operand tree for the given classes.  */
            fun withOperandFor(
                joinClass: Class<Join?>?,
                aggregateClass: Class<Aggregate?>?
            ): JoinToSemiJoinRuleConfig? {
                return withOperandSupplier { b ->
                    b.operand(joinClass).predicate { join: Join -> isJoinTypeSupported(join) }
                        .inputs(
                            { b2 -> b2.operand(RelNode::class.java).anyInputs() }
                        ) { b3 -> b3.operand(aggregateClass).anyInputs() }
                }
                    .`as`(JoinToSemiJoinRuleConfig::class.java)
            }

            companion object {
                val DEFAULT: JoinToSemiJoinRuleConfig = ImmutableJoinToSemiJoinRuleConfig.of()
                    .withDescription("SemiJoinRule:join")
                    .withOperandFor(Join::class.java, Aggregate::class.java)
            }
        }
    }

    /**
     * Rule configuration.
     */
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SemiJoinRule?
    }

    companion object {
        private fun isJoinTypeSupported(join: Join): Boolean {
            val type: JoinRelType = join.getJoinType()
            return type === JoinRelType.INNER || type === JoinRelType.LEFT
        }

        /**
         * Tests if an Aggregate always produces 1 row and 0 columns.
         */
        private fun isEmptyAggregate(aggregate: Aggregate): Boolean {
            return aggregate.getRowType().getFieldCount() === 0
        }
    }
}
