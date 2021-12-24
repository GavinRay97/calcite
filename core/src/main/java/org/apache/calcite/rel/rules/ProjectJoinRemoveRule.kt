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
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List
import java.util.stream.Collectors

/**
 * Planner rule that matches an [Project]
 * on a [Join] and removes the join provided that the join is a left join
 * or right join and the join keys are unique.
 *
 *
 * For instance,
 *
 * <blockquote>
 * <pre>select s.product_id
 * from sales as s
 * left join product as p
 * on s.product_id = p.product_id</pre></blockquote>
 *
 *
 * becomes
 *
 * <blockquote>
 * <pre>select s.product_id from sales as s</pre></blockquote>
 */
@Value.Enclosing
class ProjectJoinRemoveRule
/** Creates a ProjectJoinRemoveRule.  */
protected constructor(config: Config?) : RelRule<ProjectJoinRemoveRule.Config?>(config), SubstitutionRule {
    @Deprecated // to be removed before 2.0
    constructor(
        projectClass: Class<out Project?>?,
        joinClass: Class<out Join?>?, relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(projectClass, joinClass)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: Project = call.rel(0)
        val join: Join = call.rel(1)
        val isLeftJoin = join.getJoinType() === JoinRelType.LEFT
        val lower = if (isLeftJoin) join.getLeft().getRowType().getFieldCount() else 0
        val upper: Int =
            if (isLeftJoin) join.getRowType().getFieldCount() else join.getLeft().getRowType().getFieldCount()

        // Check whether the project uses columns whose index is between
        // lower(included) and upper(excluded).
        for (expr in project.getProjects()) {
            if (RelOptUtil.InputFinder.bits(expr).asList().stream().anyMatch { i -> i >= lower && i < upper }) {
                return
            }
        }
        val leftKeys: List<Integer> = ArrayList()
        val rightKeys: List<Integer> = ArrayList()
        RelOptUtil.splitJoinCondition(
            join.getLeft(), join.getRight(),
            join.getCondition(), leftKeys, rightKeys,
            ArrayList()
        )
        val joinKeys: List<Integer> = if (isLeftJoin) rightKeys else leftKeys
        val columns: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        joinKeys.forEach(columns::set)
        val mq: RelMetadataQuery = call.getMetadataQuery()
        if (!Boolean.TRUE.equals(
                mq.areColumnsUnique(
                    if (isLeftJoin) join.getRight() else join.getLeft(),
                    columns.build()
                )
            )
        ) {
            return
        }
        val node: RelNode
        node = if (isLeftJoin) {
            project
                .copy(
                    project.getTraitSet(), join.getLeft(), project.getProjects(),
                    project.getRowType()
                )
        } else {
            val offset: Int = join.getLeft().getRowType().getFieldCount()
            val newExprs: List<RexNode> = project.getProjects().stream()
                .map { expr -> RexUtil.shift(expr, -offset) }
                .collect(Collectors.toList())
            project.copy(
                project.getTraitSet(), join.getRight(), newExprs,
                project.getRowType()
            )
        }
        call.transformTo(node)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectJoinRemoveRule? {
            return ProjectJoinRemoveRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            joinClass: Class<out Join?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 ->
                    b1.operand(joinClass).predicate { join ->
                        (join.getJoinType() === JoinRelType.LEFT
                                || join.getJoinType() === JoinRelType.RIGHT)
                    }.anyInputs()
                }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectJoinRemoveRule.Config.of()
                .withOperandFor(LogicalProject::class.java, LogicalJoin::class.java)
        }
    }
}
