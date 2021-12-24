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
 * Planner rule that pushes
 * a [org.apache.calcite.rel.core.Sort]
 * past a [org.apache.calcite.rel.core.Project].
 *
 * @see CoreRules.SORT_PROJECT_TRANSPOSE
 */
@Value.Enclosing
class SortProjectTransposeRule
/** Creates a SortProjectTransposeRule.  */
protected constructor(config: Config?) : RelRule<SortProjectTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        sortClass: Class<out Sort?>?,
        projectClass: Class<out Project?>?
    ) : this(Config.DEFAULT.withOperandFor(sortClass, projectClass)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        sortClass: Class<out Sort?>?,
        projectClass: Class<out Project?>?,
        description: String?
    ) : this(
        Config.DEFAULT.withDescription(description)
            .`as`(Config::class.java)
            .withOperandFor(sortClass, projectClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        sortClass: Class<out Sort?>?,
        projectClass: Class<out Project?>?,
        relBuilderFactory: RelBuilderFactory?, description: String?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .withDescription(description)
            .`as`(Config::class.java)
            .withOperandFor(sortClass, projectClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?, description: String?
    ) : this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(operand: RelOptRuleOperand?) : this(Config.DEFAULT
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        val project: Project = call.rel(1)
        val cluster: RelOptCluster = project.getCluster()
        if (sort.getConvention() !== project.getConvention()) {
            return
        }

        // Determine mapping between project input and output fields. If sort
        // relies on non-trivial expressions, we can't push.
        val map: Mappings.TargetMapping = RelOptUtil.permutationIgnoreCast(
            project.getProjects(), project.getInput().getRowType()
        )
        for (fc in sort.getCollation().getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                return
            }
            val node: RexNode = project.getProjects().get(fc.getFieldIndex())
            if (node.isA(SqlKind.CAST)) {
                // Check whether it is a monotonic preserving cast, otherwise we cannot push
                val cast: RexCall = node as RexCall
                val newFc: RelFieldCollation = Objects.requireNonNull(RexUtil.apply(map, fc))
                val binding: RexCallBinding = RexCallBinding.create(
                    cluster.getTypeFactory(), cast,
                    ImmutableList.of(RelCollations.of(newFc))
                )
                if (cast.getOperator().getMonotonicity(binding) === SqlMonotonicity.NOT_MONOTONIC) {
                    return
                }
            }
        }
        val newCollation: RelCollation = cluster.traitSet().canonize(
            RexUtil.apply(map, sort.getCollation())
        )
        val newSort: Sort = sort.copy(
            sort.getTraitSet().replace(newCollation),
            project.getInput(),
            newCollation,
            sort.offset,
            sort.fetch
        )
        val newProject: RelNode = project.copy(
            sort.getTraitSet(),
            ImmutableList.of(newSort)
        )
        // Not only is newProject equivalent to sort;
        // newSort is equivalent to project's input
        // (but only if the sort is not also applying an offset/limit).
        val equiv: Map<RelNode, RelNode>
        equiv = if (sort.offset == null && sort.fetch == null && cluster.getPlanner().getRelTraitDefs()
                .contains(RelCollationTraitDef.INSTANCE)
        ) {
            ImmutableMap.of(newSort, project.getInput())
        } else {
            ImmutableMap.of()
        }
        call.transformTo(newProject, equiv)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SortProjectTransposeRule? {
            return SortProjectTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            sortClass: Class<out Sort?>?,
            projectClass: Class<out Project?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(sortClass).oneInput { b1 ->
                    b1.operand(projectClass)
                        .predicate { p -> !p.containsOver() }.anyInputs()
                }
            }
                .`as`(Config::class.java)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            sortClass: Class<out Sort?>?,
            projectClass: Class<out Project?>?,
            inputClass: Class<out RelNode?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(sortClass).oneInput { b1 ->
                    b1.operand(projectClass)
                        .predicate { p -> !p.containsOver() }
                        .oneInput { b2 -> b2.operand(inputClass).anyInputs() }
                }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableSortProjectTransposeRule.Config.of()
                .withOperandFor(Sort::class.java, LogicalProject::class.java)
        }
    }
}
