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
 * Planner rule that pushes
 * a [org.apache.calcite.rel.core.Filter]
 * past a [org.apache.calcite.rel.core.Project].
 *
 * @see CoreRules.FILTER_PROJECT_TRANSPOSE
 */
@Value.Enclosing
class FilterProjectTransposeRule
/** Creates a FilterProjectTransposeRule.  */
protected constructor(config: Config?) : RelRule<FilterProjectTransposeRule.Config?>(config), TransformationRule {
    /**
     * Creates a FilterProjectTransposeRule.
     *
     *
     * Equivalent to the rule created by
     * [.FilterProjectTransposeRule]
     * with some default predicates that do not allow a filter to be pushed
     * past the project if there is a correlation condition anywhere in the
     * filter (since in some cases it can prevent a
     * [org.apache.calcite.rel.core.Correlate] from being de-correlated).
     */
    @Deprecated // to be removed before 2.0
    constructor(
        filterClass: Class<out Filter?>?,
        projectClass: Class<out Project?>?,
        copyFilter: Boolean, copyProject: Boolean,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(
                filterClass,
                { f -> !RexUtil.containsCorrelation(f.getCondition()) },
                projectClass
            ) { project -> true }
            .withCopyFilter(copyFilter)
            .withCopyProject(copyProject)) {
    }

    /**
     * Creates a FilterProjectTransposeRule.
     *
     *
     * If `copyFilter` is true, creates the same kind of Filter as
     * matched in the rule, otherwise it creates a Filter using the RelBuilder
     * obtained by the `relBuilderFactory`.
     * Similarly for `copyProject`.
     *
     *
     * Defining predicates for the Filter (using `filterPredicate`)
     * and/or the Project (using `projectPredicate` allows making the rule
     * more restrictive.
     */
    @Deprecated // to be removed before 2.0
    constructor(
        filterClass: Class<F?>?,
        filterPredicate: Predicate<in F?>?,
        projectClass: Class<P?>?,
        projectPredicate: Predicate<in P?>?,
        copyFilter: Boolean, copyProject: Boolean,
        relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b0 ->
            b0.operand(filterClass).predicate(filterPredicate)
                .oneInput { b1 ->
                    b1.operand(projectClass).predicate(projectPredicate)
                        .anyInputs()
                }
        }
        .`as`(Config::class.java)
        .withCopyFilter(copyFilter)
        .withCopyProject(copyProject)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        filterClass: Class<out Filter?>?,
        filterFactory: FilterFactory?,
        projectClass: Class<out Project?>?,
        projectFactory: RelFactories.ProjectFactory?
    ) : this(Config.DEFAULT
        .withRelBuilderFactory(RelBuilder.proto(filterFactory, projectFactory))
        .withOperandSupplier { b0 ->
            b0.operand(filterClass)
                .predicate { filter -> !RexUtil.containsCorrelation(filter.getCondition()) }
                .oneInput { b2 ->
                    b2.operand(projectClass)
                        .predicate { project -> true }
                        .anyInputs()
                }
        }
        .`as`(Config::class.java)
        .withCopyFilter(filterFactory == null)
        .withCopyProject(projectFactory == null)) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        operand: RelOptRuleOperand?,
        copyFilter: Boolean,
        copyProject: Boolean,
        relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)
        .withCopyFilter(copyFilter)
        .withCopyProject(copyProject)) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filter: Filter = call.rel(0)
        val project: Project = call.rel(1)
        if (project.containsOver()) {
            // In general a filter cannot be pushed below a windowing calculation.
            // Applying the filter before the aggregation function changes
            // the results of the windowing invocation.
            //
            // When the filter is on the PARTITION BY expression of the OVER clause
            // it can be pushed down. For now we don't support this.
            return
        }
        // convert the filter to one that references the child of the project
        var newCondition: RexNode = RelOptUtil.pushPastProject(filter.getCondition(), project)
        val relBuilder: RelBuilder = call.builder()
        val newFilterRel: RelNode
        if (config.isCopyFilter()) {
            val input: RelNode = project.getInput()
            val traitSet: RelTraitSet = filter.getTraitSet()
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) {
                    Collections.singletonList(
                        input.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)
                    )
                }
                .replaceIfs(
                    RelDistributionTraitDef.INSTANCE
                ) {
                    Collections.singletonList(
                        input.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE)
                    )
                }
            newCondition = RexUtil.removeNullabilityCast(relBuilder.getTypeFactory(), newCondition)
            newFilterRel = filter.copy(traitSet, input, newCondition)
        } else {
            newFilterRel = relBuilder.push(project.getInput()).filter(newCondition).build()
        }
        val newProject: RelNode = if (config.isCopyProject()) project.copy(
            project.getTraitSet(), newFilterRel,
            project.getProjects(), project.getRowType()
        ) else relBuilder.push(newFilterRel)
            .project(project.getProjects(), project.getRowType().getFieldNames())
            .build()
        call.transformTo(newProject)
    }

    /** Rule configuration.
     *
     *
     * If `copyFilter` is true, creates the same kind of Filter as
     * matched in the rule, otherwise it creates a Filter using the RelBuilder
     * obtained by the `relBuilderFactory`.
     * Similarly for `copyProject`.
     *
     *
     * Defining predicates for the Filter (using `filterPredicate`)
     * and/or the Project (using `projectPredicate` allows making the rule
     * more restrictive.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterProjectTransposeRule? {
            return FilterProjectTransposeRule(this)
        }

        /** Whether to create a [Filter] of the same convention as the
         * matched Filter.  */
        @get:Value.Default
        val isCopyFilter: Boolean
            get() = true

        /** Sets [.isCopyFilter].  */
        fun withCopyFilter(copyFilter: Boolean): Config?

        /** Whether to create a [Project] of the same convention as the
         * matched Project.  */
        @get:Value.Default
        val isCopyProject: Boolean
            get() = true

        /** Sets [.isCopyProject].  */
        fun withCopyProject(copyProject: Boolean): Config?

        /** Defines an operand tree for the given 2 classes.  */
        fun withOperandFor(
            filterClass: Class<out Filter?>?,
            filterPredicate: Predicate<Filter?>?,
            projectClass: Class<out Project?>?,
            projectPredicate: Predicate<Project?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass).predicate(filterPredicate)
                    .oneInput { b1 -> b1.operand(projectClass).predicate(projectPredicate).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        /** Defines an operand tree for the given 3 classes.  */
        fun withOperandFor(
            filterClass: Class<out Filter?>?,
            projectClass: Class<out Project?>?,
            relClass: Class<out RelNode?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass)
                    .oneInput { b1 -> b1.operand(projectClass).oneInput { b2 -> b2.operand(relClass).anyInputs() } }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterProjectTransposeRule.Config.of()
                .withOperandFor(
                    Filter::class.java,
                    { f -> !RexUtil.containsCorrelation(f.getCondition()) },
                    Project::class.java
                ) { p -> true }
                .withCopyFilter(true)
                .withCopyProject(true)
        }
    }
}
