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
 * Planner rule that matches a
 * [org.apache.calcite.rel.core.Join] one of whose inputs is a
 * [org.apache.calcite.rel.logical.LogicalProject], and
 * pulls the project up.
 *
 *
 * Projections are pulled up if the
 * [org.apache.calcite.rel.logical.LogicalProject] doesn't originate from
 * a null generating input in an outer join.
 */
@Value.Enclosing
class JoinProjectTransposeRule
/** Creates a JoinProjectTransposeRule.  */
protected constructor(config: Config?) : RelRule<JoinProjectTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        description: String?, includeOuter: Boolean,
        relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT.withDescription(description)
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)
        .withIncludeOuter(includeOuter)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        description: String?
    ) : this(Config.DEFAULT.withDescription(description)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        description: String?, projectFactory: ProjectFactory?
    ) : this(Config.DEFAULT.withDescription(description)
        .withRelBuilderFactory(RelBuilder.proto(Contexts.of(projectFactory)))
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        description: String?, includeOuter: Boolean,
        projectFactory: ProjectFactory?
    ) : this(Config.DEFAULT.withDescription(description)
        .withRelBuilderFactory(RelBuilder.proto(Contexts.of(projectFactory)))
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)
        .withIncludeOuter(includeOuter)) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)
        val joinType: JoinRelType = join.getJoinType()
        var leftProject: Project?
        var rightProject: Project?
        var leftJoinChild: RelNode
        var rightJoinChild: RelNode

        // If 1) the rule works on outer joins, or
        //    2) input's projection doesn't generate nulls
        val includeOuter: Boolean = config.isIncludeOuter()
        if (hasLeftChild(call)
            && (includeOuter || !joinType.generatesNullsOnLeft())
        ) {
            leftProject = call.rel(1)
            leftJoinChild = getProjectChild(call, leftProject, true)
        } else {
            leftProject = null
            leftJoinChild = call.rel(1)
        }
        if (hasRightChild(call)
            && (includeOuter || !joinType.generatesNullsOnRight())
        ) {
            rightProject = getRightChild(call)
            rightJoinChild = getProjectChild(call, rightProject, false)
        } else {
            rightProject = null
            rightJoinChild = join.getRight()
        }

        // Skip projects containing over clause
        if (leftProject != null && leftProject.containsOver()) {
            leftProject = null
            leftJoinChild = join.getLeft()
        }
        if (rightProject != null && rightProject.containsOver()) {
            rightProject = null
            rightJoinChild = join.getRight()
        }
        if (leftProject == null && rightProject == null) {
            return
        }
        if (includeOuter) {
            if (leftProject != null && joinType.generatesNullsOnLeft()
                && !Strong.allStrong(leftProject.getProjects())
            ) {
                return
            }
            if (rightProject != null && joinType.generatesNullsOnRight()
                && !Strong.allStrong(rightProject.getProjects())
            ) {
                return
            }
        }

        // Construct two RexPrograms and combine them.  The bottom program
        // is a join of the projection expressions from the left and/or
        // right projects that feed into the join.  The top program contains
        // the join condition.

        // Create a row type representing a concatenation of the inputs
        // underneath the projects that feed into the join.  This is the input
        // into the bottom RexProgram.  Note that the join type is an inner
        // join because the inputs haven't actually been joined yet.
        val joinChildrenRowType: RelDataType = SqlValidatorUtil.deriveJoinRowType(
            leftJoinChild.getRowType(),
            rightJoinChild.getRowType(),
            JoinRelType.INNER,
            join.getCluster().getTypeFactory(),
            null,
            Collections.emptyList()
        )

        // Create projection expressions, combining the projection expressions
        // from the projects that feed into the join.  For the RHS projection
        // expressions, shift them to the right by the number of fields on
        // the LHS.  If the join input was not a projection, simply create
        // references to the inputs.
        val nProjExprs: Int = join.getRowType().getFieldCount()
        val projects: List<Pair<RexNode?, String?>> = ArrayList()
        val rexBuilder: RexBuilder = join.getCluster().getRexBuilder()
        createProjectExprs(
            leftProject,
            leftJoinChild,
            0,
            rexBuilder,
            joinChildrenRowType.getFieldList(),
            projects
        )
        val leftFields: List<RelDataTypeField> = leftJoinChild.getRowType().getFieldList()
        val nFieldsLeft: Int = leftFields.size()
        createProjectExprs(
            rightProject,
            rightJoinChild,
            nFieldsLeft,
            rexBuilder,
            joinChildrenRowType.getFieldList(),
            projects
        )
        val projTypes: List<RelDataType> = ArrayList()
        for (i in 0 until nProjExprs) {
            projTypes.add(projects[i].left.getType())
        }
        val projRowType: RelDataType = rexBuilder.getTypeFactory().createStructType(
            projTypes,
            Pair.right(projects)
        )

        // create the RexPrograms and merge them
        val bottomProgram: RexProgram = RexProgram.create(
            joinChildrenRowType,
            Pair.left(projects),
            null,
            projRowType,
            rexBuilder
        )
        val topProgramBuilder = RexProgramBuilder(
            projRowType,
            rexBuilder
        )
        topProgramBuilder.addIdentity()
        topProgramBuilder.addCondition(join.getCondition())
        val topProgram: RexProgram = topProgramBuilder.getProgram()
        val mergedProgram: RexProgram = RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder
        )

        // expand out the join condition and construct a new LogicalJoin that
        // directly references the join children without the intervening
        // ProjectRels
        val newCondition: RexNode = mergedProgram.expandLocalRef(
            requireNonNull(
                mergedProgram.getCondition()
            ) { "mergedProgram.getCondition() for $mergedProgram" })
        val newJoin: Join = join.copy(
            join.getTraitSet(), newCondition,
            leftJoinChild, rightJoinChild, join.getJoinType(),
            join.isSemiJoinDone()
        )

        // expand out the new projection expressions; if the join is an
        // outer join, modify the expressions to reference the join output
        val newProjExprs: List<RexNode> = ArrayList()
        val projList: List<RexLocalRef> = mergedProgram.getProjectList()
        val newJoinFields: List<RelDataTypeField> = newJoin.getRowType().getFieldList()
        val nJoinFields: Int = newJoinFields.size()
        val adjustments = IntArray(nJoinFields)
        for (i in 0 until nProjExprs) {
            var newExpr: RexNode = mergedProgram.expandLocalRef(projList[i])
            if (joinType.isOuterJoin()) {
                newExpr = newExpr.accept(
                    RexInputConverter(
                        rexBuilder,
                        joinChildrenRowType.getFieldList(),
                        newJoinFields,
                        adjustments
                    )
                )
            }
            newProjExprs.add(newExpr)
        }

        // finally, create the projection on top of the join
        val relBuilder: RelBuilder = call.builder()
        relBuilder.push(newJoin)
        relBuilder.project(newProjExprs, join.getRowType().getFieldNames())
        // if the join was outer, we might need a cast after the
        // projection to fix differences wrt nullability of fields
        if (joinType.isOuterJoin()) {
            relBuilder.convert(join.getRowType(), false)
        }
        call.transformTo(relBuilder.build())
    }

    /** Returns whether the rule was invoked with a left project child.  */
    protected fun hasLeftChild(call: RelOptRuleCall): Boolean {
        return call.rel(1) is Project
    }

    /** Returns whether the rule was invoked with 2 children.  */
    protected fun hasRightChild(call: RelOptRuleCall): Boolean {
        return call.rels.length === 3
    }

    /** Returns the Project corresponding to the right child.  */
    protected fun getRightChild(call: RelOptRuleCall): Project {
        return call.rel(2)
    }

    /**
     * Returns the child of the project that will be used as input into the new
     * LogicalJoin once the projects are pulled above the LogicalJoin.
     *
     * @param call      RelOptRuleCall
     * @param project   project RelNode
     * @param leftChild true if the project corresponds to the left projection
     * @return child of the project that will be used as input into the new
     * LogicalJoin once the projects are pulled above the LogicalJoin
     */
    protected fun getProjectChild(
        call: RelOptRuleCall?,
        project: Project?,
        leftChild: Boolean
    ): RelNode {
        return project.getInput()
    }

    /**
     * Creates projection expressions corresponding to one of the inputs into
     * the join.
     *
     * @param project            the projection input into the join (if it exists)
     * @param joinChild          the child of the projection input (if there is a
     * projection); otherwise, this is the join input
     * @param adjustmentAmount   the amount the expressions need to be shifted by
     * @param rexBuilder         rex builder
     * @param joinChildrenFields concatenation of the fields from the left and
     * right join inputs (once the projections have been
     * removed)
     * @param projects           Projection expressions &amp; names to be created
     */
    protected fun createProjectExprs(
        @Nullable project: Project?,
        joinChild: RelNode,
        adjustmentAmount: Int,
        rexBuilder: RexBuilder,
        joinChildrenFields: List<RelDataTypeField?>?,
        projects: List<Pair<RexNode?, String?>?>
    ) {
        val childFields: List<RelDataTypeField> = joinChild.getRowType().getFieldList()
        if (project != null) {
            val namedProjects: List<Pair<RexNode, String>> = project.getNamedProjects()
            val nChildFields: Int = childFields.size()
            val adjustments = IntArray(nChildFields)
            for (i in 0 until nChildFields) {
                adjustments[i] = adjustmentAmount
            }
            for (pair in namedProjects) {
                var e: RexNode = pair.left
                if (adjustmentAmount != 0) {
                    // shift the references by the adjustment amount
                    e = e.accept(
                        RexInputConverter(
                            rexBuilder,
                            childFields,
                            joinChildrenFields,
                            adjustments
                        )
                    )
                }
                projects.add(Pair.of(e, pair.right))
            }
        } else {
            // no projection; just create references to the inputs
            for (i in 0 until childFields.size()) {
                val field: RelDataTypeField = childFields[i]
                projects.add(
                    Pair.of(
                        rexBuilder.makeInputRef(
                            field.getType(),
                            i + adjustmentAmount
                        ),
                        field.getName()
                    )
                )
            }
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinProjectTransposeRule? {
            return JoinProjectTransposeRule(this)
        }

        /** Whether to include outer joins, default false.  */
        @get:Value.Default
        val isIncludeOuter: Boolean
            get() = false

        /** Sets [.isIncludeOuter].  */
        fun withIncludeOuter(includeOuter: Boolean): Config?

        companion object {
            val DEFAULT: Config = ImmutableJoinProjectTransposeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalJoin::class.java).inputs(
                        { b1 -> b1.operand(LogicalProject::class.java).anyInputs() }
                    ) { b2 -> b2.operand(LogicalProject::class.java).anyInputs() }
                }
                .withDescription("JoinProjectTransposeRule(Project-Project)")
            val LEFT: Config = DEFAULT
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalJoin::class.java)
                        .inputs { b1 -> b1.operand(LogicalProject::class.java).anyInputs() }
                }
                .withDescription("JoinProjectTransposeRule(Project-Other)")
                .`as`(Config::class.java)
            val RIGHT: Config = DEFAULT
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalJoin::class.java).inputs(
                        { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                    ) { b2 -> b2.operand(LogicalProject::class.java).anyInputs() }
                }
                .withDescription("JoinProjectTransposeRule(Other-Project)")
                .`as`(Config::class.java)
            val OUTER: Config = DEFAULT
                .withDescription(
                    "Join(IncludingOuter)ProjectTransposeRule(Project-Project)"
                )
                .`as`(Config::class.java)
                .withIncludeOuter(true)
            val LEFT_OUTER: Config = LEFT
                .withDescription(
                    "Join(IncludingOuter)ProjectTransposeRule(Project-Other)"
                )
                .`as`(Config::class.java)
                .withIncludeOuter(true)
            val RIGHT_OUTER: Config = RIGHT
                .withDescription(
                    "Join(IncludingOuter)ProjectTransposeRule(Other-Project)"
                )
                .`as`(Config::class.java)
                .withIncludeOuter(true)
        }
    }
}
