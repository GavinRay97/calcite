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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.RelOptCluster

/**
 * `Calc` is an abstract base class for implementations of
 * [org.apache.calcite.rel.logical.LogicalCalc].
 */
abstract class Calc @SuppressWarnings("method.invocation.invalid") protected constructor(
    cluster: RelOptCluster?,
    traits: RelTraitSet?,
    hints: List<RelHint?>?,
    child: RelNode?,
    program: RexProgram
) : SingleRel(cluster, traits, child), Hintable {
    //~ Instance fields --------------------------------------------------------
    protected val hints: ImmutableList<RelHint>
    protected val program: RexProgram
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Calc.
     *
     * @param cluster Cluster
     * @param traits Traits
     * @param hints Hints of this relational expression
     * @param child Input relation
     * @param program Calc program
     */
    init {
        this.rowType = program.getOutputRowType()
        this.program = program
        this.hints = ImmutableList.copyOf(hints)
        assert(isValid(Litmus.THROW, null))
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?,
        traits: RelTraitSet?,
        child: RelNode?,
        program: RexProgram?
    ) : this(cluster, traits, ImmutableList.of(), child, program) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?,
        traits: RelTraitSet?,
        child: RelNode?,
        program: RexProgram?,
        collationList: List<RelCollation?>?
    ) : this(cluster, traits, ImmutableList.of(), child, program) {
        Util.discard(collationList)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): Calc {
        return copy(traitSet, sole(inputs), program)
    }

    /**
     * Creates a copy of this `Calc`.
     *
     * @param traitSet Traits
     * @param child Input relation
     * @param program Calc program
     * @return New `Calc` if any parameter differs from the value of this
     * `Calc`, or just `this` if all the parameters are the same
     *
     * @see .copy
     */
    abstract fun copy(
        traitSet: RelTraitSet?,
        child: RelNode?,
        program: RexProgram?
    ): Calc

    @Deprecated // to be removed before 2.0
    fun copy(
        traitSet: RelTraitSet?,
        child: RelNode?,
        program: RexProgram?,
        collationList: List<RelCollation?>?
    ): Calc {
        Util.discard(collationList)
        return copy(traitSet, child, program)
    }

    /** Returns whether this Calc contains any windowed-aggregate functions.  */
    fun containsOver(): Boolean {
        return RexOver.containsOver(program)
    }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        if (!RelOptUtil.equal(
                "program's input type",
                program.getInputRowType(),
                "child's output type",
                getInput().getRowType(), litmus
            )
        ) {
            return litmus.fail(null)
        }
        if (!program.isValid(litmus, context)) {
            return litmus.fail(null)
        }
        return if (!program.isNormalized(litmus, getCluster().getRexBuilder())) {
            litmus.fail(null)
        } else litmus.succeed()
    }

    fun getProgram(): RexProgram {
        return program
    }

    @Override
    fun getHints(): ImmutableList<RelHint> {
        return hints
    }

    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        return RelMdUtil.estimateFilteredRows(getInput(), program, mq)
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        val dRows: Double = mq.getRowCount(this)
        val dCpu: Double = (mq.getRowCount(getInput())
                * program.getExprCount())
        val dIo = 0.0
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo)
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return program.explainCalc(super.explainTerms(pw))
    }

    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val oldExprs: List<RexNode> = program.getExprList()
        val exprs: List<RexNode> = shuttle.apply(oldExprs)
        val oldProjects: List<RexLocalRef> = program.getProjectList()
        val projects: List<RexLocalRef> = shuttle.apply(oldProjects)
        val oldCondition: RexLocalRef = program.getCondition()
        val condition: RexNode?
        if (oldCondition != null) {
            condition = shuttle.apply(oldCondition)
            assert(condition is RexLocalRef) {
                ("Invalid condition after rewrite. Expected RexLocalRef, got "
                        + condition)
            }
        } else {
            condition = null
        }
        if (exprs === oldExprs && projects === oldProjects && condition === oldCondition) {
            return this
        }
        val rexBuilder: RexBuilder = getCluster().getRexBuilder()
        val rowType: RelDataType = RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projects,
            getRowType().getFieldNames(),
            null
        )
        val newProgram: RexProgram = RexProgramBuilder.create(
            rexBuilder, program.getInputRowType(), exprs, projects,
            condition, rowType, true, null
        )
            .getProgram(false)
        return copy(traitSet, getInput(), newProgram)
    }
}
