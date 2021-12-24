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
 * Relational expression that iterates over its input
 * and returns elements for which `condition` evaluates to
 * `true`.
 *
 *
 * If the condition allows nulls, then a null value is treated the same as
 * false.
 *
 * @see org.apache.calcite.rel.logical.LogicalFilter
 */
abstract class Filter @SuppressWarnings("method.invocation.invalid") protected constructor(
    cluster: RelOptCluster?,
    traits: RelTraitSet?,
    child: RelNode?,
    condition: RexNode
) : SingleRel(cluster, traits, child) {
    //~ Instance fields --------------------------------------------------------
    protected val condition: RexNode
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a filter.
     *
     * @param cluster   Cluster that this relational expression belongs to
     * @param traits    the traits of this rel
     * @param child     input relational expression
     * @param condition boolean expression which determines whether a row is
     * allowed to pass
     */
    init {
        this.condition = requireNonNull(condition, "condition")
        assert(RexUtil.isFlat(condition)) { "RexUtil.isFlat should be true for condition $condition" }
        assert(isValid(Litmus.THROW, null))
    }

    /**
     * Creates a Filter by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), input.getInput(),
        requireNonNull(input.getExpression("condition"), "condition")
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): RelNode {
        return copy(traitSet, sole(inputs), getCondition())
    }

    abstract fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        condition: RexNode?
    ): Filter

    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val condition: RexNode = shuttle.apply(condition)
        return if (this.condition === condition) {
            this
        } else copy(traitSet, getInput(), condition)
    }

    fun getCondition(): RexNode {
        return condition
    }

    /** Returns whether this Filter contains any windowed-aggregate functions.  */
    fun containsOver(): Boolean {
        return RexOver.containsOver(condition)
    }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        if (RexUtil.isNullabilityCast(getCluster().getTypeFactory(), condition)) {
            return litmus.fail("Cast for just nullability not allowed")
        }
        val checker = RexChecker(getInput().getRowType(), context, litmus)
        condition.accept(checker)
        return if (checker.getFailureCount() > 0) {
            litmus.fail(null)
        } else litmus.succeed()
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        val dRows: Double = mq.getRowCount(this)
        val dCpu: Double = mq.getRowCount(getInput())
        val dIo = 0.0
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo)
    }

    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        return RelMdUtil.estimateFilteredRows(getInput(), condition, mq)
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("condition", condition)
    }

    @API(since = "1.24", status = API.Status.INTERNAL)
    @EnsuresNonNullIf(expression = "#1", result = true)
    protected fun deepEquals0(@Nullable obj: Object?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || getClass() !== obj.getClass()) {
            return false
        }
        val o = obj as Filter
        return (traitSet.equals(o.traitSet)
                && input.deepEquals(o.input)
                && condition.equals(o.condition)
                && getRowType().equalsSansFieldNames(o.getRowType()))
    }

    @API(since = "1.24", status = API.Status.INTERNAL)
    protected fun deepHashCode0(): Int {
        return Objects.hash(traitSet, input.deepHashCode(), condition)
    }

    companion object {
        @Deprecated // to be removed before 2.0
        fun estimateFilteredRows(child: RelNode, program: RexProgram?): Double {
            val mq: RelMetadataQuery = child.getCluster().getMetadataQuery()
            return RelMdUtil.estimateFilteredRows(child, program, mq)
        }

        @Deprecated // to be removed before 2.0
        fun estimateFilteredRows(child: RelNode, condition: RexNode?): Double {
            val mq: RelMetadataQuery = child.getCluster().getMetadataQuery()
            return RelMdUtil.estimateFilteredRows(child, condition, mq)
        }
    }
}
