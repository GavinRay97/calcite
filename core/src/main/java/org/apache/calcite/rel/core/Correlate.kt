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
 * A relational operator that performs nested-loop joins.
 *
 *
 * It behaves like a kind of [org.apache.calcite.rel.core.Join],
 * but works by setting variables in its environment and restarting its
 * right-hand input.
 *
 *
 * Correlate is not a join since: typical rules should not match Correlate.
 *
 *
 * A Correlate is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 *
 * <table>
 * <caption>Mapping of physical operations to logical ones</caption>
 * <tr><th>Physical operation</th><th>Logical operation</th></tr>
 * <tr><td>NestedLoops</td><td>Correlate(A, B, regular)</td></tr>
 * <tr><td>NestedLoopsOuter</td><td>Correlate(A, B, outer)</td></tr>
 * <tr><td>NestedLoopsSemi</td><td>Correlate(A, B, semi)</td></tr>
 * <tr><td>NestedLoopsAnti</td><td>Correlate(A, B, anti)</td></tr>
 * <tr><td>HashJoin</td><td>EquiJoin(A, B)</td></tr>
 * <tr><td>HashJoinOuter</td><td>EquiJoin(A, B, outer)</td></tr>
 * <tr><td>HashJoinSemi</td><td>SemiJoin(A, B, semi)</td></tr>
 * <tr><td>HashJoinAnti</td><td>SemiJoin(A, B, anti)</td></tr>
</table> *
 *
 * @see CorrelationId
 */
abstract class Correlate @SuppressWarnings("method.invocation.invalid") protected constructor(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    left: RelNode?,
    right: RelNode?,
    correlationId: CorrelationId?,
    requiredColumns: ImmutableBitSet?,
    joinType: JoinRelType
) : BiRel(cluster, traitSet, left, right) {
    //~ Instance fields --------------------------------------------------------
    protected val correlationId: CorrelationId
    protected val requiredColumns: ImmutableBitSet
    protected val joinType: JoinRelType
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Correlate.
     *
     * @param cluster      Cluster this relational expression belongs to
     * @param left         Left input relational expression
     * @param right        Right input relational expression
     * @param correlationId Variable name for the row of left input
     * @param requiredColumns Set of columns that are used by correlation
     * @param joinType Join type
     */
    init {
        assert(!joinType.generatesNullsOnLeft()) { "Correlate has invalid join type $joinType" }
        this.joinType = requireNonNull(joinType, "joinType")
        this.correlationId = requireNonNull(correlationId, "correlationId")
        this.requiredColumns = requireNonNull(requiredColumns, "requiredColumns")
        assert(isValid(Litmus.THROW, null))
    }

    /**
     * Creates a Correlate by parsing serialized output.
     *
     * @param input Input representation
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
        input.getInputs().get(1),
        CorrelationId(
            requireNonNull(input.get("correlation") as Integer, "correlation")
        ),
        input.getBitSet("requiredColumns"),
        requireNonNull(input.getEnum("joinType", JoinRelType::class.java), "joinType")
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        val leftColumns: ImmutableBitSet = ImmutableBitSet.range(left.getRowType().getFieldCount())
        return (super.isValid(litmus, context)
                && litmus.check(
            leftColumns.contains(requiredColumns),
            "Required columns {} not subset of left columns {}", requiredColumns, leftColumns
        )
                && RelOptUtil.notContainsCorrelation(left, correlationId, litmus))
    }

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): Correlate {
        assert(inputs.size() === 2)
        return copy(
            traitSet,
            inputs[0],
            inputs[1],
            correlationId,
            requiredColumns,
            joinType
        )
    }

    abstract fun copy(
        traitSet: RelTraitSet?,
        left: RelNode?, right: RelNode?, correlationId: CorrelationId?,
        requiredColumns: ImmutableBitSet?, joinType: JoinRelType?
    ): Correlate

    fun getJoinType(): JoinRelType {
        return joinType
    }

    @Override
    protected fun deriveRowType(): RelDataType {
        return when (joinType) {
            LEFT, INNER -> SqlValidatorUtil.deriveJoinRowType(
                left.getRowType(),
                right.getRowType(), joinType,
                getCluster().getTypeFactory(), null,
                ImmutableList.of()
            )
            ANTI, SEMI -> left.getRowType()
            else -> throw IllegalStateException("Unknown join type $joinType")
        }
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("correlation", correlationId)
            .item("joinType", joinType.lowerName)
            .item("requiredColumns", requiredColumns)
    }

    /**
     * Returns the correlating expressions.
     *
     * @return correlating expressions
     */
    fun getCorrelationId(): CorrelationId {
        return correlationId
    }

    @get:Override
    val correlVariable: String
        get() = correlationId.getName()

    /**
     * Returns the required columns in left relation required for the correlation
     * in the right.
     *
     * @return columns in left relation required for the correlation in the right
     */
    fun getRequiredColumns(): ImmutableBitSet {
        return requiredColumns
    }

    @get:Override
    val variablesSet: Set<org.apache.calcite.rel.core.CorrelationId>
        get() = ImmutableSet.of(correlationId)

    @Override
    fun estimateRowCount(mq: RelMetadataQuery): Double {
        val leftRowCount: Double = mq.getRowCount(left)
        return when (joinType) {
            SEMI, ANTI -> leftRowCount
            else -> leftRowCount * mq.getRowCount(right)
        }
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        val rowCount: Double = mq.getRowCount(this)
        val rightRowCount: Double = right.estimateRowCount(mq)
        val leftRowCount: Double = left.estimateRowCount(mq)
        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeInfiniteCost()
        }
        val restartCount: Double = mq.getRowCount(getLeft()) ?: return planner.getCostFactory().makeInfiniteCost()
        // RelMetadataQuery.getCumulativeCost(getRight()); does not work for
        // RelSubset, so we ask planner to cost-estimate right relation
        val rightCost: RelOptCost =
            planner.getCost(getRight(), mq) ?: return planner.getCostFactory().makeInfiniteCost()
        val rescanCost: RelOptCost = rightCost.multiplyBy(Math.max(1.0, restartCount - 1))
        return planner.getCostFactory().makeCost(
            rowCount /* generate results */ + leftRowCount /* scan left results */,
            0, 0
        ).plus(rescanCost)
    }
}
