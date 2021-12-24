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
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 *
 * Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
abstract class Join protected constructor(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    hints: List<RelHint?>?,
    left: RelNode?,
    right: RelNode?,
    condition: RexNode?,
    variablesSet: Set<CorrelationId?>?,
    joinType: JoinRelType?
) : BiRel(cluster, traitSet, left, right), Hintable {
    //~ Instance fields --------------------------------------------------------
    protected val condition: RexNode?
    protected val variablesSet: ImmutableSet<CorrelationId>
    protected val hints: ImmutableList<RelHint>

    /**
     * Values must be of enumeration [JoinRelType], except that
     * [JoinRelType.RIGHT] is disallowed.
     */
    protected val joinType: JoinRelType
    protected val joinInfo: JoinInfo
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Join.
     *
     * @param cluster          Cluster
     * @param traitSet         Trait set
     * @param hints            Hints
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     * @param joinType         Join type
     * @param variablesSet     variables that are set by the
     * LHS and used by the RHS and are not available to
     * nodes above this Join in the tree
     */
    init {
        this.condition = Objects.requireNonNull(condition, "condition")
        this.variablesSet = ImmutableSet.copyOf(variablesSet)
        this.joinType = Objects.requireNonNull(joinType, "joinType")
        joinInfo = JoinInfo.of(left, right, condition)
        this.hints = ImmutableList.copyOf(hints)
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?, left: RelNode?,
        right: RelNode?, condition: RexNode?, variablesSet: Set<CorrelationId?>?,
        joinType: JoinRelType?
    ) : this(
        cluster, traitSet, ImmutableList.of(), left, right,
        condition, variablesSet, joinType
    ) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        left: RelNode?,
        right: RelNode?,
        condition: RexNode?,
        joinType: JoinRelType?,
        variablesStopped: Set<String>
    ) : this(
        cluster, traitSet, ImmutableList.of(), left, right, condition,
        CorrelationId.setOf(variablesStopped), joinType
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val condition: RexNode = shuttle.apply(condition)
        return if (this.condition === condition) {
            this
        } else copy(traitSet, condition, left, right, joinType, isSemiJoinDone)
    }

    fun getCondition(): RexNode? {
        return condition
    }

    fun getJoinType(): JoinRelType {
        return joinType
    }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        if (!super.isValid(litmus, context)) {
            return false
        }
        if (getRowType().getFieldCount()
            !== (systemFieldList.size()
                    + left.getRowType().getFieldCount()
                    + if (joinType.projectsRight()) right.getRowType().getFieldCount() else 0)
        ) {
            return litmus.fail("field count mismatch")
        }
        if (condition != null) {
            if (condition.getType().getSqlTypeName() !== SqlTypeName.BOOLEAN) {
                return litmus.fail(
                    "condition must be boolean: {}",
                    condition.getType()
                )
            }
            // The input to the condition is a row type consisting of system
            // fields, left fields, and right fields. Very similar to the
            // output row type, except that fields have not yet been made due
            // due to outer joins.
            val checker = RexChecker(
                getCluster().getTypeFactory().builder()
                    .addAll(systemFieldList)
                    .addAll(getLeft().getRowType().getFieldList())
                    .addAll(getRight().getRowType().getFieldList())
                    .build(),
                context, litmus
            )
            condition.accept(checker)
            if (checker.getFailureCount() > 0) {
                return litmus.fail(
                    checker.getFailureCount()
                            + " failures in condition " + condition
                )
            }
        }
        return litmus.succeed()
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        // Maybe we should remove this for semi-join?
        if (isSemiJoin) {
            // REVIEW jvs 9-Apr-2006:  Just for now...
            return planner.getCostFactory().makeTinyCost()
        }
        val rowCount: Double = mq.getRowCount(this)
        return planner.getCostFactory().makeCost(rowCount, 0, 0)
    }

    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        return Util.first(RelMdUtil.getJoinRowCount(mq, this, condition), 1.0)
    }

    @Override
    fun getVariablesSet(): Set<CorrelationId> {
        return variablesSet
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("condition", condition)
            .item("joinType", joinType.lowerName)
            .itemIf(
                "systemFields",
                systemFieldList,
                !systemFieldList.isEmpty()
            )
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
        val o = obj as Join
        return (traitSet.equals(o.traitSet)
                && left.deepEquals(o.left)
                && right.deepEquals(o.right)
                && condition.equals(o.condition)
                && joinType === o.joinType && hints.equals(o.hints)
                && getRowType().equalsSansFieldNames(o.getRowType()))
    }

    @API(since = "1.24", status = API.Status.INTERNAL)
    protected fun deepHashCode0(): Int {
        return Objects.hash(
            traitSet,
            left.deepHashCode(), right.deepHashCode(),
            condition, joinType, hints
        )
    }

    @Override
    protected fun deriveRowType(): RelDataType {
        return SqlValidatorUtil.deriveJoinRowType(
            left.getRowType(),
            right.getRowType(), joinType, getCluster().getTypeFactory(), null,
            systemFieldList
        )
    }

    /**
     * Returns whether this LogicalJoin has already spawned a
     * `SemiJoin` via
     * [org.apache.calcite.rel.rules.JoinAddRedundantSemiJoinRule].
     *
     *
     * The base implementation returns false.
     *
     * @return whether this join has already spawned a semi join
     */
    val isSemiJoinDone: Boolean
        get() = false

    /**
     * Returns whether this Join is a semijoin.
     *
     * @return true if this Join's join type is semi.
     */
    val isSemiJoin: Boolean
        get() = joinType === JoinRelType.SEMI

    /**
     * Returns a list of system fields that will be prefixed to
     * output row type.
     *
     * @return list of system fields
     */
    val systemFieldList: List<Any>
        get() = Collections.emptyList()

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): Join {
        assert(inputs.size() === 2)
        return copy(
            traitSet, getCondition(), inputs[0], inputs[1],
            joinType, isSemiJoinDone
        )
    }

    /**
     * Creates a copy of this join, overriding condition, system fields and
     * inputs.
     *
     *
     * General contract as [RelNode.copy].
     *
     * @param traitSet      Traits
     * @param conditionExpr Condition
     * @param left          Left input
     * @param right         Right input
     * @param joinType      Join type
     * @param semiJoinDone  Whether this join has been translated to a
     * semi-join
     * @return Copy of this join
     */
    abstract fun copy(
        traitSet: RelTraitSet?, conditionExpr: RexNode?,
        left: RelNode?, right: RelNode?, joinType: JoinRelType?, semiJoinDone: Boolean
    ): Join

    /**
     * Analyzes the join condition.
     *
     * @return Analyzed join condition
     */
    fun analyzeCondition(): JoinInfo {
        return joinInfo
    }

    @Override
    fun getHints(): ImmutableList<RelHint> {
        return hints
    }

    companion object {
        // CHECKSTYLE: IGNORE 1

        @Deprecated // to be removed before 2.0
        @Deprecated("Use {@link RelMdUtil#getJoinRowCount(RelMetadataQuery, Join, RexNode)}. ")
        fun estimateJoinedRows(
            joinRel: Join,
            condition: RexNode?
        ): Double {
            val mq: RelMetadataQuery = joinRel.getCluster().getMetadataQuery()
            return Util.first(RelMdUtil.getJoinRowCount(mq, joinRel, condition), 1.0)
        }

        @Deprecated // to be removed before 2.0
        fun deriveJoinRowType(
            leftType: RelDataType?,
            rightType: RelDataType?,
            joinType: JoinRelType?,
            typeFactory: RelDataTypeFactory?,
            @Nullable fieldNameList: List<String?>?,
            systemFieldList: List<RelDataTypeField?>?
        ): RelDataType {
            return SqlValidatorUtil.deriveJoinRowType(
                leftType, rightType, joinType,
                typeFactory, fieldNameList, systemFieldList
            )
        }

        @Deprecated // to be removed before 2.0
        fun createJoinType(
            typeFactory: RelDataTypeFactory?,
            leftType: RelDataType?,
            rightType: RelDataType?,
            fieldNameList: List<String?>?,
            systemFieldList: List<RelDataTypeField?>?
        ): RelDataType {
            return SqlValidatorUtil.createJoinType(
                typeFactory, leftType, rightType,
                fieldNameList, systemFieldList
            )
        }
    }
}
