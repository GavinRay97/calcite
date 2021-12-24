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
package org.apache.calcite.rel.logical

import org.apache.calcite.plan.Convention

/**
 * Sub-class of [org.apache.calcite.rel.core.Join]
 * not targeted at any particular engine or calling convention.
 *
 *
 * Some rules:
 *
 *
 *  * [org.apache.calcite.rel.rules.JoinExtractFilterRule] converts an
 * [inner join][LogicalJoin] to a [filter][LogicalFilter] on top of a
 * [cartesian inner join][LogicalJoin].
 *
 *  * `net.sf.farrago.fennel.rel.FennelCartesianJoinRule`
 * implements a LogicalJoin as a cartesian product.
 *
 *
 */
class LogicalJoin(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    hints: List<RelHint?>?,
    left: RelNode?,
    right: RelNode?,
    condition: RexNode?,
    variablesSet: Set<CorrelationId?>?,
    joinType: JoinRelType?,
    //~ Instance fields --------------------------------------------------------
    // NOTE jvs 14-Mar-2006:  Normally we don't use state like this
    // to control rule firing, but due to the non-local nature of
    // semijoin optimizations, it's pretty much required.
    @get:Override val isSemiJoinDone: Boolean,
    systemFieldList: ImmutableList<RelDataTypeField?>?
) : Join(cluster, traitSet, hints, left, right, condition, variablesSet, joinType) {
    private val systemFieldList: ImmutableList<RelDataTypeField?>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalJoin.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster          Cluster
     * @param traitSet         Trait set
     * @param hints            Hints
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     * @param joinType         Join type
     * @param variablesSet     Set of variables that are set by the
     * LHS and used by the RHS and are not available to
     * nodes above this LogicalJoin in the tree
     * @param semiJoinDone     Whether this join has been translated to a
     * semi-join
     * @param systemFieldList  List of system fields that will be prefixed to
     * output row type; typically empty but must not be
     * null
     * @see .isSemiJoinDone
     */
    init {
        this.systemFieldList = requireNonNull(systemFieldList, "systemFieldList")
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        left: RelNode?, right: RelNode?, condition: RexNode?, variablesSet: Set<CorrelationId?>?,
        joinType: JoinRelType?, semiJoinDone: Boolean,
        systemFieldList: ImmutableList<RelDataTypeField?>?
    ) : this(
        cluster, traitSet, ImmutableList.of(), left, right, condition,
        variablesSet, joinType, semiJoinDone, systemFieldList
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?, left: RelNode?,
        right: RelNode?, condition: RexNode?, joinType: JoinRelType?,
        variablesStopped: Set<String?>?, semiJoinDone: Boolean,
        systemFieldList: ImmutableList<RelDataTypeField?>?
    ) : this(
        cluster, traitSet, ImmutableList.of(), left, right, condition,
        CorrelationId.setOf(variablesStopped), joinType, semiJoinDone,
        systemFieldList
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster, left: RelNode?, right: RelNode?,
        condition: RexNode?, joinType: JoinRelType?, variablesStopped: Set<String?>?
    ) : this(
        cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(),
        left, right, condition, CorrelationId.setOf(variablesStopped),
        joinType, false, ImmutableList.of()
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster, left: RelNode?, right: RelNode?,
        condition: RexNode?, joinType: JoinRelType?, variablesStopped: Set<String?>?,
        semiJoinDone: Boolean, systemFieldList: ImmutableList<RelDataTypeField?>?
    ) : this(
        cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(),
        left, right, condition, CorrelationId.setOf(variablesStopped), joinType,
        semiJoinDone, systemFieldList
    ) {
    }

    /**
     * Creates a LogicalJoin by parsing serialized output.
     */
    constructor(input: RelInput) : this(
        input.getCluster(), input.getCluster().traitSetOf(Convention.NONE),
        ArrayList(),
        input.getInputs().get(0), input.getInputs().get(1),
        requireNonNull(input.getExpression("condition"), "condition"),
        ImmutableSet.of(),
        requireNonNull(input.getEnum("joinType", JoinRelType::class.java), "joinType"),
        false,
        ImmutableList.of()
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet, conditionExpr: RexNode?,
        left: RelNode?, right: RelNode?, joinType: JoinRelType?, semiJoinDone: Boolean
    ): LogicalJoin {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return LogicalJoin(
            getCluster(),
            getCluster().traitSetOf(Convention.NONE), hints, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList
        )
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
            .itemIf("semiJoinDone", isSemiJoinDone, isSemiJoinDone)
    }

    @Override
    fun deepEquals(@Nullable obj: Object): Boolean {
        return if (this == obj) {
            true
        } else deepEquals0(obj)
                && isSemiJoinDone == (obj as LogicalJoin).isSemiJoinDone && systemFieldList.equals((obj as LogicalJoin).systemFieldList)
    }

    @Override
    fun deepHashCode(): Int {
        return Objects.hash(deepHashCode0(), isSemiJoinDone, systemFieldList)
    }

    @Override
    fun getSystemFieldList(): List<RelDataTypeField?> {
        return systemFieldList
    }

    @Override
    fun withHints(hintList: List<RelHint?>?): RelNode {
        return LogicalJoin(
            getCluster(), traitSet, hintList,
            left, right, condition, variablesSet, joinType, isSemiJoinDone, systemFieldList
        )
    }

    companion object {
        /** Creates a LogicalJoin.  */
        fun create(
            left: RelNode, right: RelNode?, hints: List<RelHint?>?,
            condition: RexNode?, variablesSet: Set<CorrelationId?>?, joinType: JoinRelType?
        ): LogicalJoin {
            return create(
                left, right, hints, condition, variablesSet, joinType, false,
                ImmutableList.of()
            )
        }

        /** Creates a LogicalJoin, flagged with whether it has been translated to a
         * semi-join.  */
        fun create(
            left: RelNode, right: RelNode?, hints: List<RelHint?>?,
            condition: RexNode?, variablesSet: Set<CorrelationId?>?, joinType: JoinRelType?,
            semiJoinDone: Boolean, systemFieldList: ImmutableList<RelDataTypeField?>?
        ): LogicalJoin {
            val cluster: RelOptCluster = left.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return LogicalJoin(
                cluster, traitSet, hints, left, right, condition,
                variablesSet, joinType, semiJoinDone, systemFieldList
            )
        }
    }
}
