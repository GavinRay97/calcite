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
 * A relational operator that performs nested-loop joins.
 *
 *
 * It behaves like a kind of [org.apache.calcite.rel.core.Join],
 * but works by setting variables in its environment and restarting its
 * right-hand input.
 *
 *
 * A LogicalCorrelate is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 *
 * @see org.apache.calcite.rel.core.CorrelationId
 */
class LogicalCorrelate  //~ Instance fields --------------------------------------------------------
//~ Constructors -----------------------------------------------------------
/**
 * Creates a LogicalCorrelate.
 * @param cluster      cluster this relational expression belongs to
 * @param left         left input relational expression
 * @param right        right input relational expression
 * @param correlationId variable name for the row of left input
 * @param requiredColumns Required columns
 * @param joinType     join type
 */
    (
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    left: RelNode?,
    right: RelNode?,
    correlationId: CorrelationId?,
    requiredColumns: ImmutableBitSet?,
    joinType: JoinRelType?
) : Correlate(
    cluster,
    traitSet,
    left,
    right,
    correlationId,
    requiredColumns,
    joinType
) {
    /**
     * Creates a LogicalCorrelate by parsing serialized output.
     */
    constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
        input.getInputs().get(1),
        CorrelationId(
            requireNonNull(input.get("correlation"), "correlation") as Integer?
        ),
        input.getBitSet("requiredColumns"),
        requireNonNull(input.getEnum("joinType", JoinRelType::class.java), "joinType")
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet,
        left: RelNode?, right: RelNode?, correlationId: CorrelationId?,
        requiredColumns: ImmutableBitSet?, joinType: JoinRelType?
    ): LogicalCorrelate {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return LogicalCorrelate(
            getCluster(), traitSet, left, right,
            correlationId, requiredColumns, joinType
        )
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    companion object {
        /** Creates a LogicalCorrelate.  */
        fun create(
            left: RelNode, right: RelNode?,
            correlationId: CorrelationId?, requiredColumns: ImmutableBitSet?,
            joinType: JoinRelType?
        ): LogicalCorrelate {
            val cluster: RelOptCluster = left.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return LogicalCorrelate(
                cluster, traitSet, left, right, correlationId,
                requiredColumns, joinType
            )
        }
    }
}
