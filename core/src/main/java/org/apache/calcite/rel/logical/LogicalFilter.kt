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
 * Sub-class of [org.apache.calcite.rel.core.Filter]
 * not targeted at any particular engine or calling convention.
 */
class LogicalFilter : Filter {
    private val variablesSet: ImmutableSet<CorrelationId>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalFilter.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster   Cluster that this relational expression belongs to
     * @param child     Input relational expression
     * @param condition Boolean expression which determines whether a row is
     * allowed to pass
     * @param variablesSet Correlation variables set by this relational expression
     * to be used by nested expressions
     */
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        child: RelNode?,
        condition: RexNode?,
        variablesSet: ImmutableSet<CorrelationId>?
    ) : super(cluster, traitSet, child, condition) {
        this.variablesSet = Objects.requireNonNull(variablesSet, "variablesSet")
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        child: RelNode?,
        condition: RexNode?
    ) : this(cluster, traitSet, child, condition, ImmutableSet.of()) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster,
        child: RelNode?,
        condition: RexNode?
    ) : this(
        cluster, cluster.traitSetOf(Convention.NONE), child, condition,
        ImmutableSet.of()
    ) {
    }

    /**
     * Creates a LogicalFilter by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {
        variablesSet = ImmutableSet.of()
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun getVariablesSet(): Set<CorrelationId> {
        return variablesSet
    }

    @Override
    fun copy(
        traitSet: RelTraitSet, input: RelNode?,
        condition: RexNode?
    ): LogicalFilter {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return LogicalFilter(
            getCluster(), traitSet, input, condition,
            variablesSet
        )
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty())
    }

    @Override
    fun deepEquals(@Nullable obj: Object): Boolean {
        return (deepEquals0(obj)
                && variablesSet.equals((obj as LogicalFilter).variablesSet))
    }

    @Override
    fun deepHashCode(): Int {
        return Objects.hash(deepHashCode0(), variablesSet)
    }

    companion object {
        /** Creates a LogicalFilter.  */
        fun create(input: RelNode, condition: RexNode?): LogicalFilter {
            return create(input, condition, ImmutableSet.of())
        }

        /** Creates a LogicalFilter.  */
        fun create(
            input: RelNode, condition: RexNode?,
            variablesSet: ImmutableSet<CorrelationId>?
        ): LogicalFilter {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.filter(mq, input) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.filter(mq, input) }
            return LogicalFilter(cluster, traitSet, input, condition, variablesSet)
        }
    }
}
