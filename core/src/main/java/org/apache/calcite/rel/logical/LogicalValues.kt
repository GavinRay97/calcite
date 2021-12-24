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
 * Sub-class of [org.apache.calcite.rel.core.Values]
 * not targeted at any particular engine or calling convention.
 */
class LogicalValues : Values {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalValues.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param rowType Row type for tuples produced by this rel
     * @param tuples  2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all
     * tuples must be of same length, conforming to rowType
     */
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        rowType: RelDataType?,
        tuples: ImmutableList<ImmutableList<RexLiteral?>?>?
    ) : super(cluster, rowType, tuples, traitSet) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster,
        rowType: RelDataType?,
        tuples: ImmutableList<ImmutableList<RexLiteral?>?>?
    ) : this(cluster, cluster.traitSetOf(Convention.NONE), rowType, tuples) {
    }

    /**
     * Creates a LogicalValues by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>): RelNode {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        assert(inputs.isEmpty())
        return LogicalValues(getCluster(), traitSet, getRowType(), tuples)
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    companion object {
        /** Creates a LogicalValues.  */
        fun create(
            cluster: RelOptCluster,
            rowType: RelDataType?,
            tuples: ImmutableList<ImmutableList<RexLiteral?>?>?
        ): LogicalValues {
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.values(mq, rowType, tuples) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.values(rowType, tuples) }
            return LogicalValues(cluster, traitSet, rowType, tuples)
        }

        /** Creates a LogicalValues that outputs no rows of a given row type.  */
        fun createEmpty(
            cluster: RelOptCluster,
            rowType: RelDataType?
        ): LogicalValues {
            return create(
                cluster, rowType,
                ImmutableList.of()
            )
        }

        /** Creates a LogicalValues that outputs one row and one column.  */
        fun createOneRow(cluster: RelOptCluster): LogicalValues {
            val rowType: RelDataType = cluster.getTypeFactory().builder()
                .add("ZERO", SqlTypeName.INTEGER).nullable(false)
                .build()
            val tuples: ImmutableList<ImmutableList<RexLiteral?>?> = ImmutableList.of(
                ImmutableList.of(
                    cluster.getRexBuilder().makeExactLiteral(
                        BigDecimal.ZERO,
                        rowType.getFieldList().get(0).getType()
                    )
                )
            )
            return create(cluster, rowType, tuples)
        }
    }
}
