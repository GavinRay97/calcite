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
 * Sub-class of [Match]
 * not targeted at any particular engine or calling convention.
 */
class LogicalMatch
/**
 * Creates a LogicalMatch.
 *
 *
 * Use [.create] unless you know what you're doing.
 *
 * @param cluster Cluster
 * @param traitSet Trait set
 * @param input Input relational expression
 * @param rowType Row type
 * @param pattern Regular Expression defining pattern variables
 * @param strictStart Whether it is a strict start pattern
 * @param strictEnd Whether it is a strict end pattern
 * @param patternDefinitions Pattern definitions
 * @param measures Measure definitions
 * @param after After match definitions
 * @param subsets Subset definitions
 * @param allRows Whether all rows per match (false means one row per match)
 * @param partitionKeys Partition by columns
 * @param orderKeys Order by columns
 * @param interval Interval definition, null if WITHIN clause is not defined
 */
    (
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, rowType: RelDataType?, pattern: RexNode?,
    strictStart: Boolean, strictEnd: Boolean,
    patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
    after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?,
    allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
    @Nullable interval: RexNode?
) : Match(
    cluster, traitSet, input, rowType, pattern, strictStart, strictEnd,
    patternDefinitions, measures, after, subsets, allRows, partitionKeys,
    orderKeys, interval
) {
    //~ Methods ------------------------------------------------------
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): RelNode {
        return LogicalMatch(
            getCluster(), traitSet, inputs[0], getRowType(),
            pattern, strictStart, strictEnd, patternDefinitions, measures, after,
            subsets, allRows, partitionKeys, orderKeys, interval
        )
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    companion object {
        /**
         * Creates a LogicalMatch.
         */
        fun create(
            input: RelNode, rowType: RelDataType?,
            pattern: RexNode?, strictStart: Boolean, strictEnd: Boolean,
            patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
            after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?, allRows: Boolean,
            partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?, @Nullable interval: RexNode?
        ): LogicalMatch {
            val cluster: RelOptCluster = input.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return create(
                cluster, traitSet, input, rowType, pattern,
                strictStart, strictEnd, patternDefinitions, measures, after, subsets,
                allRows, partitionKeys, orderKeys, interval
            )
        }

        /**
         * Creates a LogicalMatch.
         */
        fun create(
            cluster: RelOptCluster?,
            traitSet: RelTraitSet?, input: RelNode?, rowType: RelDataType?,
            pattern: RexNode?, strictStart: Boolean, strictEnd: Boolean,
            patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
            after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?,
            allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
            @Nullable interval: RexNode?
        ): LogicalMatch {
            return LogicalMatch(
                cluster, traitSet, input, rowType, pattern,
                strictStart, strictEnd, patternDefinitions, measures, after, subsets,
                allRows, partitionKeys, orderKeys, interval
            )
        }
    }
}
