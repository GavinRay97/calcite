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
 * Relational expression that returns the contents of a relation expression as
 * it was at a given time in the past.
 *
 *
 * For example, if `Products` is a temporal table, and
 * [TableScan](Products) is a relational operator that returns all
 * versions of the contents of the table, then
 * [Snapshot](TableScan(Products)) is a relational operator that only
 * returns the contents whose versions that overlap with the given specific
 * period (i.e. those that started before given period and ended after it).
 */
abstract class Snapshot @SuppressWarnings("method.invocation.invalid") protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
    period: RexNode?
) : SingleRel(cluster, traitSet, input) {
    //~ Instance fields --------------------------------------------------------
    private val period: RexNode
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Snapshot.
     *
     * @param cluster   Cluster that this relational expression belongs to
     * @param traitSet  The traits of this relational expression
     * @param input     Input relational expression
     * @param period    Timestamp expression which as the table was at the given
     * time in the past
     */
    init {
        this.period = Objects.requireNonNull(period, "period")
        assert(isValid(Litmus.THROW, null))
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return copy(traitSet, sole(inputs), getPeriod())
    }

    abstract fun copy(traitSet: RelTraitSet?, input: RelNode?, period: RexNode?): Snapshot
    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val condition: RexNode = shuttle.apply(period)
        return if (period === condition) {
            this
        } else copy(traitSet, getInput(), condition)
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("period", period)
    }

    fun getPeriod(): RexNode {
        return period
    }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        val dataType: RelDataType = period.getType()
        return if (dataType.getSqlTypeName() !== SqlTypeName.TIMESTAMP) {
            litmus.fail(
                ("The system time period specification expects Timestamp type but is '"
                        + dataType.getSqlTypeName()) + "'"
            )
        } else litmus.succeed()
    }
}
