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
package org.apache.calcite.rel.metadata

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter

/**
 * RelMdPercentageOriginalRows supplies a default implementation of
 * [RelMetadataQuery.getPercentageOriginalRows] for the standard logical
 * algebra.
 */
class RelMdPercentageOriginalRows  //~ Methods ----------------------------------------------------------------
private constructor() {
    @Nullable
    fun getPercentageOriginalRows(rel: Aggregate, mq: RelMetadataQuery): Double {
        // REVIEW jvs 28-Mar-2006: The assumption here seems to be that
        // aggregation does not apply any filtering, so it does not modify the
        // percentage.  That's very much oversimplified.
        return mq.getPercentageOriginalRows(rel.getInput())
    }

    fun getPercentageOriginalRows(rel: Union, mq: RelMetadataQuery): Double? {
        var numerator = 0.0
        var denominator = 0.0

        // Ignore rel.isDistinct() because it's the same as an aggregate.

        // REVIEW jvs 28-Mar-2006: The original Broadbase formula was broken.
        // It was multiplying percentage into the numerator term rather than
        // than dividing it out of the denominator term, which would be OK if
        // there weren't summation going on.  Probably the cause of the error
        // was the desire to avoid division by zero, which I don't know how to
        // handle so I punt, meaning we return a totally wrong answer in the
        // case where a huge table has been completely filtered away.
        for (input in rel.getInputs()) {
            val rowCount: Double = mq.getRowCount(input) ?: continue
            val percentage: Double = mq.getPercentageOriginalRows(input) ?: continue
            if (percentage != 0.0) {
                denominator += rowCount / percentage
                numerator += rowCount
            }
        }
        return RelMdPercentageOriginalRows.Companion.quotientForPercentage(numerator, denominator)
    }

    @Nullable
    fun getPercentageOriginalRows(rel: Join, mq: RelMetadataQuery): Double? {
        // Assume any single-table filter conditions have already
        // been pushed down.

        // REVIEW jvs 28-Mar-2006: As with aggregation, this is
        // oversimplified.

        // REVIEW jvs 28-Mar-2006:  need any special casing for SemiJoin?
        val left: Double = mq.getPercentageOriginalRows(rel.getLeft()) ?: return null
        val right: Double = mq.getPercentageOriginalRows(rel.getRight()) ?: return null
        return left * right
    }

    // Catch-all rule when none of the others apply.
    @Nullable
    fun getPercentageOriginalRows(rel: RelNode, mq: RelMetadataQuery): Double? {
        if (rel.getInputs().size() > 1) {
            // No generic formula available for multiple inputs.
            return null
        }
        if (rel.getInputs().size() === 0) {
            // Assume no filtering happening at leaf.
            return 1.0
        }
        val child: RelNode = rel.getInputs().get(0)
        val childPercentage: Double = mq.getPercentageOriginalRows(child) ?: return null

        // Compute product of percentage filtering from this rel (assuming any
        // filtering is the effect of single-table filters) with the percentage
        // filtering performed by the child.
        val relPercentage: Double = RelMdPercentageOriginalRows.Companion.quotientForPercentage(
            mq.getRowCount(rel),
            mq.getRowCount(child)
        )
            ?: return null
        val percent = relPercentage * childPercentage

        // this check is needed in cases where this method is called on a
        // physical rel
        return if (percent < 0.0 || percent > 1.0) {
            null
        } else relPercentage * childPercentage
    }

    // Ditto for getNonCumulativeCost
    @Nullable
    fun getCumulativeCost(rel: RelNode, mq: RelMetadataQuery): RelOptCost? {
        var cost: RelOptCost = mq.getNonCumulativeCost(rel) ?: return null
        val inputs: List<RelNode> = rel.getInputs()
        for (input in inputs) {
            val inputCost: RelOptCost = mq.getCumulativeCost(input) ?: return null
            cost = cost.plus(inputCost)
        }
        return cost
    }

    @Nullable
    fun getCumulativeCost(
        rel: EnumerableInterpreter?,
        mq: RelMetadataQuery
    ): RelOptCost {
        return mq.getNonCumulativeCost(rel)
    }

    // Ditto for getNonCumulativeCost
    @Nullable
    fun getNonCumulativeCost(rel: RelNode, mq: RelMetadataQuery?): RelOptCost {
        return rel.computeSelfCost(rel.getCluster().getPlanner(), mq)
    }

    /**
     * Binds [RelMdPercentageOriginalRows] to [BuiltInMetadata.CumulativeCost].
     */
    class RelMdCumulativeCost : RelMdPercentageOriginalRows(), MetadataHandler<BuiltInMetadata.CumulativeCost?> {
        // to be removed before 2.0
        @get:Override
        @get:Deprecated
        override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        // to be removed before 2.0 get() {
        return BuiltInMetadata.CumulativeCost.DEF
    }
}

/**
 * Binds [RelMdPercentageOriginalRows] to [BuiltInMetadata.NonCumulativeCost].
 */
private class RelMdNonCumulativeCost : RelMdPercentageOriginalRows(),
    MetadataHandler<BuiltInMetadata.NonCumulativeCost?> {
    // to be removed before 2.0
    @get:Deprecated
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Deprecated // to be removed before 2.0
        @Override get() = BuiltInMetadata.NonCumulativeCost.DEF
}

/**
 * Binds [RelMdPercentageOriginalRows] to [BuiltInMetadata.PercentageOriginalRows].
 */
private class RelMdPercentageOriginalRowsHandler : RelMdPercentageOriginalRows(),
    MetadataHandler<BuiltInMetadata.PercentageOriginalRows?> {
    // to be removed before 2.0
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Deprecated // to be removed before 2.0
        @Override get() = BuiltInMetadata.PercentageOriginalRows.DEF
}

companion object {
    val SOURCE: RelMetadataProvider = ChainedRelMetadataProvider.of(
        ImmutableList.of(
            ReflectiveRelMetadataProvider.reflectiveSource(
                RelMdPercentageOriginalRows.RelMdPercentageOriginalRowsHandler(),
                BuiltInMetadata.PercentageOriginalRows.Handler::class.java
            ),
            ReflectiveRelMetadataProvider.reflectiveSource(
                RelMdPercentageOriginalRows.RelMdCumulativeCost(),
                BuiltInMetadata.CumulativeCost.Handler::class.java
            ),
            ReflectiveRelMetadataProvider.reflectiveSource(
                RelMdPercentageOriginalRows.RelMdNonCumulativeCost(),
                BuiltInMetadata.NonCumulativeCost.Handler::class.java
            )
        )
    )

    @PolyNull
    private fun quotientForPercentage(
        @PolyNull numerator: Double?,
        @PolyNull denominator: Double?
    ): Double? {
        if (numerator == null || denominator == null) {
            return null
        }

        // may need epsilon instead
        return if (denominator == 0.0) {
            // cap at 100%
            1.0
        } else {
            numerator / denominator
        }
    }
}
}
