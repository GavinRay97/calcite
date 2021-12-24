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

import org.apache.calcite.plan.RelOptUtil

/**
 * RelMdDistinctRowCount supplies a default implementation of
 * [RelMetadataQuery.getDistinctRowCount] for the standard logical
 * algebra.
 */
class RelMdDistinctRowCount  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<BuiltInMetadata.DistinctRowCount?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.DistinctRowCount.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.DistinctRowCount.getDistinctRowCount],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getDistinctRowCount
     */
    @Nullable
    fun getDistinctRowCount(
        rel: RelNode?, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?
    ): Double? {
        // REVIEW zfong 4/19/06 - Broadbase code does not take into
        // consideration selectivity of predicates passed in.  Also, they
        // assume the rows are unique even if the table is not
        val uniq: Boolean = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey)
        return if (uniq) {
            NumberUtil.multiply(
                mq.getRowCount(rel),
                mq.getSelectivity(rel, predicate)
            )
        } else null
    }

    @Nullable
    fun getDistinctRowCount(
        rel: Union, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?
    ): Double? {
        var rowCount = 0.0
        val adjustments = IntArray(rel.getRowType().getFieldCount())
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        for (input in rel.getInputs()) {
            // convert the predicate to reference the types of the union child
            var modifiedPred: RexNode?
            modifiedPred = if (predicate == null) {
                null
            } else {
                predicate.accept(
                    RexInputConverter(
                        rexBuilder,
                        null,
                        input.getRowType().getFieldList(),
                        adjustments
                    )
                )
            }
            val partialRowCount: Double = mq.getDistinctRowCount(input, groupKey, modifiedPred) ?: return null
            rowCount += partialRowCount
        }
        return rowCount
    }

    @Nullable
    fun getDistinctRowCount(
        rel: Sort, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?
    ): Double? {
        return mq.getDistinctRowCount(rel.getInput(), groupKey, predicate)
    }

    @Nullable
    fun getDistinctRowCount(
        rel: TableModify, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?
    ): Double? {
        return mq.getDistinctRowCount(rel.getInput(), groupKey, predicate)
    }

    @Nullable
    fun getDistinctRowCount(
        rel: Exchange, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?
    ): Double? {
        return mq.getDistinctRowCount(rel.getInput(), groupKey, predicate)
    }

    @Nullable
    fun getDistinctRowCount(
        rel: Filter, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet, @Nullable predicate: RexNode?
    ): Double? {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1.0
            }
        }
        // REVIEW zfong 4/18/06 - In the Broadbase code, duplicates are not
        // removed from the two filter lists.  However, the code below is
        // doing so.
        val unionPreds: RexNode = RelMdUtil.unionPreds(
            rel.getCluster().getRexBuilder(),
            predicate,
            rel.getCondition()
        )
        return mq.getDistinctRowCount(rel.getInput(), groupKey, unionPreds)
    }

    @Nullable
    fun getDistinctRowCount(
        rel: Join, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet, @Nullable predicate: RexNode?
    ): Double? {
        return RelMdUtil.getJoinDistinctRowCount(
            mq, rel, rel.getJoinType(),
            groupKey, predicate, false
        )
    }

    @Nullable
    fun getDistinctRowCount(
        rel: Aggregate, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet, @Nullable predicate: RexNode?
    ): Double? {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1.0
            }
        }
        // determine which predicates can be applied on the child of the
        // aggregate
        val notPushable: List<RexNode> = ArrayList()
        val pushable: List<RexNode> = ArrayList()
        RelOptUtil.splitFilters(
            rel.getGroupSet(),
            predicate,
            pushable,
            notPushable
        )
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        val childPreds: RexNode = RexUtil.composeConjunction(rexBuilder, pushable, true)

        // set the bits as they correspond to the child input
        val childKey: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        RelMdUtil.setAggChildKeys(groupKey, rel, childKey)
        val distinctRowCount: Double = mq.getDistinctRowCount(rel.getInput(), childKey.build(), childPreds)
        return if (distinctRowCount == null) {
            null
        } else if (notPushable.isEmpty()) {
            distinctRowCount
        } else {
            val preds: RexNode = RexUtil.composeConjunction(rexBuilder, notPushable, true)
            distinctRowCount * RelMdUtil.guessSelectivity(preds)
        }
    }

    fun getDistinctRowCount(
        rel: Values, mq: RelMetadataQuery?,
        groupKey: ImmutableBitSet, @Nullable predicate: RexNode?
    ): Double? {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1.0
            }
        }
        val set: Set<List<Comparable>> = HashSet()
        val values: List<Comparable> = ArrayList(groupKey.cardinality())
        for (tuple in rel.tuples) {
            for (column in groupKey) {
                val literal: RexLiteral = tuple.get(column)
                val value: Comparable = literal.getValueAs(Comparable::class.java)
                values.add(if (value == null) NullSentinel.INSTANCE else value)
            }
            set.add(ImmutableList.copyOf(values))
            values.clear()
        }
        val nRows: Double = set.size()
        return if (predicate == null || predicate.isAlwaysTrue()) {
            nRows
        } else {
            val selectivity: Double = RelMdUtil.guessSelectivity(predicate)
            RelMdUtil.numDistinctVals(nRows, nRows * selectivity)
        }
    }

    @Nullable
    fun getDistinctRowCount(
        rel: Project, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet, @Nullable predicate: RexNode?
    ): Double? {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1.0
            }
        }

        // try to remove const columns from the group keys, as they do not
        // affect the distinct row count
        val nonConstCols: ImmutableBitSet = RexUtil.getNonConstColumns(groupKey, rel.getProjects())
        if (nonConstCols.cardinality() === 0) {
            // all columns are constants, the distinct row count should be 1
            return 1.0
        }
        if (nonConstCols.cardinality() < groupKey.cardinality()) {
            // some const columns can be removed, call the method recursively
            // with the trimmed columns
            return getDistinctRowCount(rel, mq, nonConstCols, predicate)
        }
        val baseCols: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val projCols: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val projExprs: List<RexNode> = rel.getProjects()
        RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols)
        val notPushable: List<RexNode> = ArrayList()
        val pushable: List<RexNode> = ArrayList()
        RelOptUtil.splitFilters(
            ImmutableBitSet.range(rel.getRowType().getFieldCount()),
            predicate,
            pushable,
            notPushable
        )
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()

        // get the distinct row count of the child input, passing in the
        // columns and filters that only reference the child; convert the
        // filter to reference the children projection expressions
        val childPred: RexNode = RexUtil.composeConjunction(rexBuilder, pushable, true)
        val modifiedPred: RexNode?
        modifiedPred = if (childPred == null) {
            null
        } else {
            RelOptUtil.pushPastProject(childPred, rel)
        }
        var distinctRowCount: Double? = mq.getDistinctRowCount(
            rel.getInput(), baseCols.build(),
            modifiedPred
        )
        if (distinctRowCount == null) {
            return null
        } else if (!notPushable.isEmpty()) {
            val preds: RexNode = RexUtil.composeConjunction(rexBuilder, notPushable, true)
            distinctRowCount *= RelMdUtil.guessSelectivity(preds)
        }

        // No further computation required if the projection expressions
        // are all column references
        if (projCols.cardinality() === 0) {
            return distinctRowCount
        }

        // multiply by the cardinality of the non-child projection expressions
        for (bit in projCols.build()) {
            val subRowCount: Double = RelMdUtil.cardOfProjExpr(mq, rel, projExprs[bit]) ?: return null
            distinctRowCount *= subRowCount
        }
        return RelMdUtil.numDistinctVals(distinctRowCount, mq.getRowCount(rel))
    }

    @Nullable
    fun getDistinctRowCount(
        rel: RelSubset, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?
    ): Double? {
        val best: RelNode = rel.getBest()
        if (best != null) {
            return mq.getDistinctRowCount(best, groupKey, predicate)
        }
        if (!Bug.CALCITE_1048_FIXED) {
            return getDistinctRowCount(rel as RelNode, mq, groupKey, predicate)
        }
        var d: Double? = null
        for (r2 in rel.getRels()) {
            try {
                val d2: Double = mq.getDistinctRowCount(r2, groupKey, predicate)
                d = NumberUtil.min(d, d2)
            } catch (e: CyclicMetadataException) {
                // Ignore this relational expression; there will be non-cyclic ones
                // in this set.
            }
        }
        return d
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdDistinctRowCount(), BuiltInMetadata.DistinctRowCount.Handler::class.java
        )
    }
}
