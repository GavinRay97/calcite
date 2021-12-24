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

import org.apache.calcite.adapter.enumerable.EnumerableLimit

/**
 * RelMdMaxRowCount supplies a default implementation of
 * [RelMetadataQuery.getMaxRowCount] for the standard logical algebra.
 */
class RelMdMaxRowCount : MetadataHandler<BuiltInMetadata.MaxRowCount?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.MaxRowCount.DEF

    @Nullable
    fun getMaxRowCount(rel: Union, mq: RelMetadataQuery): Double? {
        var rowCount = 0.0
        for (input in rel.getInputs()) {
            val partialRowCount: Double = mq.getMaxRowCount(input) ?: return null
            rowCount += partialRowCount
        }
        return rowCount
    }

    @Nullable
    fun getMaxRowCount(rel: Intersect, mq: RelMetadataQuery): Double? {
        // max row count is the smallest of the inputs
        var rowCount: Double? = null
        for (input in rel.getInputs()) {
            val partialRowCount: Double = mq.getMaxRowCount(input)
            if (rowCount == null
                || partialRowCount != null && partialRowCount < rowCount
            ) {
                rowCount = partialRowCount
            }
        }
        return rowCount
    }

    @Nullable
    fun getMaxRowCount(rel: Minus, mq: RelMetadataQuery): Double {
        return mq.getMaxRowCount(rel.getInput(0))
    }

    @Nullable
    fun getMaxRowCount(rel: Filter, mq: RelMetadataQuery): Double {
        return if (rel.getCondition().isAlwaysFalse()) {
            0.0
        } else mq.getMaxRowCount(rel.getInput())
    }

    @Nullable
    fun getMaxRowCount(rel: Calc, mq: RelMetadataQuery): Double {
        return mq.getMaxRowCount(rel.getInput())
    }

    @Nullable
    fun getMaxRowCount(rel: Project, mq: RelMetadataQuery): Double {
        return mq.getMaxRowCount(rel.getInput())
    }

    @Nullable
    fun getMaxRowCount(rel: Exchange, mq: RelMetadataQuery): Double {
        return mq.getMaxRowCount(rel.getInput())
    }

    fun getMaxRowCount(rel: Sort, mq: RelMetadataQuery): Double? {
        var rowCount: Double = mq.getMaxRowCount(rel.getInput())
        if (rowCount == null) {
            rowCount = Double.POSITIVE_INFINITY
        }
        val offset = if (rel.offset == null) 0 else RexLiteral.intValue(rel.offset)
        rowCount = Math.max(rowCount - offset, 0.0)
        if (rel.fetch != null) {
            val limit: Int = RexLiteral.intValue(rel.fetch)
            if (limit < rowCount) {
                return limit.toDouble()
            }
        }
        return rowCount
    }

    fun getMaxRowCount(rel: EnumerableLimit, mq: RelMetadataQuery): Double? {
        var rowCount: Double = mq.getMaxRowCount(rel.getInput())
        if (rowCount == null) {
            rowCount = Double.POSITIVE_INFINITY
        }
        val offset = if (rel.offset == null) 0 else RexLiteral.intValue(rel.offset)
        rowCount = Math.max(rowCount - offset, 0.0)
        if (rel.fetch != null) {
            val limit: Int = RexLiteral.intValue(rel.fetch)
            if (limit < rowCount) {
                return limit.toDouble()
            }
        }
        return rowCount
    }

    @Nullable
    fun getMaxRowCount(rel: Aggregate, mq: RelMetadataQuery): Double? {
        if (rel.getGroupSet().isEmpty()) {
            // Aggregate with no GROUP BY always returns 1 row (even on empty table).
            return 1.0
        }

        // Aggregate with constant GROUP BY always returns 1 row
        if (rel.getGroupType() === Aggregate.Group.SIMPLE) {
            val predicateList: RelOptPredicateList = mq.getPulledUpPredicates(rel.getInput())
            if (!RelOptPredicateList.isEmpty(predicateList)
                && allGroupKeysAreConstant(rel, predicateList)
            ) {
                return 1.0
            }
        }
        val rowCount: Double = mq.getMaxRowCount(rel.getInput()) ?: return null
        return rowCount * rel.getGroupSets().size()
    }

    @Nullable
    fun getMaxRowCount(rel: Join, mq: RelMetadataQuery): Double? {
        var left: Double = mq.getMaxRowCount(rel.getLeft())
        var right: Double = mq.getMaxRowCount(rel.getRight())
        if (left == null || right == null) {
            return null
        }
        if (left < 1.0 && rel.getJoinType().generatesNullsOnLeft()) {
            left = 1.0
        }
        if (right < 1.0 && rel.getJoinType().generatesNullsOnRight()) {
            right = 1.0
        }
        return left * right
    }

    fun getMaxRowCount(rel: TableScan?, mq: RelMetadataQuery?): Double {
        // For typical tables, there is no upper bound to the number of rows.
        return Double.POSITIVE_INFINITY
    }

    fun getMaxRowCount(values: Values, mq: RelMetadataQuery?): Double {
        // For Values, the maximum row count is the actual row count.
        // This is especially useful if Values is empty.
        return values.getTuples().size()
    }

    @Nullable
    fun getMaxRowCount(rel: TableModify, mq: RelMetadataQuery): Double {
        return mq.getMaxRowCount(rel.getInput())
    }

    fun getMaxRowCount(rel: RelSubset, mq: RelMetadataQuery?): Double {
        // FIXME This is a short-term fix for [CALCITE-1018]. A complete
        // solution will come with [CALCITE-1048].
        Util.discard(Bug.CALCITE_1048_FIXED)
        for (node in rel.getRels()) {
            if (node is Sort) {
                val sort: Sort = node as Sort
                if (sort.fetch != null) {
                    return RexLiteral.intValue(sort.fetch)
                }
            }
        }
        return Double.POSITIVE_INFINITY
    }

    // Catch-all rule when none of the others apply.
    @Nullable
    fun getMaxRowCount(rel: RelNode?, mq: RelMetadataQuery?): Double? {
        return null
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdMaxRowCount(), BuiltInMetadata.MaxRowCount.Handler::class.java
        )

        private fun allGroupKeysAreConstant(
            aggregate: Aggregate,
            predicateList: RelOptPredicateList
        ): Boolean {
            val rexBuilder: RexBuilder = aggregate.getCluster().getRexBuilder()
            for (key in aggregate.getGroupSet()) {
                if (!predicateList.constantMap.containsKey(
                        rexBuilder.makeInputRef(aggregate.getInput(), key)
                    )
                ) {
                    return false
                }
            }
            return true
        }
    }
}
