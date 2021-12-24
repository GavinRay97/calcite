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
 * RelMdMinRowCount supplies a default implementation of
 * [RelMetadataQuery.getMinRowCount] for the standard logical algebra.
 */
class RelMdMinRowCount : MetadataHandler<BuiltInMetadata.MinRowCount?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.MinRowCount.DEF

    fun getMinRowCount(rel: Union, mq: RelMetadataQuery): Double {
        var rowCount = 0.0
        for (input in rel.getInputs()) {
            val partialRowCount: Double = mq.getMinRowCount(input)
            if (partialRowCount != null) {
                rowCount += partialRowCount
            }
        }
        return if (rel.all) {
            rowCount
        } else {
            Math.min(rowCount, 1.0)
        }
    }

    fun getMinRowCount(rel: Intersect?, mq: RelMetadataQuery?): Double {
        return 0.0 // no lower bound
    }

    fun getMinRowCount(rel: Minus?, mq: RelMetadataQuery?): Double {
        return 0.0 // no lower bound
    }

    fun getMinRowCount(rel: Filter?, mq: RelMetadataQuery?): Double {
        return 0.0 // no lower bound
    }

    @Nullable
    fun getMinRowCount(rel: Calc, mq: RelMetadataQuery): Double {
        return if (rel.getProgram().getCondition() != null) {
            // no lower bound
            0.0
        } else {
            mq.getMinRowCount(rel.getInput())
        }
    }

    @Nullable
    fun getMinRowCount(rel: Project, mq: RelMetadataQuery): Double {
        return mq.getMinRowCount(rel.getInput())
    }

    @Nullable
    fun getMinRowCount(rel: Exchange, mq: RelMetadataQuery): Double {
        return mq.getMinRowCount(rel.getInput())
    }

    @Nullable
    fun getMinRowCount(rel: TableModify, mq: RelMetadataQuery): Double {
        return mq.getMinRowCount(rel.getInput())
    }

    fun getMinRowCount(rel: Sort, mq: RelMetadataQuery): Double? {
        var rowCount: Double = mq.getMinRowCount(rel.getInput())
        if (rowCount == null) {
            rowCount = 0.0
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

    fun getMinRowCount(rel: EnumerableLimit, mq: RelMetadataQuery): Double? {
        var rowCount: Double = mq.getMinRowCount(rel.getInput())
        if (rowCount == null) {
            rowCount = 0.0
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

    fun getMinRowCount(rel: Aggregate, mq: RelMetadataQuery): Double {
        if (rel.getGroupSet().isEmpty()) {
            // Aggregate with no GROUP BY always returns 1 row (even on empty table).
            return 1.0
        }
        val rowCount: Double = mq.getMinRowCount(rel.getInput())
        return if (rowCount != null && rowCount >= 1.0) {
            rel.getGroupSets().size()
        } else 0.0
    }

    fun getMinRowCount(rel: Join?, mq: RelMetadataQuery?): Double {
        return 0.0
    }

    fun getMinRowCount(rel: TableScan?, mq: RelMetadataQuery?): Double {
        return 0.0
    }

    fun getMinRowCount(values: Values, mq: RelMetadataQuery?): Double {
        // For Values, the minimum row count is the actual row count.
        return values.getTuples().size()
    }

    fun getMinRowCount(rel: RelSubset, mq: RelMetadataQuery?): Double {
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
        return 0.0
    }

    // Catch-all rule when none of the others apply.
    @Nullable
    fun getMinRowCount(rel: RelNode?, mq: RelMetadataQuery?): Double? {
        return null
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdMinRowCount(), BuiltInMetadata.MinRowCount.Handler::class.java
        )
    }
}
