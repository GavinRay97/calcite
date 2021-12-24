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
 * RelMdRowCount supplies a default implementation of
 * [RelMetadataQuery.getRowCount] for the standard logical algebra.
 */
class RelMdRowCount : MetadataHandler<BuiltInMetadata.RowCount?> {
    //~ Methods ----------------------------------------------------------------
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Override get() = BuiltInMetadata.RowCount.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.RowCount.getRowCount],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getRowCount
     */
    @Nullable
    fun getRowCount(rel: RelNode, mq: RelMetadataQuery?): Double {
        return rel.estimateRowCount(mq)
    }

    @SuppressWarnings("CatchAndPrintStackTrace")
    @Nullable
    fun getRowCount(subset: RelSubset, mq: RelMetadataQuery): Double {
        if (!Bug.CALCITE_1048_FIXED) {
            return mq.getRowCount(subset.getBestOrOriginal())
        }
        var v: Double? = null
        for (r in subset.getRels()) {
            try {
                v = NumberUtil.min(v, mq.getRowCount(r))
            } catch (e: CyclicMetadataException) {
                // ignore this rel; there will be other, non-cyclic ones
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
        return Util.first(v, 1e6) // if set is empty, estimate large
    }

    @Nullable
    fun getRowCount(rel: Union, mq: RelMetadataQuery): Double? {
        var rowCount = 0.0
        for (input in rel.getInputs()) {
            val partialRowCount: Double = mq.getRowCount(input) ?: return null
            rowCount += partialRowCount
        }
        if (!rel.all) {
            rowCount *= 0.5
        }
        return rowCount
    }

    @Nullable
    fun getRowCount(rel: Intersect, mq: RelMetadataQuery): Double? {
        var rowCount: Double? = null
        for (input in rel.getInputs()) {
            val partialRowCount: Double = mq.getRowCount(input)
            if (rowCount == null
                || partialRowCount != null && partialRowCount < rowCount
            ) {
                rowCount = partialRowCount
            }
        }
        return if (rowCount == null || !rel.all) {
            rowCount
        } else {
            rowCount * 2
        }
    }

    @Nullable
    fun getRowCount(rel: Minus, mq: RelMetadataQuery): Double? {
        var rowCount: Double? = null
        for (input in rel.getInputs()) {
            val partialRowCount: Double = mq.getRowCount(input)
            if (rowCount == null
                || partialRowCount != null && partialRowCount < rowCount
            ) {
                rowCount = partialRowCount
            }
        }
        return rowCount
    }

    fun getRowCount(rel: Filter, mq: RelMetadataQuery?): Double {
        return RelMdUtil.estimateFilteredRows(
            rel.getInput(), rel.getCondition(),
            mq
        )
    }

    fun getRowCount(rel: Calc, mq: RelMetadataQuery?): Double {
        return RelMdUtil.estimateFilteredRows(rel.getInput(), rel.getProgram(), mq)
    }

    @Nullable
    fun getRowCount(rel: Project, mq: RelMetadataQuery): Double {
        return mq.getRowCount(rel.getInput())
    }

    @Nullable
    fun getRowCount(rel: Sort, mq: RelMetadataQuery): Double? {
        var rowCount: Double = mq.getRowCount(rel.getInput()) ?: return null
        if (rel.offset is RexDynamicParam) {
            return rowCount
        }
        val offset = if (rel.offset == null) 0 else RexLiteral.intValue(rel.offset)
        rowCount = Math.max(rowCount - offset, 0.0)
        if (rel.fetch != null) {
            if (rel.fetch is RexDynamicParam) {
                return rowCount
            }
            val limit: Int = RexLiteral.intValue(rel.fetch)
            if (limit < rowCount) {
                return limit.toDouble()
            }
        }
        return rowCount
    }

    @Nullable
    fun getRowCount(rel: EnumerableLimit, mq: RelMetadataQuery): Double? {
        var rowCount: Double = mq.getRowCount(rel.getInput()) ?: return null
        if (rel.offset is RexDynamicParam) {
            return rowCount
        }
        val offset = if (rel.offset == null) 0 else RexLiteral.intValue(rel.offset)
        rowCount = Math.max(rowCount - offset, 0.0)
        if (rel.fetch != null) {
            if (rel.fetch is RexDynamicParam) {
                return rowCount
            }
            val limit: Int = RexLiteral.intValue(rel.fetch)
            if (limit < rowCount) {
                return limit.toDouble()
            }
        }
        return rowCount
    }

    // Covers Converter, Interpreter
    @Nullable
    fun getRowCount(rel: SingleRel, mq: RelMetadataQuery): Double {
        return mq.getRowCount(rel.getInput())
    }

    @Nullable
    fun getRowCount(rel: Join, mq: RelMetadataQuery): Double? {
        return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition())
    }

    fun getRowCount(rel: Aggregate, mq: RelMetadataQuery): Double {
        val groupKey: ImmutableBitSet = rel.getGroupSet()

        // rowCount is the cardinality of the group by columns
        var distinctRowCount: Double = mq.getDistinctRowCount(rel.getInput(), groupKey, null)
        if (distinctRowCount == null) {
            distinctRowCount = mq.getRowCount(rel.getInput()) / 10
        }

        // Grouping sets multiply
        distinctRowCount *= rel.getGroupSets().size()
        return distinctRowCount
    }

    fun getRowCount(rel: TableScan, mq: RelMetadataQuery?): Double {
        return rel.estimateRowCount(mq)
    }

    fun getRowCount(rel: Values, mq: RelMetadataQuery?): Double {
        return rel.estimateRowCount(mq)
    }

    @Nullable
    fun getRowCount(rel: Exchange, mq: RelMetadataQuery): Double {
        return mq.getRowCount(rel.getInput())
    }

    @Nullable
    fun getRowCount(rel: TableModify, mq: RelMetadataQuery): Double {
        return mq.getRowCount(rel.getInput())
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdRowCount(), BuiltInMetadata.RowCount.Handler::class.java
        )
    }
}
