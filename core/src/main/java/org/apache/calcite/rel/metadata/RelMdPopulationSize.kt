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

import org.apache.calcite.rel.RelNode

/**
 * RelMdPopulationSize supplies a default implementation of
 * [RelMetadataQuery.getPopulationSize] for the standard logical algebra.
 */
class RelMdPopulationSize  //~ Constructors -----------------------------------------------------------
private constructor() : MetadataHandler<BuiltInMetadata.PopulationSize?> {
    //~ Methods ----------------------------------------------------------------
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Override get() = BuiltInMetadata.PopulationSize.DEF

    @Nullable
    fun getPopulationSize(
        rel: Filter, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?
    ): Double? {
        return mq.getPopulationSize(rel.getInput(), groupKey)
    }

    @Nullable
    fun getPopulationSize(
        rel: Sort, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?
    ): Double? {
        return mq.getPopulationSize(rel.getInput(), groupKey)
    }

    @Nullable
    fun getPopulationSize(
        rel: Exchange, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?
    ): Double? {
        return mq.getPopulationSize(rel.getInput(), groupKey)
    }

    @Nullable
    fun getPopulationSize(
        rel: TableModify, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?
    ): Double? {
        return mq.getPopulationSize(rel.getInput(), groupKey)
    }

    @Nullable
    fun getPopulationSize(
        rel: Union, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?
    ): Double? {
        var population = 0.0
        for (input in rel.getInputs()) {
            val subPop: Double = mq.getPopulationSize(input, groupKey) ?: return null
            population += subPop
        }
        return population
    }

    @Nullable
    fun getPopulationSize(
        rel: Join, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet
    ): Double? {
        return RelMdUtil.getJoinPopulationSize(mq, rel, groupKey)
    }

    @Nullable
    fun getPopulationSize(
        rel: Aggregate, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet
    ): Double? {
        val childKey: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        RelMdUtil.setAggChildKeys(groupKey, rel, childKey)
        return mq.getPopulationSize(rel.getInput(), childKey.build())
    }

    fun getPopulationSize(
        rel: Values, mq: RelMetadataQuery?,
        groupKey: ImmutableBitSet?
    ): Double {
        // assume half the rows are duplicates
        return rel.estimateRowCount(mq) / 2
    }

    @Nullable
    fun getPopulationSize(
        rel: Project, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet
    ): Double? {
        // try to remove const columns from the group keys, as they do not
        // affect the population size
        val nonConstCols: ImmutableBitSet = RexUtil.getNonConstColumns(groupKey, rel.getProjects())
        if (nonConstCols.cardinality() === 0) {
            // all columns are constants, the population size should be 1
            return 1.0
        }
        if (nonConstCols.cardinality() < groupKey.cardinality()) {
            // some const columns can be removed, call the method recursively
            // with the trimmed columns
            return getPopulationSize(rel, mq, nonConstCols)
        }
        val baseCols: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val projCols: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val projExprs: List<RexNode> = rel.getProjects()
        RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols)
        var population: Double? = mq.getPopulationSize(rel.getInput(), baseCols.build()) ?: return null

        // No further computation required if the projection expressions are
        // all column references
        if (projCols.cardinality() === 0) {
            return population
        }
        for (bit in projCols.build()) {
            val subRowCount: Double = RelMdUtil.cardOfProjExpr(mq, rel, projExprs[bit]) ?: return null
            population *= subRowCount
        }

        // REVIEW zfong 6/22/06 - Broadbase did not have the call to
        // numDistinctVals.  This is needed; otherwise, population can be
        // larger than the number of rows in the RelNode.
        return RelMdUtil.numDistinctVals(population, mq.getRowCount(rel))
    }

    /** Catch-all implementation for
     * [BuiltInMetadata.PopulationSize.getPopulationSize],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getPopulationSize
     */
    @Nullable
    fun getPopulationSize(
        rel: RelNode?, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet?
    ): Double? {
        // if the keys are unique, return the row count; otherwise, we have
        // no further information on which to return any legitimate value

        // REVIEW zfong 4/11/06 - Broadbase code returns the product of each
        // unique key, which would result in the population being larger
        // than the total rows in the relnode
        val uniq: Boolean = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey)
        return if (uniq) {
            mq.getRowCount(rel)
        } else null
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdPopulationSize(), BuiltInMetadata.PopulationSize.Handler::class.java
        )
    }
}
