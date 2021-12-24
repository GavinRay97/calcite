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

import org.apache.calcite.linq4j.Linq4j

/**
 * RelMdUniqueKeys supplies a default implementation of
 * [RelMetadataQuery.getUniqueKeys] for the standard logical algebra.
 */
class RelMdUniqueKeys  //~ Constructors -----------------------------------------------------------
private constructor() : MetadataHandler<BuiltInMetadata.UniqueKeys?> {
    //~ Methods ----------------------------------------------------------------
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Override get() = BuiltInMetadata.UniqueKeys.DEF

    @Nullable
    fun getUniqueKeys(
        rel: Filter, mq: RelMetadataQuery,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        return mq.getUniqueKeys(rel.getInput(), ignoreNulls)
    }

    @Nullable
    fun getUniqueKeys(
        rel: Sort, mq: RelMetadataQuery,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        return mq.getUniqueKeys(rel.getInput(), ignoreNulls)
    }

    @Nullable
    fun getUniqueKeys(
        rel: Correlate, mq: RelMetadataQuery,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        return mq.getUniqueKeys(rel.getLeft(), ignoreNulls)
    }

    @Nullable
    fun getUniqueKeys(
        rel: TableModify, mq: RelMetadataQuery,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        return mq.getUniqueKeys(rel.getInput(), ignoreNulls)
    }

    fun getUniqueKeys(
        rel: Project, mq: RelMetadataQuery,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        return getProjectUniqueKeys(rel, mq, ignoreNulls, rel.getProjects())
    }

    @Nullable
    fun getUniqueKeys(
        rel: Calc, mq: RelMetadataQuery,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        val program: RexProgram = rel.getProgram()
        return getProjectUniqueKeys(
            rel, mq, ignoreNulls,
            Util.transform(program.getProjectList(), program::expandLocalRef)
        )
    }

    @Nullable
    fun getUniqueKeys(
        rel: Join, mq: RelMetadataQuery,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        if (!rel.getJoinType().projectsRight()) {
            // only return the unique keys from the LHS since a semijoin only
            // returns the LHS
            return mq.getUniqueKeys(rel.getLeft(), ignoreNulls)
        }
        val left: RelNode = rel.getLeft()
        val right: RelNode = rel.getRight()

        // first add the different combinations of concatenated unique keys
        // from the left and the right, adjusting the right hand side keys to
        // reflect the addition of the left hand side
        //
        // NOTE zfong 12/18/06 - If the number of tables in a join is large,
        // the number of combinations of unique key sets will explode.  If
        // that is undesirable, use RelMetadataQuery.areColumnsUnique() as
        // an alternative way of getting unique key information.
        val retSet: Set<ImmutableBitSet> = HashSet()
        val leftSet: Set<ImmutableBitSet> = mq.getUniqueKeys(left, ignoreNulls)
        var rightSet: Set<ImmutableBitSet?>? = null
        val tmpRightSet: Set<ImmutableBitSet> = mq.getUniqueKeys(right, ignoreNulls)
        val nFieldsOnLeft: Int = left.getRowType().getFieldCount()
        if (tmpRightSet != null) {
            rightSet = HashSet()
            for (colMask in tmpRightSet) {
                val tmpMask: ImmutableBitSet.Builder = ImmutableBitSet.builder()
                for (bit in colMask) {
                    tmpMask.set(bit + nFieldsOnLeft)
                }
                rightSet.add(tmpMask.build())
            }
            if (leftSet != null) {
                for (colMaskRight in rightSet) {
                    for (colMaskLeft in leftSet) {
                        retSet.add(colMaskLeft.union(colMaskRight))
                    }
                }
            }
        }

        // locate the columns that participate in equijoins
        val joinInfo: JoinInfo = rel.analyzeCondition()

        // determine if either or both the LHS and RHS are unique on the
        // equijoin columns
        val leftUnique: Boolean = mq.areColumnsUnique(left, joinInfo.leftSet(), ignoreNulls)
        val rightUnique: Boolean = mq.areColumnsUnique(right, joinInfo.rightSet(), ignoreNulls)

        // if the right hand side is unique on its equijoin columns, then we can
        // add the unique keys from left if the left hand side is not null
        // generating
        if (rightUnique != null
            && rightUnique
            && leftSet != null
            && !rel.getJoinType().generatesNullsOnLeft()
        ) {
            retSet.addAll(leftSet)
        }

        // same as above except left and right are reversed
        if (leftUnique != null
            && leftUnique
            && rightSet != null
            && !rel.getJoinType().generatesNullsOnRight()
        ) {
            retSet.addAll(rightSet)
        }
        return retSet
    }

    fun getUniqueKeys(
        rel: Aggregate, mq: RelMetadataQuery?,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        return if (Aggregate.isSimple(rel) || ignoreNulls) {
            // group by keys form a unique key
            ImmutableSet.of(rel.getGroupSet())
        } else {
            // If the aggregate has grouping sets, all group by keys might be null which means group by
            // keys do not form a unique key.
            ImmutableSet.of()
        }
    }

    fun getUniqueKeys(
        rel: Union, mq: RelMetadataQuery?,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        return if (!rel.all) {
            ImmutableSet.of(
                ImmutableBitSet.range(rel.getRowType().getFieldCount())
            )
        } else ImmutableSet.of()
    }

    /**
     * Any unique key of any input of Intersect is an unique key of the Intersect.
     */
    fun getUniqueKeys(
        rel: Intersect,
        mq: RelMetadataQuery, ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        val keys: ImmutableSet.Builder<ImmutableBitSet> = Builder()
        for (input in rel.getInputs()) {
            val uniqueKeys: Set<ImmutableBitSet> = mq.getUniqueKeys(input, ignoreNulls)
            if (uniqueKeys != null) {
                keys.addAll(uniqueKeys)
            }
        }
        val uniqueKeys: ImmutableSet<ImmutableBitSet> = keys.build()
        if (!uniqueKeys.isEmpty()) {
            return uniqueKeys
        }
        return if (!rel.all) {
            ImmutableSet.of(
                ImmutableBitSet.range(rel.getRowType().getFieldCount())
            )
        } else ImmutableSet.of()
    }

    /**
     * The unique keys of Minus are precisely the unique keys of its first input.
     */
    fun getUniqueKeys(
        rel: Minus,
        mq: RelMetadataQuery, ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        val uniqueKeys: Set<ImmutableBitSet> = mq.getUniqueKeys(rel.getInput(0), ignoreNulls)
        if (uniqueKeys != null) {
            return uniqueKeys
        }
        return if (!rel.all) {
            ImmutableSet.of(
                ImmutableBitSet.range(rel.getRowType().getFieldCount())
            )
        } else ImmutableSet.of()
    }

    @Nullable
    fun getUniqueKeys(
        rel: TableScan, mq: RelMetadataQuery?,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet>? {
        val keys: List<ImmutableBitSet> = rel.getTable().getKeys() ?: return null
        for (key in keys) {
            assert(rel.getTable().isKey(key))
        }
        return ImmutableSet.copyOf(keys)
    }

    // Catch-all rule when none of the others apply.
    @Nullable
    fun getUniqueKeys(
        rel: RelNode?, mq: RelMetadataQuery?,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet>? {
        // no information available
        return null
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdUniqueKeys(), BuiltInMetadata.UniqueKeys.Handler::class.java
        )

        private fun getProjectUniqueKeys(
            rel: SingleRel, mq: RelMetadataQuery,
            ignoreNulls: Boolean, projExprs: List<RexNode>
        ): Set<ImmutableBitSet> {
            // LogicalProject maps a set of rows to a different set;
            // Without knowledge of the mapping function(whether it
            // preserves uniqueness), it is only safe to derive uniqueness
            // info from the child of a project when the mapping is f(a) => a.
            //
            // Further more, the unique bitset coming from the child needs
            // to be mapped to match the output of the project.

            // Single input can be mapped to multiple outputs
            val inToOutPosBuilder: ImmutableMultimap.Builder<Integer, Integer> = ImmutableMultimap.builder()
            val mappedInColumnsBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()

            // Build an input to output position map.
            for (i in 0 until projExprs.size()) {
                val projExpr: RexNode = projExprs[i]
                if (projExpr is RexInputRef) {
                    val inputIndex: Int = (projExpr as RexInputRef).getIndex()
                    inToOutPosBuilder.put(inputIndex, i)
                    mappedInColumnsBuilder.set(inputIndex)
                }
            }
            val inColumnsUsed: ImmutableBitSet = mappedInColumnsBuilder.build()
            if (inColumnsUsed.isEmpty()) {
                // if there's no RexInputRef in the projected expressions
                // return empty set.
                return ImmutableSet.of()
            }
            val childUniqueKeySet: Set<ImmutableBitSet> = mq.getUniqueKeys(rel.getInput(), ignoreNulls)
                ?: return ImmutableSet.of()
            val mapInToOutPos: Map<Integer, ImmutableBitSet> =
                Maps.transformValues(inToOutPosBuilder.build().asMap(), ImmutableBitSet::of)
            val resultBuilder: ImmutableSet.Builder<ImmutableBitSet> = ImmutableSet.builder()
            // Now add to the projUniqueKeySet the child keys that are fully
            // projected.
            for (colMask in childUniqueKeySet) {
                if (!inColumnsUsed.contains(colMask)) {
                    // colMask contains a column that is not projected as RexInput => the key is not unique
                    continue
                }
                // colMask is mapped to output project, however, the column can be mapped more than once:
                // select id, id, id, unique2, unique2
                // the resulting unique keys would be {{0},{3}}, {{0},{4}}, {{0},{1},{4}}, ...
                val product: Iterable<List<ImmutableBitSet>> = Linq4j.product(
                    Util.transform(
                        colMask
                    ) { `in` ->
                        Util.filter(
                            requireNonNull(
                                mapInToOutPos[`in`]
                            ) { "no entry for column $`in` in mapInToOutPos: $mapInToOutPos" }
                                .powerSet()) { bs -> !bs.isEmpty() }
                    })
                resultBuilder.addAll(Util.transform(product, ImmutableBitSet::union))
            }
            return resultBuilder.build()
        }
    }
}
