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

import org.apache.calcite.adapter.enumerable.EnumerableCorrelate

/**
 * RelMdCollation supplies a default implementation of
 * [org.apache.calcite.rel.metadata.RelMetadataQuery.collations]
 * for the standard logical algebra.
 */
class RelMdCollation  //~ Constructors -----------------------------------------------------------
private constructor() : MetadataHandler<BuiltInMetadata.Collation?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.Collation.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.Collation.collations],
     * invoked using reflection, for any relational expression not
     * handled by a more specific method.
     *
     *
     * [org.apache.calcite.rel.core.Union],
     * [org.apache.calcite.rel.core.Intersect],
     * [org.apache.calcite.rel.core.Minus],
     * [org.apache.calcite.rel.core.Join],
     * [org.apache.calcite.rel.core.Correlate]
     * do not in general return sorted results
     * (but implementations using particular algorithms may).
     *
     * @param rel Relational expression
     * @return Relational expression's collations
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.collations
     */
    @Nullable
    fun collations(
        rel: RelNode?,
        mq: RelMetadataQuery?
    ): ImmutableList<RelCollation>? {
        return null
    }

    @Nullable
    fun collations(
        rel: Window,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return copyOf(window(mq, rel.getInput(), rel.groups))
    }

    @Nullable
    fun collations(
        rel: Match,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return copyOf(
            match(
                mq, rel.getInput(), rel.getRowType(), rel.getPattern(),
                rel.isStrictStart(), rel.isStrictEnd(),
                rel.getPatternDefinitions(), rel.getMeasures(), rel.getAfter(),
                rel.getSubsets(), rel.isAllRows(), rel.getPartitionKeys(),
                rel.getOrderKeys(), rel.getInterval()
            )
        )
    }

    @Nullable
    fun collations(
        rel: Filter,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return mq.collations(rel.getInput())
    }

    @Nullable
    fun collations(
        rel: TableModify,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return mq.collations(rel.getInput())
    }

    @Nullable
    fun collations(
        scan: TableScan,
        mq: RelMetadataQuery?
    ): ImmutableList<RelCollation> {
        return copyOf(table(scan.getTable()))
    }

    @Nullable
    fun collations(
        join: EnumerableMergeJoin,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        // In general a join is not sorted. But a merge join preserves the sort
        // order of the left and right sides.
        return copyOf(
            mergeJoin(
                mq, join.getLeft(), join.getRight(),
                join.analyzeCondition().leftKeys, join.analyzeCondition().rightKeys,
                join.getJoinType()
            )
        )
    }

    @Nullable
    fun collations(
        join: EnumerableHashJoin,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return copyOf(
            enumerableHashJoin(mq, join.getLeft(), join.getRight(), join.getJoinType())
        )
    }

    @Nullable
    fun collations(
        join: EnumerableNestedLoopJoin,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return copyOf(
            enumerableNestedLoopJoin(
                mq, join.getLeft(), join.getRight(),
                join.getJoinType()
            )
        )
    }

    @Nullable
    fun collations(
        mergeUnion: EnumerableMergeUnion,
        mq: RelMetadataQuery?
    ): ImmutableList<RelCollation>? {
        val collation: RelCollation = mergeUnion.getTraitSet().getCollation()
            ?: // should not happen
            return null
        // MergeUnion guarantees order, like a sort
        return copyOf(sort(collation))
    }

    @Nullable
    fun collations(
        join: EnumerableCorrelate,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return copyOf(
            enumerableCorrelate(
                mq, join.getLeft(), join.getRight(),
                join.getJoinType()
            )
        )
    }

    @Nullable
    fun collations(
        sort: Sort,
        mq: RelMetadataQuery?
    ): ImmutableList<RelCollation> {
        return copyOf(
            sort(sort.getCollation())
        )
    }

    @Nullable
    fun collations(
        sort: SortExchange,
        mq: RelMetadataQuery?
    ): ImmutableList<RelCollation> {
        return copyOf(
            sort(sort.getCollation())
        )
    }

    @Nullable
    fun collations(
        project: Project,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return copyOf(
            project(mq, project.getInput(), project.getProjects())
        )
    }

    @Nullable
    fun collations(
        calc: Calc,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return copyOf(calc(mq, calc.getInput(), calc.getProgram()))
    }

    @Nullable
    fun collations(
        values: Values,
        mq: RelMetadataQuery?
    ): ImmutableList<RelCollation> {
        return copyOf(
            values(mq, values.getRowType(), values.getTuples())
        )
    }

    @Nullable
    fun collations(
        rel: JdbcToEnumerableConverter,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return mq.collations(rel.getInput())
    }

    @Nullable
    fun collations(
        rel: HepRelVertex,
        mq: RelMetadataQuery
    ): ImmutableList<RelCollation> {
        return mq.collations(rel.getCurrentRel())
    }

    @Nullable
    fun collations(
        rel: RelSubset,
        mq: RelMetadataQuery?
    ): ImmutableList<RelCollation> {
        return copyOf(
            Objects.requireNonNull(
                rel.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE)
            )
        )
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdCollation(), BuiltInMetadata.Collation.Handler::class.java
        )

        private fun <E> copyOf(@Nullable values: Collection<E>?): @Nullable ImmutableList<E>? {
            return if (values == null) null else ImmutableList.copyOf(values)
        }
        // Helper methods
        /** Helper method to determine a
         * [org.apache.calcite.rel.core.TableScan]'s collation.  */
        @Nullable
        fun table(table: RelOptTable): List<RelCollation> {
            return table.getCollationList()
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Snapshot]'s collation.  */
        @Nullable
        fun snapshot(mq: RelMetadataQuery, input: RelNode?): List<RelCollation> {
            return mq.collations(input)
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Sort]'s collation.  */
        fun sort(collation: RelCollation?): List<RelCollation> {
            return ImmutableList.of(collation)
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Filter]'s collation.  */
        @Nullable
        fun filter(mq: RelMetadataQuery, input: RelNode?): List<RelCollation> {
            return mq.collations(input)
        }

        /** Helper method to determine a
         * limit's collation.  */
        @Nullable
        fun limit(mq: RelMetadataQuery, input: RelNode?): List<RelCollation> {
            return mq.collations(input)
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Calc]'s collation.  */
        @Nullable
        fun calc(
            mq: RelMetadataQuery, input: RelNode,
            program: RexProgram
        ): List<RelCollation> {
            val projects: List<RexNode> = program
                .getProjectList()
                .stream()
                .map(program::expandLocalRef)
                .collect(Collectors.toList())
            return project(mq, input, projects)
        }

        /** Helper method to determine a [Project]'s collation.  */
        @Nullable
        fun project(
            mq: RelMetadataQuery,
            input: RelNode, projects: List<RexNode?>
        ): List<RelCollation> {
            val collations: NavigableSet<RelCollation> = TreeSet()
            val inputCollations: List<RelCollation> = mq.collations(input)
            if (inputCollations == null || inputCollations.isEmpty()) {
                return ImmutableList.of()
            }
            val targets: Multimap<Integer, Integer> = LinkedListMultimap.create()
            val targetsWithMonotonicity: Map<Integer, SqlMonotonicity> = HashMap()
            for (project in Ord.< RexNode > zip < RexNode ? > projects) {
                if (project.e is RexInputRef) {
                    targets.put((project.e as RexInputRef).getIndex(), project.i)
                } else if (project.e is RexCall) {
                    val call: RexCall = project.e as RexCall
                    val binding: RexCallBinding =
                        RexCallBinding.create(input.getCluster().getTypeFactory(), call, inputCollations)
                    targetsWithMonotonicity.put(project.i, call.getOperator().getMonotonicity(binding))
                }
            }
            val fieldCollations: List<RelFieldCollation> = ArrayList()
            loop@ for (ic in inputCollations) {
                if (ic.getFieldCollations().isEmpty()) {
                    continue
                }
                fieldCollations.clear()
                for (ifc in ic.getFieldCollations()) {
                    val integers: Collection<Integer> = targets.get(ifc.getFieldIndex())
                    if (integers.isEmpty()) {
                        continue@loop   // cannot do this collation
                    }
                    fieldCollations.add(ifc.withFieldIndex(integers.iterator().next()))
                }
                assert(!fieldCollations.isEmpty())
                collations.add(RelCollations.of(fieldCollations))
            }
            val fieldCollationsForRexCalls: List<RelFieldCollation> = ArrayList()
            for (entry in targetsWithMonotonicity.entrySet()) {
                val value: SqlMonotonicity = entry.getValue()
                when (value) {
                    NOT_MONOTONIC, CONSTANT -> {}
                    else -> fieldCollationsForRexCalls.add(
                        RelFieldCollation(
                            entry.getKey(),
                            RelFieldCollation.Direction.of(value)
                        )
                    )
                }
            }
            if (!fieldCollationsForRexCalls.isEmpty()) {
                collations.add(RelCollations.of(fieldCollationsForRexCalls))
            }
            return copyOf(collations)
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Window]'s collation.
         *
         *
         * A Window projects the fields of its input first, followed by the output
         * from each of its windows. Assuming (quite reasonably) that the
         * implementation does not re-order its input rows, then any collations of its
         * input are preserved.  */
        @Nullable
        fun window(
            mq: RelMetadataQuery, input: RelNode?,
            groups: ImmutableList<Window.Group?>?
        ): List<RelCollation> {
            return mq.collations(input)
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Match]'s collation.  */
        @Nullable
        fun match(
            mq: RelMetadataQuery, input: RelNode?,
            rowType: RelDataType?, pattern: RexNode?,
            strictStart: Boolean, strictEnd: Boolean,
            patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
            after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?,
            allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
            @Nullable interval: RexNode?
        ): List<RelCollation> {
            return mq.collations(input)
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Values]'s collation.
         *
         *
         * We actually under-report the collations. A Values with 0 or 1 rows - an
         * edge case, but legitimate and very common - is ordered by every permutation
         * of every subset of the columns.
         *
         *
         * So, our algorithm aims to:
         *  * produce at most N collations (where N is the number of columns);
         *  * make each collation as long as possible;
         *  * do not repeat combinations already emitted -
         * if we've emitted `(a, b)` do not later emit `(b, a)`;
         *  * probe the actual values and make sure that each collation is
         * consistent with the data
         *
         *
         *
         * So, for an empty Values with 4 columns, we would emit
         * `(a, b, c, d), (b, c, d), (c, d), (d)`.  */
        fun values(
            mq: RelMetadataQuery?,
            rowType: RelDataType, tuples: ImmutableList<ImmutableList<RexLiteral?>?>?
        ): List<RelCollation> {
            Util.discard(mq) // for future use
            val list: List<RelCollation> = ArrayList()
            val n: Int = rowType.getFieldCount()
            val pairs: List<Pair<RelFieldCollation, Ordering<List<RexLiteral>>>> = ArrayList()
            outer@ for (i in 0 until n) {
                pairs.clear()
                for (j in i until n) {
                    val fieldCollation = RelFieldCollation(j)
                    val comparator: Ordering<List<RexLiteral?>?> = comparator(fieldCollation)
                    var ordering: Ordering<List<RexLiteral?>?>
                    ordering = if (pairs.isEmpty()) {
                        comparator
                    } else {
                        Util.last(pairs).right.compound(comparator)
                    }
                    pairs.add(Pair.of(fieldCollation, ordering))
                    if (!ordering.isOrdered(tuples)) {
                        if (j == i) {
                            continue@outer
                        }
                        pairs.remove(pairs.size() - 1)
                    }
                }
                if (!pairs.isEmpty()) {
                    list.add(RelCollations.of(Pair.left(pairs)))
                }
            }
            return list
        }

        fun comparator(
            fieldCollation: RelFieldCollation
        ): Ordering<List<RexLiteral>> {
            val nullComparison: Int = fieldCollation.nullDirection.nullComparison
            val x: Int = fieldCollation.getFieldIndex()
            return when (fieldCollation.direction) {
                ASCENDING -> object : Ordering<List<RexLiteral?>?>() {
                    @Override
                    fun compare(o1: List<RexLiteral>, o2: List<RexLiteral>): Int {
                        val c1: Comparable = o1[x].getValueAs(Comparable::class.java)
                        val c2: Comparable = o2[x].getValueAs(Comparable::class.java)
                        return RelFieldCollation.compare(c1, c2, nullComparison)
                    }
                }
                else -> object : Ordering<List<RexLiteral?>?>() {
                    @Override
                    fun compare(o1: List<RexLiteral>, o2: List<RexLiteral>): Int {
                        val c1: Comparable = o1[x].getValueAs(Comparable::class.java)
                        val c2: Comparable = o2[x].getValueAs(Comparable::class.java)
                        return RelFieldCollation.compare(c2, c1, -nullComparison)
                    }
                }
            }
        }

        /** Helper method to determine a [Join]'s collation assuming that it
         * uses a merge-join algorithm.
         *
         *
         * If the inputs are sorted on other keys *in addition to* the join
         * key, the result preserves those collations too.
         */
        @Deprecated // to be removed before 2.0
        @Nullable
        @Deprecated("Use {@link #mergeJoin(RelMetadataQuery, RelNode, RelNode, ImmutableIntList, ImmutableIntList, JoinRelType)} ")
        fun mergeJoin(
            mq: RelMetadataQuery,
            left: RelNode,
            right: RelNode?,
            leftKeys: ImmutableIntList?,
            rightKeys: ImmutableIntList?
        ): List<RelCollation>? {
            return mergeJoin(mq, left, right, leftKeys, rightKeys, JoinRelType.INNER)
        }

        /** Helper method to determine a [Join]'s collation assuming that it
         * uses a merge-join algorithm.
         *
         *
         * If the inputs are sorted on other keys *in addition to* the join
         * key, the result preserves those collations too.  */
        @Nullable
        fun mergeJoin(
            mq: RelMetadataQuery,
            left: RelNode, right: RelNode?,
            leftKeys: ImmutableIntList?, rightKeys: ImmutableIntList?, joinType: JoinRelType
        ): List<RelCollation>? {
            assert(EnumerableMergeJoin.isMergeJoinSupported(joinType)) { "EnumerableMergeJoin unsupported for join type $joinType" }
            val leftCollations: ImmutableList<RelCollation> = mq.collations(left)
            if (!joinType.projectsRight()) {
                return leftCollations
            }
            if (leftCollations == null) {
                return null
            }
            val rightCollations: ImmutableList<RelCollation> = mq.collations(right) ?: return leftCollations
            val builder: ImmutableList.Builder<RelCollation> = ImmutableList.builder()
            builder.addAll(leftCollations)
            val leftFieldCount: Int = left.getRowType().getFieldCount()
            for (collation in rightCollations) {
                builder.add(RelCollations.shift(collation, leftFieldCount))
            }
            return builder.build()
        }

        /**
         * Returns the collation of [EnumerableHashJoin] based on its inputs and the join type.
         */
        @Nullable
        fun enumerableHashJoin(
            mq: RelMetadataQuery,
            left: RelNode?, right: RelNode?, joinType: JoinRelType
        ): List<RelCollation>? {
            return if (joinType === JoinRelType.SEMI) {
                enumerableSemiJoin(mq, left, right)
            } else {
                enumerableJoin0(mq, left, right, joinType)
            }
        }

        /**
         * Returns the collation of [EnumerableNestedLoopJoin]
         * based on its inputs and the join type.
         */
        @Nullable
        fun enumerableNestedLoopJoin(
            mq: RelMetadataQuery,
            left: RelNode?, right: RelNode?, joinType: JoinRelType
        ): List<RelCollation>? {
            return enumerableJoin0(mq, left, right, joinType)
        }

        @Nullable
        fun enumerableCorrelate(
            mq: RelMetadataQuery,
            left: RelNode?, right: RelNode?, joinType: JoinRelType?
        ): List<RelCollation> {
            // The current implementation always preserve the sort order of the left input
            return mq.collations(left)
        }

        @Nullable
        fun enumerableSemiJoin(
            mq: RelMetadataQuery,
            left: RelNode?, right: RelNode?
        ): List<RelCollation> {
            // The current implementation always preserve the sort order of the left input
            return mq.collations(left)
        }

        @SuppressWarnings("unused")
        @Nullable
        fun enumerableBatchNestedLoopJoin(
            mq: RelMetadataQuery,
            left: RelNode?, right: RelNode?, joinType: JoinRelType?
        ): List<RelCollation> {
            // The current implementation always preserve the sort order of the left input
            return mq.collations(left)
        }

        @SuppressWarnings("unused")
        @Nullable
        private fun enumerableJoin0(
            mq: RelMetadataQuery,
            left: RelNode?, right: RelNode?, joinType: JoinRelType
        ): List<RelCollation>? {
            // The current implementation can preserve the sort order of the left input if one of the
            // following conditions hold:
            // (i) join type is INNER or LEFT;
            // (ii) RelCollation always orders nulls last.
            val leftCollations: ImmutableList<RelCollation> = mq.collations(left) ?: return null
            when (joinType) {
                SEMI, ANTI, INNER, LEFT -> return leftCollations
                RIGHT, FULL -> {
                    for (collation in leftCollations) {
                        for (field in collation.getFieldCollations()) {
                            if (!(RelFieldCollation.NullDirection.LAST === field.nullDirection)) {
                                return null
                            }
                        }
                    }
                    return leftCollations
                }
                else -> {}
            }
            return null
        }
    }
}
