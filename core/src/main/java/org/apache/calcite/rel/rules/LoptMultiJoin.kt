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
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.RelOptUtil

/**
 * Utility class that keeps track of the join factors that
 * make up a [MultiJoin].
 */
class LoptMultiJoin(multiJoin: MultiJoin) {
    //~ Instance fields --------------------------------------------------------
    /** The MultiJoin being optimized.  */
    var multiJoin: MultiJoin

    /**
     * Join filters associated with the MultiJoin, decomposed into a list.
     * Excludes left/right outer join filters.
     */
    private val joinFilters: List<RexNode>

    /**
     * All join filters associated with the MultiJoin, decomposed into a
     * list. Includes left/right outer join filters.
     */
    private val allJoinFilters: List<RexNode>
    /**
     * Returns the number of factors in this multi-join.
     */
    /** Number of factors into the MultiJoin.  */
    val numJoinFactors: Int
    /**
     * Returns the total number of fields in the multi-join.
     */
    /** Total number of fields in the MultiJoin.  */
    val numTotalFields: Int

    /** Original inputs into the MultiJoin.  */
    private val joinFactors: ImmutableList<RelNode>

    /**
     * If a join factor is null-generating in a left or right outer join,
     * joinTypes indicates the join type corresponding to the factor. Otherwise,
     * it is set to INNER.
     */
    private val joinTypes: ImmutableList<JoinRelType>

    /**
     * If a join factor is null-generating in a left or right outer join, the
     * bitmap contains the non-null generating factors that the null-generating
     * factor is dependent upon.
     */
    private val outerJoinFactors: Array<ImmutableBitSet?>

    /**
     * Bitmap corresponding to the fields projected from each join factor, after
     * row scan processing has completed. This excludes fields referenced in
     * join conditions, unless the field appears in the final projection list.
     */
    private val projFields: List<ImmutableBitSet>

    /**
     * Map containing reference counts of the fields referenced in join
     * conditions for each join factor. If a field is only required for a
     * semijoin, then it is removed from the reference count. (Hence the need
     * for reference counts instead of simply a bitmap.) The map is indexed by
     * the factor number.
     */
    private val joinFieldRefCountsMap: Map<Integer, IntArray>

    /**
     * For each join filter, associates a bitmap indicating all factors
     * referenced by the filter.
     */
    private val factorsRefByJoinFilter: Map<RexNode, ImmutableBitSet> = HashMap()

    /**
     * For each join filter, associates a bitmap indicating all fields
     * referenced by the filter.
     */
    private val fieldsRefByJoinFilter: Map<RexNode, ImmutableBitSet> = HashMap()

    /**
     * Starting RexInputRef index corresponding to each join factor.
     */
    var joinStart: IntArray

    /**
     * Number of fields in each join factor.
     */
    var nFieldsInJoinFactor: IntArray

    /**
     * Bitmap indicating which factors each factor references in join filters
     * that correspond to comparisons.
     */
    var factorsRefByFactor: @MonotonicNonNull Array<ImmutableBitSet?>?
    /**
     * Returns weights of the different factors relative to one another.
     */
    /**
     * Weights of each factor combination.
     */
    var factorWeights: @MonotonicNonNull Array<IntArray>?

    /**
     * Type factory.
     */
    val factory: RelDataTypeFactory

    /**
     * Indicates for each factor whether its join can be removed because it is
     * the dimension table in a semijoin. If it can be, the entry indicates the
     * factor id of the fact table (corresponding to the dimension table) in the
     * semijoin that allows the factor to be removed. If the factor cannot be
     * removed, the entry corresponding to the factor is null.
     */
    @Nullable
    var joinRemovalFactors: Array<Integer?>

    /**
     * The semijoins that allow the join of a dimension table to be removed.
     */
    var joinRemovalSemiJoins: Array<LogicalJoin?>

    /**
     * Set of null-generating factors whose corresponding outer join can be
     * removed from the query plan.
     */
    var removableOuterJoinFactors: Set<Integer>

    /**
     * Map consisting of all pairs of self-joins where the self-join can be
     * removed because the join between the identical factors is an equality
     * join on the same set of unique keys. The map is keyed by either factor in
     * the self join.
     */
    var removableSelfJoinPairs: Map<Integer, RemovableSelfJoin>

    //~ Constructors -----------------------------------------------------------
    init {
        this.multiJoin = multiJoin
        joinFactors = ImmutableList.copyOf(multiJoin.getInputs())
        numJoinFactors = joinFactors.size()
        projFields = multiJoin.getProjFields()
        joinFieldRefCountsMap = multiJoin.getCopyJoinFieldRefCountsMap()
        joinFilters = Lists.newArrayList(RelOptUtil.conjunctions(multiJoin.getJoinFilter()))
        allJoinFilters = ArrayList(joinFilters)
        val outerJoinFilters: List<RexNode> = multiJoin.getOuterJoinConditions()
        for (i in 0 until numJoinFactors) {
            allJoinFilters.addAll(RelOptUtil.conjunctions(outerJoinFilters[i]))
        }
        var start = 0
        numTotalFields = multiJoin.getRowType().getFieldCount()
        joinStart = IntArray(numJoinFactors)
        nFieldsInJoinFactor = IntArray(numJoinFactors)
        for (i in 0 until numJoinFactors) {
            joinStart[i] = start
            nFieldsInJoinFactor[i] = joinFactors.get(i).getRowType().getFieldCount()
            start += nFieldsInJoinFactor[i]
        }

        // Extract outer join information from the join factors, including the type
        // of outer join and the factors that a null-generating factor is dependent
        // upon.
        joinTypes = ImmutableList.copyOf(multiJoin.getJoinTypes())
        val outerJoinConds: List<RexNode> = this.multiJoin.getOuterJoinConditions()
        outerJoinFactors = arrayOfNulls<ImmutableBitSet>(numJoinFactors)
        for (i in 0 until numJoinFactors) {
            val outerJoinCond: RexNode = outerJoinConds[i]
            if (outerJoinCond != null) {
                // set a bitmap containing the factors referenced in the
                // ON condition of the outer join; mask off the factor
                // corresponding to the factor itself
                var dependentFactors: ImmutableBitSet = getJoinFilterFactorBitmap(outerJoinCond, false)
                dependentFactors = dependentFactors.clear(i)
                outerJoinFactors[i] = dependentFactors
            }
        }

        // determine which join factors each join filter references
        setJoinFilterRefs()
        factory = multiJoin.getCluster().getTypeFactory()
        joinRemovalFactors = arrayOfNulls<Integer>(numJoinFactors)
        joinRemovalSemiJoins = arrayOfNulls<LogicalJoin>(numJoinFactors)
        removableOuterJoinFactors = HashSet()
        removableSelfJoinPairs = HashMap()
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the MultiJoin corresponding to this multi-join.
     */
    val multiJoinRel: MultiJoin
        get() = multiJoin

    /**
     * Returns the factor corresponding to the given factor index.
     *
     * @param factIdx Factor to be returned
     */
    fun getJoinFactor(factIdx: Int): RelNode {
        return joinFactors.get(factIdx)
    }

    /**
     * Returns the number of fields in a given factor.
     *
     * @param factIdx Desired factor
     */
    fun getNumFieldsInJoinFactor(factIdx: Int): Int {
        return nFieldsInJoinFactor[factIdx]
    }

    /**
     * Returns all non-outer join filters in this multi-join.
     */
    fun getJoinFilters(): List<RexNode> {
        return joinFilters
    }

    /**
     * Returns a bitmap corresponding to the factors referenced within
     * the specified join filter.
     *
     * @param joinFilter Filter for which information will be returned
     */
    fun getFactorsRefByJoinFilter(joinFilter: RexNode): ImmutableBitSet {
        return requireNonNull(
            factorsRefByJoinFilter[joinFilter]
        ) { "joinFilter is not found in factorsRefByJoinFilter: $joinFilter" }
    }

    /**
     * Returns an array of fields contained within the multi-join.
     */
    val multiJoinFields: List<Any>
        get() = multiJoin.getRowType().getFieldList()

    /**
     * Returns a bitmap corresponding to the fields referenced by a join filter.
     *
     * @param joinFilter the filter for which information will be returned
     */
    fun getFieldsRefByJoinFilter(joinFilter: RexNode): ImmutableBitSet {
        return requireNonNull(
            fieldsRefByJoinFilter[joinFilter]
        ) { "joinFilter is not found in fieldsRefByJoinFilter: $joinFilter" }
    }

    /**
     * Returns a bitmap corresponding to the factors referenced by the specified
     * factor in the various join filters that correspond to comparisons.
     *
     * @param factIdx Factor for which information will be returned
     */
    fun getFactorsRefByFactor(factIdx: Int): ImmutableBitSet {
        return requireNonNull(factorsRefByFactor, "factorsRefByFactor").get(factIdx)
    }

    /**
     * Returns the starting offset within the multi-join for the specified factor.
     *
     * @param factIdx Factor for which information will be returned
     */
    fun getJoinStart(factIdx: Int): Int {
        return joinStart[factIdx]
    }

    /**
     * Returns whether the factor corresponds to a null-generating factor
     * in a left or right outer join.
     *
     * @param factIdx Factor for which information will be returned
     */
    fun isNullGenerating(factIdx: Int): Boolean {
        return joinTypes.get(factIdx).isOuterJoin()
    }

    /**
     * Returns a bitmap containing the factors that a null-generating factor is
     * dependent upon, if the factor is null-generating in a left or right outer
     * join; otherwise null is returned.
     *
     * @param factIdx Factor for which information will be returned
     */
    fun getOuterJoinFactors(factIdx: Int): ImmutableBitSet? {
        return outerJoinFactors[factIdx]
    }

    /**
     * Returns outer join conditions associated with the specified null-generating
     * factor.
     *
     * @param factIdx Factor for which information will be returned
     */
    @Nullable
    fun getOuterJoinCond(factIdx: Int): RexNode {
        return multiJoin.getOuterJoinConditions().get(factIdx)
    }

    /**
     * Returns a bitmap containing the fields that are projected from a factor.
     *
     * @param factIdx Factor for which information will be returned
     */
    @Nullable
    fun getProjFields(factIdx: Int): ImmutableBitSet {
        return projFields[factIdx]
    }

    /**
     * Returns the join field reference counts for a factor.
     *
     * @param factIdx Factor for which information will be returned
     */
    fun getJoinFieldRefCounts(factIdx: Int): IntArray {
        return requireNonNull(
            joinFieldRefCountsMap[factIdx]
        ) { "no entry in joinFieldRefCountsMap found for $factIdx" }
    }

    /**
     * Returns the factor id of the fact table corresponding to a dimension
     * table in a semi-join, in the case where the join with the dimension table
     * can be removed.
     *
     * @param dimIdx Dimension factor for which information will be returned
     */
    @Nullable
    fun getJoinRemovalFactor(dimIdx: Int): Integer? {
        return joinRemovalFactors[dimIdx]
    }

    /**
     * Returns the semi-join that allows the join of a dimension table to be
     * removed.
     *
     * @param dimIdx Dimension factor for which information will be returned
     */
    fun getJoinRemovalSemiJoin(dimIdx: Int): LogicalJoin? {
        return joinRemovalSemiJoins[dimIdx]
    }

    /**
     * Indicates that a dimension factor's join can be removed because of a
     * semijoin with a fact table.
     *
     * @param dimIdx Dimension factor
     * @param factIdx Fact factor
     */
    fun setJoinRemovalFactor(dimIdx: Int, factIdx: Int) {
        joinRemovalFactors[dimIdx] = factIdx
    }

    /**
     * Indicates the semi-join that allows the join of a dimension table to be
     * removed.
     *
     * @param dimIdx Dimension factor
     * @param semiJoin the semijoin
     */
    fun setJoinRemovalSemiJoin(dimIdx: Int, semiJoin: LogicalJoin?) {
        joinRemovalSemiJoins[dimIdx] = semiJoin
    }

    /**
     * Returns a bitmap representing the factors referenced in a join filter.
     *
     * @param joinFilter the join filter
     * @param setFields if true, add the fields referenced by the join filter
     * into a map
     *
     * @return the bitmap containing the factor references
     */
    @RequiresNonNull(["joinStart", "nFieldsInJoinFactor"])
    fun getJoinFilterFactorBitmap(
        joinFilter: RexNode?,
        setFields: Boolean
    ): ImmutableBitSet {
        val fieldRefBitmap: ImmutableBitSet = fieldBitmap(joinFilter)
        if (setFields) {
            fieldsRefByJoinFilter.put(joinFilter, fieldRefBitmap)
        }
        return factorBitmap(fieldRefBitmap)
    }

    /**
     * Sets bitmaps indicating which factors and fields each join filter
     * references.
     */
    @RequiresNonNull(["allJoinFilters", "joinStart", "nFieldsInJoinFactor"])
    private fun setJoinFilterRefs() {
        val filterIter: ListIterator<RexNode> = allJoinFilters.listIterator()
        while (filterIter.hasNext()) {
            val joinFilter: RexNode = filterIter.next()

            // ignore the literal filter; if necessary, we'll add it back
            // later
            if (joinFilter.isAlwaysTrue()) {
                filterIter.remove()
            }
            val factorRefBitmap: ImmutableBitSet = getJoinFilterFactorBitmap(joinFilter, true)
            factorsRefByJoinFilter.put(joinFilter, factorRefBitmap)
        }
    }

    /**
     * Sets the bitmap indicating which factors a filter references based on
     * which fields it references.
     *
     * @param fieldRefBitmap bitmap representing fields referenced
     * @return bitmap representing factors referenced that will
     * be set by this method
     */
    @RequiresNonNull(["joinStart", "nFieldsInJoinFactor"])
    private fun factorBitmap(
        fieldRefBitmap: ImmutableBitSet
    ): ImmutableBitSet {
        val factorRefBitmap: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (field in fieldRefBitmap) {
            val factor = findRef(field)
            factorRefBitmap.set(factor)
        }
        return factorRefBitmap.build()
    }

    /**
     * Determines the join factor corresponding to a RexInputRef.
     *
     * @param rexInputRef rexInputRef index
     *
     * @return index corresponding to join factor
     */
    @RequiresNonNull(["joinStart", "nFieldsInJoinFactor"])
    fun findRef(
        rexInputRef: Int
    ): Int {
        for (i in 0 until numJoinFactors) {
            if (rexInputRef >= joinStart[i]
                && rexInputRef < joinStart[i] + nFieldsInJoinFactor[i]
            ) {
                return i
            }
        }
        throw AssertionError()
    }

    /**
     * Sets weighting for each combination of factors, depending on which join
     * filters reference which factors. Greater weight is given to equality
     * conditions. Also, sets bitmaps indicating which factors are referenced by
     * each factor within join filters that are comparisons.
     */
    fun setFactorWeights() {
        factorWeights = Array(numJoinFactors) { IntArray(numJoinFactors) }
        factorsRefByFactor = arrayOfNulls<ImmutableBitSet>(numJoinFactors)
        for (i in 0 until numJoinFactors) {
            factorsRefByFactor!![i] = ImmutableBitSet.of()
        }
        for (joinFilter in allJoinFilters) {
            val factorRefs: ImmutableBitSet? = factorsRefByJoinFilter[joinFilter]

            // don't give weights to non-comparison expressions
            if (joinFilter !is RexCall) {
                continue
            }
            if (!joinFilter.isA(SqlKind.COMPARISON)) {
                continue
            }

            // OR the factors referenced in this join filter into the
            // bitmaps corresponding to each of the factors; however,
            // exclude the bit corresponding to the factor itself
            for (factor in requireNonNull(factorRefs, "factorRefs")) {
                factorsRefByFactor!![factor] = factorsRefByFactor!![factor]
                    .rebuild()
                    .addAll(factorRefs)
                    .clear(factor)
                    .build()
            }
            if (factorRefs.cardinality() === 2) {
                val leftFactor: Int = factorRefs.nextSetBit(0)
                val rightFactor: Int = factorRefs.nextSetBit(leftFactor + 1)
                val call: RexCall = joinFilter as RexCall
                val leftFields: ImmutableBitSet = fieldBitmap(call.getOperands().get(0))
                val leftBitmap: ImmutableBitSet = factorBitmap(leftFields)

                // filter contains only two factor references, one on each
                // side of the operator
                var weight: Int
                weight = if (leftBitmap.cardinality() === 1) {
                    // give higher weight to equi-joins
                    when (joinFilter.getKind()) {
                        EQUALS -> 3
                        else -> 2
                    }
                } else {
                    // cross product of two tables
                    1
                }
                setFactorWeight(weight, leftFactor, rightFactor)
            } else {
                // multiple factor references -- set a weight for each
                // combination of factors referenced within the filter
                val list: List<Integer> = ImmutableIntList.copyOf(factorRefs)
                for (outer in list) {
                    for (inner in list) {
                        if (outer != inner) {
                            setFactorWeight(1, outer, inner)
                        }
                    }
                }
            }
        }
    }

    /**
     * Sets an individual weight if the new weight is better than the current
     * one.
     *
     * @param weight weight to be set
     * @param leftFactor index of left factor
     * @param rightFactor index of right factor
     */
    @RequiresNonNull("factorWeights")
    private fun setFactorWeight(weight: Int, leftFactor: Int, rightFactor: Int) {
        if (factorWeights!![leftFactor][rightFactor] < weight) {
            factorWeights!![leftFactor][rightFactor] = weight
            factorWeights!![rightFactor][leftFactor] = weight
        }
    }

    /**
     * Returns whether if a join tree contains all factors required.
     *
     * @param joinTree Join tree to be examined
     * @param factorsNeeded Bitmap of factors required
     *
     * @return true if join tree contains all required factors
     */
    fun hasAllFactors(
        joinTree: LoptJoinTree?,
        factorsNeeded: BitSet?
    ): Boolean {
        return BitSets.contains(BitSets.of(joinTree!!.getTreeOrder()), factorsNeeded)
    }

    /**
     * Sets a bitmap indicating all child RelNodes in a join tree.
     *
     * @param joinTree join tree to be examined
     * @param childFactors bitmap to be set
     */
    @Deprecated // to be removed before 2.0
    fun getChildFactors(
        joinTree: LoptJoinTree,
        childFactors: ImmutableBitSet.Builder
    ) {
        for (child in joinTree.getTreeOrder()) {
            childFactors.set(child)
        }
    }

    /**
     * Retrieves the fields corresponding to a join between a left and right
     * tree.
     *
     * @param left left hand side of the join
     * @param right right hand side of the join
     *
     * @return fields of the join
     */
    fun getJoinFields(
        left: LoptJoinTree?,
        right: LoptJoinTree?
    ): List<RelDataTypeField> {
        val rowType: RelDataType = factory.createJoinType(
            left!!.getJoinTree().getRowType(),
            right!!.getJoinTree().getRowType()
        )
        return rowType.getFieldList()
    }

    /**
     * Adds a join factor to the set of factors that can be removed because the
     * factor is the null-generating factor in an outer join, its join keys are
     * unique, and the factor is not projected in the query.
     *
     * @param factIdx Join factor
     */
    fun addRemovableOuterJoinFactor(factIdx: Int) {
        removableOuterJoinFactors.add(factIdx)
    }

    /**
     * Return whether the factor corresponds to the null-generating factor in
     * an outer join that can be removed.
     *
     * @param factIdx Factor in question
     */
    fun isRemovableOuterJoinFactor(factIdx: Int): Boolean {
        return removableOuterJoinFactors.contains(factIdx)
    }

    /**
     * Adds to a map that keeps track of removable self-join pairs.
     *
     * @param factor1 one of the factors in the self-join
     * @param factor2 the second factor in the self-join
     */
    fun addRemovableSelfJoinPair(factor1: Int, factor2: Int) {
        val leftFactor: Int
        val rightFactor: Int

        // Put the factor with more fields on the left so it will be
        // preserved after the self-join is removed.
        if (getNumFieldsInJoinFactor(factor1)
            > getNumFieldsInJoinFactor(factor2)
        ) {
            leftFactor = factor1
            rightFactor = factor2
        } else {
            leftFactor = factor2
            rightFactor = factor1
        }

        // Compute a column mapping such that if a column from the right
        // factor is also referenced in the left factor, we will map the
        // right reference to the left to avoid redundant references.
        val columnMapping: Map<Integer?, Integer> = HashMap()

        // First, locate the originating column for all simple column
        // references in the left factor.
        val left: RelNode = getJoinFactor(leftFactor)
        val mq: RelMetadataQuery = left.getCluster().getMetadataQuery()
        val leftFactorColMapping: Map<Integer, Integer> = HashMap()
        for (i in 0 until left.getRowType().getFieldCount()) {
            val colOrigin: RelColumnOrigin = mq.getColumnOrigin(left, i)
            if (colOrigin != null && colOrigin.isDerived()) {
                leftFactorColMapping.put(
                    colOrigin.getOriginColumnOrdinal(),
                    i
                )
            }
        }

        // Then, see if the right factor references any of the same columns
        // by locating their originating columns.  If there are matches,
        // then we want to store the corresponding offset into the left
        // factor.
        val right: RelNode = getJoinFactor(rightFactor)
        for (i in 0 until right.getRowType().getFieldCount()) {
            val colOrigin: RelColumnOrigin = mq.getColumnOrigin(right, i)
            if (colOrigin == null || !colOrigin.isDerived()) {
                continue
            }
            val leftOffset: Integer = leftFactorColMapping[colOrigin.getOriginColumnOrdinal()] ?: continue
            columnMapping.put(i, leftOffset)
        }
        val selfJoin = RemovableSelfJoin(leftFactor, rightFactor, columnMapping)
        removableSelfJoinPairs.put(leftFactor, selfJoin)
        removableSelfJoinPairs.put(rightFactor, selfJoin)
    }

    /**
     * Returns the other factor in a self-join pair if the factor passed in is
     * a factor in a removable self-join; otherwise, returns null.
     *
     * @param factIdx one of the factors in a self-join pair
     */
    @Nullable
    fun getOtherSelfJoinFactor(factIdx: Int): Integer? {
        val selfJoin = removableSelfJoinPairs[factIdx]
        return if (selfJoin == null) {
            null
        } else if (selfJoin.rightFactor == factIdx) {
            selfJoin.leftFactor
        } else {
            selfJoin.rightFactor
        }
    }

    /**
     * Returns whether the factor is the left factor in a self-join.
     *
     * @param factIdx Factor in a self-join
     */
    fun isLeftFactorInRemovableSelfJoin(factIdx: Int): Boolean {
        val selfJoin = removableSelfJoinPairs[factIdx] ?: return false
        return selfJoin.leftFactor == factIdx
    }

    /**
     * Returns whether the factor is the right factor in a self-join.
     *
     * @param factIdx Factor in a self-join
     */
    fun isRightFactorInRemovableSelfJoin(factIdx: Int): Boolean {
        val selfJoin = removableSelfJoinPairs[factIdx] ?: return false
        return selfJoin.rightFactor == factIdx
    }

    /**
     * Determines whether there is a mapping from a column in the right factor
     * of a self-join to a column from the left factor. Assumes that the right
     * factor is a part of a self-join.
     *
     * @param rightFactor the index of the right factor
     * @param rightOffset the column offset of the right factor
     *
     * @return the offset of the corresponding column in the left factor, if
     * such a column mapping exists; otherwise, null is returned
     */
    @Nullable
    fun getRightColumnMapping(rightFactor: Int, rightOffset: Int): Integer? {
        val selfJoin: RemovableSelfJoin = requireNonNull(
            removableSelfJoinPairs[rightFactor]
        ) {
            ("removableSelfJoinPairs.get(rightFactor) is null for " + rightFactor
                    + ", map=" + removableSelfJoinPairs)
        }
        assert(selfJoin.rightFactor == rightFactor)
        return selfJoin.columnMapping[rightOffset]
    }

    fun createEdge(condition: RexNode): Edge {
        val fieldRefBitmap: ImmutableBitSet = fieldBitmap(condition)
        val factorRefBitmap: ImmutableBitSet = factorBitmap(fieldRefBitmap)
        return Edge(condition, factorRefBitmap, fieldRefBitmap)
    }

    /** Information about a join-condition.  */
    class Edge(condition: RexNode, factors: ImmutableBitSet, columns: ImmutableBitSet) {
        val factors: ImmutableBitSet
        val columns: ImmutableBitSet
        val condition: RexNode

        init {
            this.condition = condition
            this.factors = factors
            this.columns = columns
        }

        @Override
        override fun toString(): String {
            return ("Edge(condition: " + condition
                    + ", factors: " + factors
                    + ", columns: " + columns + ")")
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Utility class used to keep track of the factors in a removable self-join.
     * The right factor in the self-join is the one that will be removed.
     */
    class RemovableSelfJoin internal constructor(
        /** The left factor in a removable self-join.  */
        val leftFactor: Int,
        /** The right factor in a removable self-join, namely the factor that will
         * be removed.  */
        val rightFactor: Int,
        columnMapping: Map<Integer?, Integer>
    ) {
        /** A mapping that maps references to columns from the right factor to
         * columns in the left factor, if the column is referenced in both
         * factors.  */
        val columnMapping: Map<Integer?, Integer>

        init {
            this.columnMapping = columnMapping
        }
    }

    companion object {
        private fun fieldBitmap(joinFilter: RexNode?): ImmutableBitSet {
            val inputFinder: RelOptUtil.InputFinder = InputFinder()
            joinFilter.accept(inputFinder)
            return inputFinder.build()
        }
    }
}
