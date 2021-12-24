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

import org.apache.calcite.plan.RelOptCost

/**
 * Implements the logic for determining the optimal
 * semi-joins to be used in processing joins in a query.
 */
class LoptSemiJoinOptimizer(
    mq: RelMetadataQuery,
    multiJoin: LoptMultiJoin,
    rexBuilder: RexBuilder
) {
    //~ Instance fields --------------------------------------------------------
    private val rexBuilder: RexBuilder
    private val mq: RelMetadataQuery

    /**
     * Semijoins corresponding to each join factor, if they are going to be
     * filtered by semijoins. Otherwise, the entry is the original join factor.
     */
    private val chosenSemiJoins: Array<RelNode?>

    /**
     * Associates potential semijoins with each fact table factor. The first
     * parameter in the map corresponds to the fact table. The second
     * corresponds to the dimension table and a SemiJoin that captures all
     * the necessary semijoin data between that fact and dimension table
     */
    private val possibleSemiJoins: Map<Integer, Map<Integer, LogicalJoin>> = HashMap()
    private val factorCostOrdering: Ordering<Integer> = Ordering.from(FactorCostComparator())

    //~ Constructors -----------------------------------------------------------
    init {
        // there are no semijoins yet, so initialize to the original
        // factors
        this.mq = mq
        val nJoinFactors: Int = multiJoin.getNumJoinFactors()
        chosenSemiJoins = arrayOfNulls<RelNode>(nJoinFactors)
        for (i in 0 until nJoinFactors) {
            chosenSemiJoins[i] = multiJoin.getJoinFactor(i)
        }
        this.rexBuilder = rexBuilder
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Determines all possible semijoins that can be used by dimension tables to
     * filter fact tables. Constructs SemiJoinRels corresponding to potential
     * dimension table filters and stores them in the member field
     * "possibleSemiJoins"
     *
     * @param multiJoin join factors being optimized
     */
    fun makePossibleSemiJoins(multiJoin: LoptMultiJoin) {
        possibleSemiJoins.clear()

        // semijoins can't be used with any type of outer join, including full
        if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
            return
        }
        val nJoinFactors: Int = multiJoin.getNumJoinFactors()
        for (factIdx in 0 until nJoinFactors) {
            val dimFilters: Map<Integer, List<RexNode>> = HashMap()
            val semiJoinMap: Map<Integer, LogicalJoin> = HashMap()

            // loop over all filters and find equality filters that reference
            // this factor and one other factor
            for (joinFilter in multiJoin.getJoinFilters()) {
                val dimIdx = isSuitableFilter(multiJoin, joinFilter, factIdx)
                if (dimIdx == -1) {
                    continue
                }

                // if either the fact or dimension table is null generating,
                // we cannot use semijoins
                if (multiJoin.isNullGenerating(factIdx)
                    || multiJoin.isNullGenerating(dimIdx)
                ) {
                    continue
                }

                // if we've already matched against this dimension factor,
                // then add the filter to the list associated with
                // that dimension factor; otherwise, create a new entry
                var currDimFilters: List<RexNode?>? = dimFilters[dimIdx]
                if (currDimFilters == null) {
                    currDimFilters = ArrayList()
                }
                currDimFilters.add(joinFilter)
                dimFilters.put(dimIdx, currDimFilters)
            }

            // if there are potential dimension filters, determine if there
            // are appropriate indexes
            val dimIdxes: Set<Integer> = dimFilters.keySet()
            for (dimIdx in dimIdxes) {
                val joinFilters: List<RexNode>? = dimFilters[dimIdx]
                if (joinFilters != null) {
                    val semiJoin: LogicalJoin? = findSemiJoinIndexByCost(
                        multiJoin,
                        joinFilters,
                        factIdx,
                        dimIdx
                    )

                    // if an index is available, keep track of it as a
                    // possible semijoin
                    if (semiJoin != null) {
                        semiJoinMap.put(dimIdx, semiJoin)
                        possibleSemiJoins.put(factIdx, semiJoinMap)
                    }
                }
            }
        }
    }

    /**
     * Given a list of possible filters on a fact table, determine if there is
     * an index that can be used, provided all the fact table keys originate
     * from the same underlying table.
     *
     * @param multiJoin join factors being optimized
     * @param joinFilters filters to be used on the fact table
     * @param factIdx index in join factors corresponding to the fact table
     * @param dimIdx index in join factors corresponding to the dimension table
     *
     * @return SemiJoin containing information regarding the semijoin that
     * can be used to filter the fact table
     */
    @Nullable
    private fun findSemiJoinIndexByCost(
        multiJoin: LoptMultiJoin,
        joinFilters: List<RexNode>,
        factIdx: Int,
        dimIdx: Int
    ): LogicalJoin? {
        // create a SemiJoin with the semi-join condition and keys
        var semiJoinCondition: RexNode? = RexUtil.composeConjunction(rexBuilder, joinFilters)
        var leftAdjustment = 0
        for (i in 0 until factIdx) {
            leftAdjustment -= multiJoin.getNumFieldsInJoinFactor(i)
        }
        semiJoinCondition = adjustSemiJoinCondition(
            multiJoin,
            leftAdjustment,
            semiJoinCondition,
            factIdx,
            dimIdx
        )
        val factRel: RelNode = multiJoin.getJoinFactor(factIdx)
        val dimRel: RelNode = multiJoin.getJoinFactor(dimIdx)
        val joinInfo: JoinInfo = JoinInfo.of(factRel, dimRel, semiJoinCondition)
        assert(joinInfo.leftKeys.size() > 0)

        // mutable copies
        val leftKeys: List<Integer> = Lists.newArrayList(joinInfo.leftKeys)
        val rightKeys: List<Integer> = Lists.newArrayList(joinInfo.rightKeys)

        // make sure all the fact table keys originate from the same table
        // and are simple column references
        val actualLeftKeys: List<Integer> = ArrayList()
        val factTable = validateKeys(
            factRel,
            leftKeys,
            rightKeys,
            actualLeftKeys
        ) ?: return null

        // find the best index
        val bestKeyOrder: List<Integer> = ArrayList()
        val tmpFactRel = factTable.toRel(
            ViewExpanders.simpleContext(factRel.getCluster())
        ) as LcsTableScan
        val indexOptimizer = LcsIndexOptimizer(tmpFactRel)
        val bestIndex = indexOptimizer.findSemiJoinIndexByCost(
            dimRel,
            actualLeftKeys,
            rightKeys,
            bestKeyOrder
        ) ?: return null

        // if necessary, truncate the keys to reflect the ones that match
        // the index and remove the corresponding, unnecessary filters from
        // the condition; note that we don't save the actual keys here because
        // later when the semijoin is pushed past other RelNodes, the keys will
        // be converted
        val truncatedLeftKeys: List<Integer>
        val truncatedRightKeys: List<Integer>
        if (actualLeftKeys.size() === bestKeyOrder.size()) {
            truncatedLeftKeys = leftKeys
            truncatedRightKeys = rightKeys
        } else {
            truncatedLeftKeys = ArrayList()
            truncatedRightKeys = ArrayList()
            for (key in bestKeyOrder) {
                truncatedLeftKeys.add(leftKeys[key])
                truncatedRightKeys.add(rightKeys[key])
            }
            semiJoinCondition = removeExtraFilters(
                truncatedLeftKeys,
                multiJoin.getNumFieldsInJoinFactor(factIdx),
                semiJoinCondition
            )
        }
        return LogicalJoin.create(
            factRel, dimRel, ImmutableList.of(),
            requireNonNull(semiJoinCondition, "semiJoinCondition"),
            ImmutableSet.of(), JoinRelType.SEMI
        )
    }

    /**
     * Modifies the semijoin condition to reflect the fact that the RHS is now
     * the second factor into a join and the LHS is the first.
     *
     * @param multiJoin join factors being optimized
     * @param leftAdjustment amount the left RexInputRefs need to be adjusted by
     * @param semiJoinCondition condition to be adjusted
     * @param leftIdx index of the join factor corresponding to the LHS of the
     * semijoin,
     * @param rightIdx index of the join factor corresponding to the RHS of the
     * semijoin
     *
     * @return modified semijoin condition
     */
    private fun adjustSemiJoinCondition(
        multiJoin: LoptMultiJoin,
        leftAdjustment: Int,
        semiJoinCondition: RexNode?,
        leftIdx: Int,
        rightIdx: Int
    ): RexNode? {
        // adjust the semijoin condition to reflect the fact that the
        // RHS is now the second factor into the semijoin and the LHS
        // is the first
        var rightAdjustment = 0
        for (i in 0 until rightIdx) {
            rightAdjustment -= multiJoin.getNumFieldsInJoinFactor(i)
        }
        val rightStart = -rightAdjustment
        val numFieldsLeftIdx: Int = multiJoin.getNumFieldsInJoinFactor(leftIdx)
        val numFieldsRightIdx: Int = multiJoin.getNumFieldsInJoinFactor(rightIdx)
        rightAdjustment += numFieldsLeftIdx

        // only adjust the filter if adjustments are required
        if (leftAdjustment != 0 || rightAdjustment != 0) {
            val adjustments = IntArray(multiJoin.getNumTotalFields())
            if (leftAdjustment != 0) {
                for (i in -leftAdjustment until -leftAdjustment + numFieldsLeftIdx) {
                    adjustments[i] = leftAdjustment
                }
            }
            if (rightAdjustment != 0) {
                for (i in rightStart until rightStart + numFieldsRightIdx) {
                    adjustments[i] = rightAdjustment
                }
            }
            return semiJoinCondition.accept(
                RexInputConverter(
                    rexBuilder,
                    multiJoin.getMultiJoinFields(),
                    adjustments
                )
            )
        }
        return semiJoinCondition
    }

    /**
     * Validates the candidate semijoin keys corresponding to the fact table.
     * Ensure the keys all originate from the same underlying table, and they
     * all correspond to simple column references. If unsuitable keys are found,
     * they're removed from the key list and a new list corresponding to the
     * remaining valid keys is returned.
     *
     * @param factRel fact table RelNode
     * @param leftKeys fact table semijoin keys
     * @param rightKeys dimension table semijoin keys
     * @param actualLeftKeys the remaining valid fact table semijoin keys
     *
     * @return the underlying fact table if the semijoin keys are valid;
     * otherwise null
     */
    @Nullable
    private fun validateKeys(
        factRel: RelNode,
        leftKeys: List<Integer>,
        rightKeys: List<Integer>,
        actualLeftKeys: List<Integer>
    ): LcsTable? {
        var keyIdx = 0
        var theTable: RelOptTable? = null
        val keyIter: ListIterator<Integer> = leftKeys.listIterator()
        while (keyIter.hasNext()) {
            var removeKey = false
            val colOrigin: RelColumnOrigin = mq.getColumnOrigin(factRel, keyIter.next())

            // can't use the rid column as a semijoin key
            if (colOrigin == null || !colOrigin.isDerived()
                || LucidDbSpecialOperators.isLcsRidColumnId(
                    colOrigin.getOriginColumnOrdinal()
                )
            ) {
                removeKey = true
            } else {
                val table: RelOptTable = colOrigin.getOriginTable()
                if (theTable == null) {
                    if (table !is LcsTable) {
                        // not a column store table
                        removeKey = true
                    } else {
                        theTable = table
                    }
                } else {
                    // the tables must match because the column has
                    // a simple origin
                    assert(table === theTable)
                }
            }
            if (colOrigin != null && !removeKey) {
                actualLeftKeys.add(colOrigin.getOriginColumnOrdinal())
                keyIdx++
            } else {
                keyIter.remove()
                rightKeys.remove(keyIdx)
            }
        }

        // if all keys have been removed, then we don't have any valid semijoin
        // keys
        return if (actualLeftKeys.isEmpty()) {
            null
        } else {
            theTable
        }
    }

    /**
     * Removes from an expression any sub-expressions that reference key values
     * that aren't contained in a key list passed in. The keys represent join
     * keys on one side of a join. The subexpressions are all assumed to be of
     * the form "tab1.col1 = tab2.col2".
     *
     * @param keys join keys from one side of the join
     * @param nFields number of fields in the side of the join for which the
     * keys correspond
     * @param condition original expression
     *
     * @return modified expression with filters that don't reference specified
     * keys removed
     */
    @Nullable
    private fun removeExtraFilters(
        keys: List<Integer>,
        nFields: Int,
        condition: RexNode?
    ): RexNode? {
        // recursively walk the expression; if all sub-expressions are
        // removed from one side of the expression, just return what remains
        // from the other side
        assert(condition is RexCall)
        val call: RexCall? = condition as RexCall?
        if (condition.isA(SqlKind.AND)) {
            val operands: List<RexNode> = call.getOperands()
            val left: RexNode? = removeExtraFilters(
                keys,
                nFields,
                operands[0]
            )
            val right: RexNode? = removeExtraFilters(
                keys,
                nFields,
                operands[1]
            )
            if (left == null) {
                return right
            }
            return if (right == null) {
                left
            } else rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                left,
                right
            )
        }
        assert(call.getOperator() === SqlStdOperatorTable.EQUALS)
        val operands: List<RexNode> = call.getOperands()
        assert(operands[0] is RexInputRef)
        assert(operands[1] is RexInputRef)
        var idx: Int = (operands[0] as RexInputRef).getIndex()
        if (idx < nFields) {
            if (!keys.contains(idx)) {
                return null
            }
        } else {
            idx = (operands[1] as RexInputRef).getIndex()
            if (!keys.contains(idx)) {
                return null
            }
        }
        return condition
    }

    /**
     * Finds the optimal semijoin for filtering the least costly fact table from
     * among the remaining possible semijoins to choose from. The chosen
     * semijoin is stored in the chosenSemiJoins array
     *
     * @param multiJoin join factors being optimized
     *
     * @return true if a suitable semijoin is found; false otherwise
     */
    fun chooseBestSemiJoin(multiJoin: LoptMultiJoin): Boolean {
        // sort the join factors based on the cost of each factor filtered by
        // semijoins, if semijoins have been chosen
        val nJoinFactors: Int = multiJoin.getNumJoinFactors()
        val sortedFactors: List<Integer> = factorCostOrdering.immutableSortedCopy(Util.range(nJoinFactors))

        // loop through the factors in sort order, treating the factor as
        // a fact table; analyze the possible semijoins associated with
        // that fact table
        for (i in 0 until nJoinFactors) {
            val factIdx: Integer = sortedFactors[i]
            val factRel: RelNode? = chosenSemiJoins[factIdx]
            val possibleDimensions: Map<Any, Any> = possibleSemiJoins[factIdx] ?: continue
            var bestScore = 0.0
            var bestDimIdx = -1

            // loop through each dimension table associated with the current
            // fact table and analyze the ones that have semijoins with this
            // fact table
            val dimIdxes: Set<Integer> = possibleDimensions.keySet()
            for (dimIdx in dimIdxes) {
                val semiJoin = possibleDimensions[dimIdx] ?: continue

                // keep track of the dimension table that has the best score
                // for filtering this fact table
                val score = computeScore(
                    factRel,
                    chosenSemiJoins[dimIdx],
                    semiJoin
                )
                if (score > THRESHOLD_SCORE && score > bestScore) {
                    bestDimIdx = dimIdx
                    bestScore = score
                }
            }

            // if a suitable dimension table has been found, associate it
            // with the fact table in the chosenSemiJoins array; also remove
            // the entry from possibleSemiJoins so we won't chose it again;
            // note that we create the SemiJoin using the chosen semijoins
            // already created for each factor so any chaining of filters will
            // be accounted for
            if (bestDimIdx != -1) {
                val bestDimIdxFinal = bestDimIdx
                val semiJoin: LogicalJoin = requireNonNull(
                    possibleDimensions[bestDimIdxFinal]
                ) { "possibleDimensions.get($bestDimIdxFinal) is null" }
                val chosenSemiJoin: LogicalJoin = LogicalJoin.create(
                    factRel,
                    chosenSemiJoins[bestDimIdx],
                    ImmutableList.of(),
                    semiJoin.getCondition(),
                    ImmutableSet.of(),
                    JoinRelType.SEMI
                )
                chosenSemiJoins[factIdx] = chosenSemiJoin

                // determine if the dimension table doesn't need to be joined
                // as a result of this semijoin
                removeJoin(multiJoin, chosenSemiJoin, factIdx, bestDimIdx)
                removePossibleSemiJoin(
                    possibleDimensions,
                    factIdx,
                    bestDimIdx
                )

                // need to also remove the semijoin from the possible
                // semijoins associated with this dimension table, as the
                // semijoin can only be used to filter one table, not both
                removePossibleSemiJoin(
                    possibleSemiJoins[bestDimIdx],
                    bestDimIdx,
                    factIdx
                )
                return true
            }

            // continue searching on the next fact table if we couldn't find
            // a semijoin for the current fact table
        }
        return false
    }

    /**
     * Computes a score relevant to applying a set of semijoins on a fact table.
     * The higher the score, the better.
     *
     * @param factRel fact table being filtered
     * @param dimRel dimension table that participates in semijoin
     * @param semiJoin semijoin between fact and dimension tables
     *
     * @return computed score of applying the dimension table filters on the
     * fact table
     */
    private fun computeScore(
        factRel: RelNode?,
        dimRel: RelNode?,
        semiJoin: LogicalJoin?
    ): Double {
        // Estimate savings as a result of applying semijoin filter on fact
        // table.  As a heuristic, the selectivity of the semijoin needs to
        // be less than half.  There may be instances where an even smaller
        // selectivity value is required because of the overhead of
        // index lookups on a very large fact table.  Half was chosen as
        // a middle ground based on testing that was done with a large
        // data set.
        val dimCols: ImmutableBitSet = ImmutableBitSet.of(semiJoin.analyzeCondition().rightKeys)
        val selectivity: Double = RelMdUtil.computeSemiJoinSelectivity(mq, factRel, dimRel, semiJoin)
        if (selectivity > .5) {
            return 0
        }
        val factCost: RelOptCost = mq.getCumulativeCost(factRel) ?: return 0

        // if not enough information, return a low score
        var savings: Double = ((1.0 - Math.sqrt(selectivity))
                * Math.max(1.0, factCost.getRows()))

        // Additional savings if the dimension columns are unique.  We can
        // ignore nulls since they will be filtered out by the semijoin.
        val uniq: Boolean = RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
            mq,
            dimRel, dimCols
        )
        if (uniq) {
            savings *= 2.0
        }

        // compute the cost of doing an extra scan on the dimension table,
        // including the distinct sort on top of the scan; if the dimension
        // columns are already unique, no need to add on the dup removal cost
        val dimSortCost: Double = mq.getRowCount(dimRel)
        val dupRemCost: Double = if (uniq) 0 else dimSortCost
        val dimCost: RelOptCost = mq.getCumulativeCost(dimRel)
        if (dimSortCost == null
            || dupRemCost == null
            || dimCost == null
        ) {
            return 0
        }
        var dimRows: Double = dimCost.getRows()
        if (dimRows < 1.0) {
            dimRows = 1.0
        }
        return savings / dimRows
    }

    /**
     * Determines whether a join of the dimension table in a semijoin can be
     * removed. It can be if the dimension keys are unique and the only fields
     * referenced from the dimension table are its semijoin keys. The semijoin
     * keys can be mapped to the corresponding keys from the fact table (because
     * of the equality condition associated with the semijoin keys). Therefore,
     * that's why the dimension table can be removed even though those fields
     * are referenced elsewhere in the query tree.
     *
     * @param multiJoin join factors being optimized
     * @param semiJoin semijoin under consideration
     * @param factIdx id of the fact table in the semijoin
     * @param dimIdx id of the dimension table in the semijoin
     */
    private fun removeJoin(
        multiJoin: LoptMultiJoin,
        semiJoin: LogicalJoin,
        factIdx: Int,
        dimIdx: Int
    ) {
        // if the dimension can be removed because of another semijoin, then
        // no need to proceed any further
        if (multiJoin.getJoinRemovalFactor(dimIdx) != null) {
            return
        }

        // Check if the semijoin keys corresponding to the dimension table
        // are unique.  The semijoin will filter out the nulls.
        val dimKeys: ImmutableBitSet = ImmutableBitSet.of(semiJoin.analyzeCondition().rightKeys)
        val dimRel: RelNode = multiJoin.getJoinFactor(dimIdx)
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                mq, dimRel,
                dimKeys
            )
        ) {
            return
        }

        // check that the only fields referenced from the dimension table
        // in either its projection or join conditions are the dimension
        // keys
        var dimProjRefs: ImmutableBitSet = multiJoin.getProjFields(dimIdx)
        if (dimProjRefs == null) {
            val nDimFields: Int = multiJoin.getNumFieldsInJoinFactor(dimIdx)
            dimProjRefs = ImmutableBitSet.range(0, nDimFields)
        }
        if (!dimKeys.contains(dimProjRefs)) {
            return
        }
        val dimJoinRefCounts: IntArray = multiJoin.getJoinFieldRefCounts(dimIdx)
        for (i in dimJoinRefCounts.indices) {
            if (dimJoinRefCounts[i] > 0) {
                if (!dimKeys.get(i)) {
                    return
                }
            }
        }

        // criteria met; keep track of the fact table and the semijoin that
        // allow the join of this dimension table to be removed
        multiJoin.setJoinRemovalFactor(dimIdx, factIdx)
        multiJoin.setJoinRemovalSemiJoin(dimIdx, semiJoin)

        // if the dimension table doesn't reference anything in its projection
        // and the only fields referenced in its joins are the dimension keys
        // of this semijoin, then we can decrement the join reference counts
        // corresponding to the fact table's semijoin keys, since the
        // dimension table doesn't need to use those keys
        if (dimProjRefs.cardinality() !== 0) {
            return
        }
        for (i in dimJoinRefCounts.indices) {
            if (dimJoinRefCounts[i] > 1) {
                return
            } else if (dimJoinRefCounts[i] == 1) {
                if (!dimKeys.get(i)) {
                    return
                }
            }
        }
        val factJoinRefCounts: IntArray = multiJoin.getJoinFieldRefCounts(factIdx)
        for (key in semiJoin.analyzeCondition().leftKeys) {
            factJoinRefCounts[key]--
        }
    }

    /**
     * Removes a dimension table from a fact table's list of possible semi-joins.
     *
     * @param possibleDimensions possible dimension tables associated with the
     * fact table
     * @param factIdx index corresponding to fact table
     * @param dimIdx index corresponding to dimension table
     */
    private fun removePossibleSemiJoin(
        @Nullable possibleDimensions: Map<Integer, LogicalJoin>?,
        factIdx: Integer,
        dimIdx: Integer
    ) {
        // dimension table may not have a corresponding semijoin if it
        // wasn't indexable
        if (possibleDimensions == null) {
            return
        }
        possibleDimensions.remove(dimIdx)
        if (possibleDimensions.isEmpty()) {
            possibleSemiJoins.remove(factIdx)
        } else {
            possibleSemiJoins.put(factIdx, possibleDimensions)
        }
    }

    /**
     * Returns the optimal semijoin for the specified factor; may be the factor
     * itself if semijoins are not chosen for the factor.
     *
     * @param factIdx Index corresponding to the desired factor
     */
    fun getChosenSemiJoin(factIdx: Int): RelNode? {
        return chosenSemiJoins[factIdx]
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Compares factors.  */
    private inner class FactorCostComparator : Comparator<Integer?> {
        @Override
        fun compare(rel1Idx: Integer?, rel2Idx: Integer?): Int {
            val c1: RelOptCost = mq.getCumulativeCost(chosenSemiJoins[rel1Idx])
            val c2: RelOptCost = mq.getCumulativeCost(chosenSemiJoins[rel2Idx])

            // nulls are arbitrarily sorted
            if (c1 == null || c2 == null) {
                return -1
            }
            return if (c1.isLt(c2)) -1 else if (c1.equals(c2)) 0 else 1
        }
    }

    /** Dummy class to allow code to compile.  */
    private abstract class LcsTable : RelOptTable

    /** Dummy class to allow code to compile.  */
    private class LcsTableScan

    /** Dummy class to allow code to compile.  */
    private class LcsIndexOptimizer internal constructor(rel: LcsTableScan?) {
        @Nullable
        fun findSemiJoinIndexByCost(
            dimRel: RelNode?,
            actualLeftKeys: List<Integer?>?, rightKeys: List<Integer?>?,
            bestKeyOrder: List<Integer?>?
        ): FemLocalIndex? {
            return null
        }
    }

    /** Dummy class to allow code to compile.  */
    private class FemLocalIndex

    /** Dummy class to allow code to compile.  */
    private object LucidDbSpecialOperators {
        fun isLcsRidColumnId(originColumnOrdinal: Int): Boolean {
            return false
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        // minimum score required for a join filter to be considered
        private const val THRESHOLD_SCORE = 10

        /**
         * Determines if a join filter can be used with a semijoin against a
         * specified fact table. A suitable filter is of the form "factable.col1 =
         * dimTable.col2".
         *
         * @param multiJoin join factors being optimized
         * @param joinFilter filter to be analyzed
         * @param factIdx index corresponding to the fact table
         *
         * @return index of corresponding dimension table if the filter is
         * appropriate; otherwise -1 is returned
         */
        private fun isSuitableFilter(
            multiJoin: LoptMultiJoin,
            joinFilter: RexNode,
            factIdx: Int
        ): Int {
            // ignore non-equality filters where the operands are not
            // RexInputRefs
            when (joinFilter.getKind()) {
                EQUALS -> {}
                else -> return -1
            }
            val operands: List<RexNode> = (joinFilter as RexCall).getOperands()
            if (operands[0] !is RexInputRef
                || operands[1] !is RexInputRef
            ) {
                return -1
            }

            // filter is suitable if each side of the filter only contains a
            // single factor reference and one side references the fact table and
            // the other references the dimension table; since we know this is
            // a join filter and we've already verified that the operands are
            // RexInputRefs, verify that the factors belong to the fact and
            // dimension table
            val joinRefs: ImmutableBitSet = multiJoin.getFactorsRefByJoinFilter(joinFilter)
            assert(joinRefs.cardinality() === 2)
            val factor1: Int = joinRefs.nextSetBit(0)
            val factor2: Int = joinRefs.nextSetBit(factor1 + 1)
            if (factor1 == factIdx) {
                return factor2
            }
            return if (factor2 == factIdx) {
                factor1
            } else -1
        }
    }
}
