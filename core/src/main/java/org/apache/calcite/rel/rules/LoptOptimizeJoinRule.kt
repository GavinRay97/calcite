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
 * Planner rule that implements the heuristic planner for determining optimal
 * join orderings.
 *
 *
 * It is triggered by the pattern
 * [org.apache.calcite.rel.logical.LogicalProject]
 * ([MultiJoin]).
 *
 * @see CoreRules.MULTI_JOIN_OPTIMIZE
 */
@Value.Enclosing
class LoptOptimizeJoinRule
/** Creates an LoptOptimizeJoinRule.  */
protected constructor(config: Config?) : RelRule<LoptOptimizeJoinRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        joinFactory: RelFactories.JoinFactory?,
        projectFactory: RelFactories.ProjectFactory?,
        filterFactory: FilterFactory?
    ) : this(RelBuilder.proto(joinFactory, projectFactory, filterFactory)) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val multiJoinRel: MultiJoin = call.rel(0)
        val multiJoin = LoptMultiJoin(multiJoinRel)
        val mq: RelMetadataQuery = call.getMetadataQuery()
        findRemovableOuterJoins(mq, multiJoin)
        val rexBuilder: RexBuilder = multiJoinRel.getCluster().getRexBuilder()
        val semiJoinOpt = LoptSemiJoinOptimizer(call.getMetadataQuery(), multiJoin, rexBuilder)

        // determine all possible semijoins
        semiJoinOpt.makePossibleSemiJoins(multiJoin)

        // select the optimal join filters for semijoin filtering by
        // iteratively calling chooseBestSemiJoin; chooseBestSemiJoin will
        // apply semijoins in sort order, based on the cost of scanning each
        // factor; as it selects semijoins to apply and iterates through the
        // loop, the cost of scanning a factor will decrease in accordance
        // with the semijoins selected
        var iterations = 0
        do {
            if (!semiJoinOpt.chooseBestSemiJoin(multiJoin)) {
                break
            }
            if (iterations++ > 10) {
                break
            }
        } while (true)
        multiJoin.setFactorWeights()
        findRemovableSelfJoins(mq, multiJoin)
        findBestOrderings(mq, call.builder(), multiJoin, semiJoinOpt, call)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): LoptOptimizeJoinRule? {
            return LoptOptimizeJoinRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableLoptOptimizeJoinRule.Config.of()
                .withOperandSupplier { b -> b.operand(MultiJoin::class.java).anyInputs() }
        }
    }

    companion object {
        /**
         * Locates all null generating factors whose outer join can be removed. The
         * outer join can be removed if the join keys corresponding to the null
         * generating factor are unique and no columns are projected from it.
         *
         * @param multiJoin join factors being optimized
         */
        private fun findRemovableOuterJoins(mq: RelMetadataQuery, multiJoin: LoptMultiJoin) {
            val removalCandidates: List<Integer> = ArrayList()
            for (factIdx in 0 until multiJoin.getNumJoinFactors()) {
                if (multiJoin.isNullGenerating(factIdx)) {
                    removalCandidates.add(factIdx)
                }
            }
            while (!removalCandidates.isEmpty()) {
                val retryCandidates: Set<Integer> = HashSet()
                outerForLoop@ for (factIdx in removalCandidates) {
                    // reject the factor if it is referenced in the projection list
                    val projFields: ImmutableBitSet = multiJoin.getProjFields(factIdx)
                    if (projFields == null || projFields.cardinality() > 0) {
                        continue
                    }

                    // setup a bitmap containing the equi-join keys corresponding to
                    // the null generating factor; both operands in the filter must
                    // be RexInputRefs and only one side corresponds to the null
                    // generating factor
                    val outerJoinCond: RexNode = multiJoin.getOuterJoinCond(factIdx)
                    val ojFilters: List<RexNode> = ArrayList()
                    RelOptUtil.decomposeConjunction(outerJoinCond, ojFilters)
                    val numFields: Int = multiJoin.getNumFieldsInJoinFactor(factIdx)
                    val joinKeyBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
                    val otherJoinKeyBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
                    val firstFieldNum: Int = multiJoin.getJoinStart(factIdx)
                    val lastFieldNum = firstFieldNum + numFields
                    for (filter in ojFilters) {
                        if (filter !is RexCall) {
                            continue
                        }
                        val filterCall: RexCall = filter as RexCall
                        if (filterCall.getOperator() !== SqlStdOperatorTable.EQUALS
                            || filterCall.getOperands().get(0) !is RexInputRef
                            || filterCall.getOperands().get(1) !is RexInputRef
                        ) {
                            continue
                        }
                        val leftRef: Int = (filterCall.getOperands().get(0) as RexInputRef).getIndex()
                        val rightRef: Int = (filterCall.getOperands().get(1) as RexInputRef).getIndex()
                        setJoinKey(
                            joinKeyBuilder,
                            otherJoinKeyBuilder,
                            leftRef,
                            rightRef,
                            firstFieldNum,
                            lastFieldNum,
                            true
                        )
                    }
                    if (joinKeyBuilder.cardinality() === 0) {
                        continue
                    }

                    // make sure the only join fields referenced are the ones in
                    // the current outer join
                    val joinKeys: ImmutableBitSet = joinKeyBuilder.build()
                    val joinFieldRefCounts: IntArray = multiJoin.getJoinFieldRefCounts(factIdx)
                    for (i in joinFieldRefCounts.indices) {
                        if (joinFieldRefCounts[i] > 1
                            || !joinKeys.get(i) && joinFieldRefCounts[i] == 1
                        ) {
                            continue@outerForLoop
                        }
                    }

                    // See if the join keys are unique.  Because the keys are
                    // part of an equality join condition, nulls are filtered out
                    // by the join.  So, it's ok if there are nulls in the join
                    // keys.
                    if (RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                            mq,
                            multiJoin.getJoinFactor(factIdx), joinKeys
                        )
                    ) {
                        multiJoin.addRemovableOuterJoinFactor(factIdx)

                        // Since we are no longer joining this factor,
                        // decrement the reference counters corresponding to
                        // the join keys from the other factors that join with
                        // this one.  Later, in the outermost loop, we'll have
                        // the opportunity to retry removing those factors.
                        val otherJoinKeys: ImmutableBitSet = otherJoinKeyBuilder.build()
                        for (otherKey in otherJoinKeys) {
                            val otherFactor: Int = multiJoin.findRef(otherKey)
                            if (multiJoin.isNullGenerating(otherFactor)) {
                                retryCandidates.add(otherFactor)
                            }
                            val otherJoinFieldRefCounts: IntArray = multiJoin.getJoinFieldRefCounts(otherFactor)
                            val offset: Int = multiJoin.getJoinStart(otherFactor)
                            --otherJoinFieldRefCounts[otherKey - offset]
                        }
                    }
                }
                removalCandidates.clear()
                removalCandidates.addAll(retryCandidates)
            }
        }

        /**
         * Sets a join key if only one of the specified input references corresponds
         * to a specified factor as determined by its field numbers. Also keeps
         * track of the keys from the other factor.
         *
         * @param joinKeys join keys to be set if a key is found
         * @param otherJoinKeys join keys for the other join factor
         * @param ref1 first input reference
         * @param ref2 second input reference
         * @param firstFieldNum first field number of the factor
         * @param lastFieldNum last field number + 1 of the factor
         * @param swap if true, check for the desired input reference in the second
         * input reference parameter if the first input reference isn't the correct
         * one
         */
        private fun setJoinKey(
            joinKeys: ImmutableBitSet.Builder,
            otherJoinKeys: ImmutableBitSet.Builder,
            ref1: Int,
            ref2: Int,
            firstFieldNum: Int,
            lastFieldNum: Int,
            swap: Boolean
        ) {
            if (ref1 >= firstFieldNum && ref1 < lastFieldNum) {
                if (!(ref2 >= firstFieldNum && ref2 < lastFieldNum)) {
                    joinKeys.set(ref1 - firstFieldNum)
                    otherJoinKeys.set(ref2)
                }
                return
            }
            if (swap) {
                setJoinKey(
                    joinKeys,
                    otherJoinKeys,
                    ref2,
                    ref1,
                    firstFieldNum,
                    lastFieldNum,
                    false
                )
            }
        }

        /**
         * Locates pairs of joins that are self-joins where the join can be removed
         * because the join condition between the two factors is an equality join on
         * unique keys.
         *
         * @param multiJoin join factors being optimized
         */
        private fun findRemovableSelfJoins(mq: RelMetadataQuery, multiJoin: LoptMultiJoin) {
            // Candidates for self-joins must be simple factors
            val simpleFactors: Map<Integer, RelOptTable> = getSimpleFactors(mq, multiJoin)

            // See if a simple factor is repeated and therefore potentially is
            // part of a self-join.  Restrict each factor to at most one
            // self-join.
            val repeatedTables: List<RelOptTable?> = ArrayList()
            val selfJoinPairs: Map<Integer, Integer> = HashMap()
            @KeyFor("simpleFactors") val factors: Array<Integer> =
                TreeSet(simpleFactors.keySet()).toArray(arrayOfNulls<Integer>(0))
            for (i in factors.indices) {
                if (repeatedTables.contains(simpleFactors[factors[i]])) {
                    continue
                }
                for (j in i + 1 until factors.size) {
                    @KeyFor("simpleFactors") val leftFactor: Int = factors[i]
                    @KeyFor("simpleFactors") val rightFactor: Int = factors[j]
                    if (simpleFactors[leftFactor].getQualifiedName().equals(
                            simpleFactors[rightFactor].getQualifiedName()
                        )
                    ) {
                        selfJoinPairs.put(leftFactor, rightFactor)
                        repeatedTables.add(simpleFactors[leftFactor])
                        break
                    }
                }
            }

            // From the candidate self-join pairs, determine if there is
            // the appropriate join condition between the two factors that will
            // allow the join to be removed.
            for (factor1 in selfJoinPairs.keySet()) {
                val factor2: Int = selfJoinPairs[factor1]
                val selfJoinFilters: List<RexNode> = ArrayList()
                for (filter in multiJoin.getJoinFilters()) {
                    val joinFactors: ImmutableBitSet = multiJoin.getFactorsRefByJoinFilter(filter)
                    if (joinFactors.cardinality() === 2
                        && joinFactors.get(factor1)
                        && joinFactors.get(factor2)
                    ) {
                        selfJoinFilters.add(filter)
                    }
                }
                if (selfJoinFilters.size() > 0
                    && isSelfJoinFilterUnique(
                        mq,
                        multiJoin,
                        factor1,
                        factor2,
                        selfJoinFilters
                    )
                ) {
                    multiJoin.addRemovableSelfJoinPair(factor1, factor2)
                }
            }
        }

        /**
         * Retrieves join factors that correspond to simple table references. A
         * simple table reference is a single table reference with no grouping or
         * aggregation.
         *
         * @param multiJoin join factors being optimized
         *
         * @return map consisting of the simple factors and the tables they
         * correspond
         */
        private fun getSimpleFactors(
            mq: RelMetadataQuery,
            multiJoin: LoptMultiJoin
        ): Map<Integer, RelOptTable> {
            val returnList: Map<Integer, RelOptTable> = HashMap()

            // Loop through all join factors and locate the ones where each
            // column referenced from the factor is not derived and originates
            // from the same underlying table.  Also, discard factors that
            // are null-generating or will be removed because of semijoins.
            if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
                return returnList
            }
            for (factIdx in 0 until multiJoin.getNumJoinFactors()) {
                if (multiJoin.isNullGenerating(factIdx)
                    || multiJoin.getJoinRemovalFactor(factIdx) != null
                ) {
                    continue
                }
                val rel: RelNode = multiJoin.getJoinFactor(factIdx)
                val table: RelOptTable = mq.getTableOrigin(rel)
                if (table != null) {
                    returnList.put(factIdx, table)
                }
            }
            return returnList
        }

        /**
         * Determines if the equality join filters between two factors that map to
         * the same table consist of unique, identical keys.
         *
         * @param multiJoin join factors being optimized
         * @param leftFactor left factor in the join
         * @param rightFactor right factor in the join
         * @param joinFilterList list of join filters between the two factors
         *
         * @return true if the criteria are met
         */
        private fun isSelfJoinFilterUnique(
            mq: RelMetadataQuery,
            multiJoin: LoptMultiJoin,
            leftFactor: Int,
            rightFactor: Int,
            joinFilterList: List<RexNode>
        ): Boolean {
            val rexBuilder: RexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder()
            val leftRel: RelNode = multiJoin.getJoinFactor(leftFactor)
            val rightRel: RelNode = multiJoin.getJoinFactor(rightFactor)
            var joinFilters: RexNode = RexUtil.composeConjunction(rexBuilder, joinFilterList)

            // Adjust the offsets in the filter by shifting the left factor
            // to the left and shifting the right factor to the left and then back
            // to the right by the number of fields in the left
            val adjustments = IntArray(multiJoin.getNumTotalFields())
            val leftAdjust: Int = multiJoin.getJoinStart(leftFactor)
            val nLeftFields: Int = leftRel.getRowType().getFieldCount()
            for (i in 0 until nLeftFields) {
                adjustments[leftAdjust + i] = -leftAdjust
            }
            val rightAdjust: Int = multiJoin.getJoinStart(rightFactor)
            for (i in 0 until rightRel.getRowType().getFieldCount()) {
                adjustments[rightAdjust + i] = -rightAdjust + nLeftFields
            }
            joinFilters = joinFilters.accept(
                RexInputConverter(
                    rexBuilder,
                    multiJoin.getMultiJoinFields(),
                    leftRel.getRowType().getFieldList(),
                    rightRel.getRowType().getFieldList(),
                    adjustments
                )
            )
            return areSelfJoinKeysUnique(mq, leftRel, rightRel, joinFilters)
        }

        /**
         * Generates N optimal join orderings. Each ordering contains each factor as
         * the first factor in the ordering.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins for each factor
         * @param call RelOptRuleCall associated with this rule
         */
        private fun findBestOrderings(
            mq: RelMetadataQuery,
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            call: RelOptRuleCall
        ) {
            val plans: List<RelNode> = ArrayList()
            val fieldNames: List<String> = multiJoin.getMultiJoinRel().getRowType().getFieldNames()

            // generate the N join orderings
            for (i in 0 until multiJoin.getNumJoinFactors()) {
                // first factor cannot be null generating
                if (multiJoin.isNullGenerating(i)) {
                    continue
                }
                val joinTree: LoptJoinTree = createOrdering(
                    mq,
                    relBuilder,
                    multiJoin,
                    semiJoinOpt,
                    i
                ) ?: continue
                val newProject: RelNode = createTopProject(call.builder(), multiJoin, joinTree, fieldNames)
                plans.add(newProject)
            }

            // transform the selected plans; note that we wait till then the end to
            // transform everything so any intermediate RelNodes we create are not
            // converted to RelSubsets The HEP planner will choose the join subtree
            // with the best cumulative cost. Volcano planner keeps the alternative
            // join subtrees and cost the final plan to pick the best one.
            for (plan in plans) {
                call.transformTo(plan)
            }
        }

        /**
         * Creates the topmost projection that will sit on top of the selected join
         * ordering. The projection needs to match the original join ordering. Also,
         * places any post-join filters on top of the project.
         *
         * @param multiJoin join factors being optimized
         * @param joinTree selected join ordering
         * @param fieldNames field names corresponding to the projection expressions
         *
         * @return created projection
         */
        private fun createTopProject(
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            joinTree: LoptJoinTree,
            fieldNames: List<String>
        ): RelNode {
            val newProjExprs: List<RexNode> = ArrayList()
            val rexBuilder: RexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder()

            // create a projection on top of the joins, matching the original
            // join order
            val newJoinOrder: List<Integer> = joinTree.getTreeOrder()
            val nJoinFactors: Int = multiJoin.getNumJoinFactors()
            val fields: List<RelDataTypeField> = multiJoin.getMultiJoinFields()

            // create a mapping from each factor to its field offset in the join
            // ordering
            val factorToOffsetMap: Map<Integer, Integer> = HashMap()
            var pos = 0
            var fieldStart = 0
            while (pos < nJoinFactors) {
                factorToOffsetMap.put(newJoinOrder[pos], fieldStart)
                fieldStart += multiJoin.getNumFieldsInJoinFactor(newJoinOrder[pos])
                pos++
            }
            for (currFactor in 0 until nJoinFactors) {
                // if the factor is the right factor in a removable self-join,
                // then where possible, remap references to the right factor to
                // the corresponding reference in the left factor
                var leftFactor: Integer? = null
                if (multiJoin.isRightFactorInRemovableSelfJoin(currFactor)) {
                    leftFactor = multiJoin.getOtherSelfJoinFactor(currFactor)
                }
                for (fieldPos in 0 until multiJoin.getNumFieldsInJoinFactor(currFactor)) {
                    var newOffset: Int = requireNonNull(
                        factorToOffsetMap[currFactor]
                    ) { "factorToOffsetMap.get(currFactor)" } + fieldPos
                    if (leftFactor != null) {
                        val leftOffset: Integer = multiJoin.getRightColumnMapping(currFactor, fieldPos)
                        if (leftOffset != null) {
                            newOffset = requireNonNull(
                                factorToOffsetMap[leftFactor],
                                "factorToOffsetMap.get(leftFactor)"
                            ) + leftOffset
                        }
                    }
                    newProjExprs.add(
                        rexBuilder.makeInputRef(
                            fields[newProjExprs.size()].getType(),
                            newOffset
                        )
                    )
                }
            }
            relBuilder.push(joinTree.getJoinTree())
            relBuilder.project(newProjExprs, fieldNames)

            // Place the post-join filter (if it exists) on top of the final
            // projection.
            val postJoinFilter: RexNode = multiJoin.getMultiJoinRel().getPostJoinFilter()
            if (postJoinFilter != null) {
                relBuilder.filter(postJoinFilter)
            }
            return relBuilder.build()
        }

        /**
         * Computes the cardinality of the join columns from a particular factor,
         * when that factor is joined with another join tree.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins chosen for each factor
         * @param joinTree the join tree that the factor is being joined with
         * @param filters possible join filters to select from
         * @param factor the factor being added
         *
         * @return computed cardinality
         */
        @Nullable
        private fun computeJoinCardinality(
            mq: RelMetadataQuery,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            joinTree: LoptJoinTree,
            filters: List<RexNode>,
            factor: Int
        ): Double? {
            val childFactors: ImmutableBitSet = ImmutableBitSet.builder()
                .addAll(joinTree.getTreeOrder())
                .set(factor)
                .build()
            val factorStart: Int = multiJoin.getJoinStart(factor)
            val nFields: Int = multiJoin.getNumFieldsInJoinFactor(factor)
            val joinKeys: ImmutableBitSet.Builder = ImmutableBitSet.builder()

            // first loop through the inner join filters, picking out the ones
            // that reference only the factors in either the join tree or the
            // factor that will be added
            setFactorJoinKeys(
                multiJoin,
                filters,
                childFactors,
                factorStart,
                nFields,
                joinKeys
            )

            // then loop through the outer join filters where the factor being
            // added is the null generating factor in the outer join
            setFactorJoinKeys(
                multiJoin,
                RelOptUtil.conjunctions(multiJoin.getOuterJoinCond(factor)),
                childFactors,
                factorStart,
                nFields,
                joinKeys
            )

            // if the join tree doesn't contain all the necessary factors in
            // any of the join filters, then joinKeys will be empty, so return
            // null in that case
            return if (joinKeys.isEmpty()) {
                null
            } else {
                mq.getDistinctRowCount(
                    semiJoinOpt.getChosenSemiJoin(factor),
                    joinKeys.build(), null
                )
            }
        }

        /**
         * Locates from a list of filters those that correspond to a particular join
         * tree. Then, for each of those filters, extracts the fields corresponding
         * to a particular factor, setting them in a bitmap.
         *
         * @param multiJoin join factors being optimized
         * @param filters list of join filters
         * @param joinFactors bitmap containing the factors in a particular join
         * tree
         * @param factorStart the initial offset of the factor whose join keys will
         * be extracted
         * @param nFields the number of fields in the factor whose join keys will be
         * extracted
         * @param joinKeys the bitmap that will be set with the join keys
         */
        private fun setFactorJoinKeys(
            multiJoin: LoptMultiJoin,
            filters: List<RexNode>,
            joinFactors: ImmutableBitSet,
            factorStart: Int,
            nFields: Int,
            joinKeys: ImmutableBitSet.Builder
        ) {
            for (joinFilter in filters) {
                val filterFactors: ImmutableBitSet = multiJoin.getFactorsRefByJoinFilter(joinFilter)

                // if all factors in the join filter are in the bitmap containing
                // the factors in a join tree, then from that filter, add the
                // fields corresponding to the specified factor to the join key
                // bitmap; in doing so, adjust the join keys so they start at
                // offset 0
                if (joinFactors.contains(filterFactors)) {
                    val joinFields: ImmutableBitSet = multiJoin.getFieldsRefByJoinFilter(joinFilter)
                    var field: Int = joinFields.nextSetBit(factorStart)
                    while (field >= 0
                        && field < factorStart + nFields
                    ) {
                        joinKeys.set(field - factorStart)
                        field = joinFields.nextSetBit(field + 1)
                    }
                }
            }
        }

        /**
         * Generates a join tree with a specific factor as the first factor in the
         * join tree.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins for each factor
         * @param firstFactor first factor in the tree
         *
         * @return constructed join tree or null if it is not possible for
         * firstFactor to appear as the first factor in the join
         */
        @Nullable
        private fun createOrdering(
            mq: RelMetadataQuery,
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            firstFactor: Int
        ): LoptJoinTree? {
            var joinTree: LoptJoinTree? = null
            val nJoinFactors: Int = multiJoin.getNumJoinFactors()
            val factorsToAdd: BitSet = BitSets.range(0, nJoinFactors)
            val factorsAdded = BitSet(nJoinFactors)
            val filtersToAdd: List<RexNode> = ArrayList(multiJoin.getJoinFilters())
            var prevFactor = -1
            while (factorsToAdd.cardinality() > 0) {
                var nextFactor: Int
                var selfJoin = false
                if (factorsAdded.cardinality() === 0) {
                    nextFactor = firstFactor
                } else {
                    // If the factor just added is part of a removable self-join
                    // and the other half of the self-join hasn't been added yet,
                    // then add it next.  Otherwise, look for the optimal factor
                    // to add next.
                    val selfJoinFactor: Integer = multiJoin.getOtherSelfJoinFactor(prevFactor)
                    if (selfJoinFactor != null
                        && !factorsAdded.get(selfJoinFactor)
                    ) {
                        nextFactor = selfJoinFactor
                        selfJoin = true
                    } else {
                        nextFactor = getBestNextFactor(
                            mq,
                            multiJoin,
                            factorsToAdd,
                            factorsAdded,
                            semiJoinOpt,
                            joinTree,
                            filtersToAdd
                        )
                    }
                }

                // add the factor; pass in a bitmap representing the factors
                // this factor joins with that have already been added to
                // the tree
                val factorsNeeded: BitSet = multiJoin.getFactorsRefByFactor(nextFactor).toBitSet()
                if (multiJoin.isNullGenerating(nextFactor)) {
                    factorsNeeded.or(multiJoin.getOuterJoinFactors(nextFactor).toBitSet())
                }
                factorsNeeded.and(factorsAdded)
                joinTree = addFactorToTree(
                    mq,
                    relBuilder,
                    multiJoin,
                    semiJoinOpt,
                    joinTree,
                    nextFactor,
                    factorsNeeded,
                    filtersToAdd,
                    selfJoin
                )
                if (joinTree == null) {
                    return null
                }
                factorsToAdd.clear(nextFactor)
                factorsAdded.set(nextFactor)
                prevFactor = nextFactor
            }
            assert(filtersToAdd.size() === 0)
            return joinTree
        }

        /**
         * Determines the best factor to be added next into a join tree.
         *
         * @param multiJoin join factors being optimized
         * @param factorsToAdd factors to choose from to add next
         * @param factorsAdded factors that have already been added to the join tree
         * @param semiJoinOpt optimal semijoins for each factor
         * @param joinTree join tree constructed thus far
         * @param filtersToAdd remaining filters that need to be added
         *
         * @return index of the best factor to add next
         */
        private fun getBestNextFactor(
            mq: RelMetadataQuery,
            multiJoin: LoptMultiJoin,
            factorsToAdd: BitSet,
            factorsAdded: BitSet,
            semiJoinOpt: LoptSemiJoinOptimizer,
            @Nullable joinTree: LoptJoinTree?,
            filtersToAdd: List<RexNode>
        ): Int {
            // iterate through the remaining factors and determine the
            // best one to add next
            var nextFactor = -1
            var bestWeight = 0
            var bestCardinality: Double? = null
            val factorWeights: Array<IntArray> = multiJoin.getFactorWeights()
            for (factor in BitSets.toIter(factorsToAdd)) {
                // if the factor corresponds to a dimension table whose
                // join we can remove, make sure the the corresponding fact
                // table is in the current join tree
                val factIdx: Integer = multiJoin.getJoinRemovalFactor(factor)
                if (factIdx != null) {
                    if (!factorsAdded.get(factIdx)) {
                        continue
                    }
                }

                // can't add a null-generating factor if its dependent,
                // non-null generating factors haven't been added yet
                if (multiJoin.isNullGenerating(factor)
                    && !BitSets.contains(
                        factorsAdded,
                        multiJoin.getOuterJoinFactors(factor)
                    )
                ) {
                    continue
                }

                // determine the best weight between the current factor
                // under consideration and the factors that have already
                // been added to the tree
                var dimWeight = 0
                for (prevFactor in BitSets.toIter(factorsAdded)) {
                    val factorWeight: IntArray = requireNonNull(factorWeights, "factorWeights").get(prevFactor)
                    if (factorWeight[factor] > dimWeight) {
                        dimWeight = factorWeight[factor]
                    }
                }

                // only compute the join cardinality if we know that
                // this factor joins with some part of the current join
                // tree and is potentially better than other factors
                // already considered
                var cardinality: Double? = null
                if (dimWeight > 0
                    && (dimWeight > bestWeight || dimWeight == bestWeight)
                ) {
                    cardinality = computeJoinCardinality(
                        mq,
                        multiJoin,
                        semiJoinOpt,
                        requireNonNull(joinTree, "joinTree"),
                        filtersToAdd,
                        factor
                    )
                }

                // if two factors have the same weight, pick the one
                // with the higher cardinality join key, relative to
                // the join being considered
                if (dimWeight > bestWeight
                    || (dimWeight == bestWeight
                            && (bestCardinality == null
                            || (cardinality != null
                            && cardinality > bestCardinality)))
                ) {
                    nextFactor = factor
                    bestWeight = dimWeight
                    bestCardinality = cardinality
                }
            }
            return nextFactor
        }

        /**
         * Returns whether a RelNode corresponds to a Join that wasn't one of the
         * original MultiJoin input factors.
         */
        private fun isJoinTree(rel: RelNode?): Boolean {
            // full outer joins were already optimized in a prior instantiation
            // of this rule; therefore we should never see a join input that's
            // a full outer join
            return if (rel is Join) {
                assert((rel as Join?).getJoinType() !== JoinRelType.FULL)
                true
            } else {
                false
            }
        }

        /**
         * Adds a new factor into the current join tree. The factor is either pushed
         * down into one of the subtrees of the join recursively, or it is added to
         * the top of the current tree, whichever yields a better ordering.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins for each factor
         * @param joinTree current join tree
         * @param factorToAdd new factor to be added
         * @param factorsNeeded factors that must precede the factor to be added
         * @param filtersToAdd filters remaining to be added; filters added to the
         * new join tree are removed from the list
         * @param selfJoin true if the join being created is a self-join that's
         * removable
         *
         * @return optimal join tree with the new factor added if it is possible to
         * add the factor; otherwise, null is returned
         */
        @Nullable
        private fun addFactorToTree(
            mq: RelMetadataQuery,
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            @Nullable joinTree: LoptJoinTree?,
            factorToAdd: Int,
            factorsNeeded: BitSet,
            filtersToAdd: List<RexNode>,
            selfJoin: Boolean
        ): LoptJoinTree? {

            // if the factor corresponds to the null generating factor in an outer
            // join that can be removed, then create a replacement join
            if (multiJoin.isRemovableOuterJoinFactor(factorToAdd)) {
                return createReplacementJoin(
                    relBuilder,
                    multiJoin,
                    semiJoinOpt,
                    requireNonNull(joinTree, "joinTree"),
                    -1,
                    factorToAdd,
                    ImmutableIntList.of(),
                    null,
                    filtersToAdd
                )
            }

            // if the factor corresponds to a dimension table whose join we
            // can remove, create a replacement join if the corresponding fact
            // table is in the current join tree
            if (multiJoin.getJoinRemovalFactor(factorToAdd) != null) {
                return createReplacementSemiJoin(
                    relBuilder,
                    multiJoin,
                    semiJoinOpt,
                    joinTree,
                    factorToAdd,
                    filtersToAdd
                )
            }

            // if this is the first factor in the tree, create a join tree with
            // the single factor
            if (joinTree == null) {
                return LoptJoinTree(
                    semiJoinOpt.getChosenSemiJoin(factorToAdd),
                    factorToAdd
                )
            }

            // Create a temporary copy of the filter list as we may need the
            // original list to pass into addToTop().  However, if no tree was
            // created by addToTop() because the factor being added is part of
            // a self-join, then pass the original filter list so the added
            // filters will still be removed from the list.
            val tmpFilters: List<RexNode> = ArrayList(filtersToAdd)
            val topTree: LoptJoinTree? = addToTop(
                mq,
                relBuilder,
                multiJoin,
                semiJoinOpt,
                joinTree,
                factorToAdd,
                filtersToAdd,
                selfJoin
            )
            val pushDownTree: LoptJoinTree? = pushDownFactor(
                mq,
                relBuilder,
                multiJoin,
                semiJoinOpt,
                joinTree,
                factorToAdd,
                factorsNeeded,
                if (topTree == null) filtersToAdd else tmpFilters,
                selfJoin
            )

            // pick the lower cost option, and replace the join ordering with
            // the ordering associated with the best option
            val bestTree: LoptJoinTree
            var costPushDown: RelOptCost? = null
            var costTop: RelOptCost? = null
            if (pushDownTree != null) {
                costPushDown = mq.getCumulativeCost(pushDownTree.getJoinTree())
            }
            if (topTree != null) {
                costTop = mq.getCumulativeCost(topTree.getJoinTree())
            }
            bestTree = if (pushDownTree == null) {
                topTree
            } else if (topTree == null) {
                pushDownTree
            } else {
                requireNonNull(costPushDown, "costPushDown")
                requireNonNull(costTop, "costTop")
                if (costPushDown.isEqWithEpsilon(costTop)) {
                    // if both plans cost the same (with an allowable round-off
                    // margin of error), favor the one that passes
                    // around the wider rows further up in the tree
                    if (rowWidthCost(pushDownTree.getJoinTree())
                        < rowWidthCost(topTree.getJoinTree())
                    ) {
                        pushDownTree
                    } else {
                        topTree
                    }
                } else if (costPushDown.isLt(costTop)) {
                    pushDownTree
                } else {
                    topTree
                }
            }
            return bestTree
        }

        /**
         * Computes a cost for a join tree based on the row widths of the inputs
         * into the join. Joins where the inputs have the fewest number of columns
         * lower in the tree are better than equivalent joins where the inputs with
         * the larger number of columns are lower in the tree.
         *
         * @param tree a tree of RelNodes
         *
         * @return the cost associated with the width of the tree
         */
        private fun rowWidthCost(tree: RelNode?): Int {
            // The width cost is the width of the tree itself plus the widths
            // of its children.  Hence, skinnier rows are better when they're
            // lower in the tree since the width of a RelNode contributes to
            // the cost of each LogicalJoin that appears above that RelNode.
            var width: Int = tree.getRowType().getFieldCount()
            if (isJoinTree(tree)) {
                val joinRel: Join? = tree as Join?
                width += (rowWidthCost(joinRel.getLeft())
                        + rowWidthCost(joinRel.getRight()))
            }
            return width
        }

        /**
         * Creates a join tree where the new factor is pushed down one of the
         * operands of the current join tree.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins for each factor
         * @param joinTree current join tree
         * @param factorToAdd new factor to be added
         * @param factorsNeeded factors that must precede the factor to be added
         * @param filtersToAdd filters remaining to be added; filters that are added
         * to the join tree are removed from the list
         * @param selfJoin true if the factor being added is part of a removable
         * self-join
         *
         * @return optimal join tree with the new factor pushed down the current
         * join tree if it is possible to do the pushdown; otherwise, null is
         * returned
         */
        @Nullable
        private fun pushDownFactor(
            mq: RelMetadataQuery,
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            joinTree: LoptJoinTree,
            factorToAdd: Int,
            factorsNeeded: BitSet,
            filtersToAdd: List<RexNode>,
            selfJoin: Boolean
        ): LoptJoinTree? {
            // pushdown option only works if we already have a join tree
            if (!isJoinTree(joinTree.getJoinTree())) {
                return null
            }
            var childNo = -1
            var left: LoptJoinTree? = joinTree.getLeft()
            var right: LoptJoinTree? = joinTree.getRight()
            val joinRel: Join = joinTree.getJoinTree() as Join
            val joinType: JoinRelType = joinRel.getJoinType()

            // can't push factors pass self-joins because in order to later remove
            // them, we need to keep the factors together
            if (joinTree.isRemovableSelfJoin()) {
                return null
            }

            // If there are no constraints as to which side the factor must
            // be pushed, arbitrarily push to the left.  In the case of a
            // self-join, always push to the input that contains the other
            // half of the self-join.
            if (selfJoin) {
                val selfJoinFactor = BitSet(multiJoin.getNumJoinFactors())
                val factor: Integer = requireNonNull(
                    multiJoin.getOtherSelfJoinFactor(factorToAdd)
                ) { "multiJoin.getOtherSelfJoinFactor($factorToAdd) is null" }
                selfJoinFactor.set(factor)
                childNo = if (multiJoin.hasAllFactors(left, selfJoinFactor)) {
                    0
                } else {
                    assert(multiJoin.hasAllFactors(right, selfJoinFactor))
                    1
                }
            } else if (factorsNeeded.cardinality() === 0
                && !joinType.generatesNullsOnLeft()
            ) {
                childNo = 0
            } else {
                // push to the left if the LHS contains all factors that the
                // current factor needs and that side is not null-generating;
                // same check for RHS
                if (multiJoin.hasAllFactors(left, factorsNeeded)
                    && !joinType.generatesNullsOnLeft()
                ) {
                    childNo = 0
                } else if (multiJoin.hasAllFactors(right, factorsNeeded)
                    && !joinType.generatesNullsOnRight()
                ) {
                    childNo = 1
                }
                // if it couldn't be pushed down to either side, then it can
                // only be put on top
            }
            if (childNo == -1) {
                return null
            }

            // remember the original join order before the pushdown so we can
            // appropriately adjust any filters already attached to the join
            // node
            val origJoinOrder: List<Integer> = joinTree.getTreeOrder()

            // recursively pushdown the factor
            var subTree: LoptJoinTree? = if (childNo == 0) left else right
            subTree = addFactorToTree(
                mq,
                relBuilder,
                multiJoin,
                semiJoinOpt,
                subTree,
                factorToAdd,
                factorsNeeded,
                filtersToAdd,
                selfJoin
            )
            if (childNo == 0) {
                left = subTree
            } else {
                right = subTree
            }

            // adjust the join condition from the original join tree to reflect
            // pushdown of the new factor as well as any swapping that may have
            // been done during the pushdown
            var newCondition: RexNode = (joinTree.getJoinTree() as Join).getCondition()
            newCondition = adjustFilter(
                multiJoin,
                requireNonNull(left, "left"),
                requireNonNull(right, "right"),
                newCondition,
                factorToAdd,
                origJoinOrder,
                joinTree.getJoinTree().getRowType().getFieldList()
            )

            // determine if additional filters apply as a result of adding the
            // new factor, provided this isn't a left or right outer join; for
            // those cases, the additional filters will be added on top of the
            // join in createJoinSubtree
            if (joinType !== JoinRelType.LEFT && joinType !== JoinRelType.RIGHT) {
                val condition: RexNode? = addFilters(
                    multiJoin,
                    left,
                    -1,
                    right,
                    filtersToAdd,
                    true
                )
                val rexBuilder: RexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder()
                newCondition = RelOptUtil.andJoinFilters(
                    rexBuilder,
                    newCondition,
                    condition
                )
            }

            // create the new join tree with the factor pushed down
            return createJoinSubtree(
                mq,
                relBuilder,
                multiJoin,
                left,
                right,
                newCondition,
                joinType,
                filtersToAdd,
                false,
                false
            )
        }

        /**
         * Creates a join tree with the new factor added to the top of the tree.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins for each factor
         * @param joinTree current join tree
         * @param factorToAdd new factor to be added
         * @param filtersToAdd filters remaining to be added; modifies the list to
         * remove filters that can be added to the join tree
         * @param selfJoin true if the join being created is a self-join that's
         * removable
         *
         * @return new join tree
         */
        @Nullable
        private fun addToTop(
            mq: RelMetadataQuery,
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            joinTree: LoptJoinTree,
            factorToAdd: Int,
            filtersToAdd: List<RexNode>,
            selfJoin: Boolean
        ): LoptJoinTree? {
            // self-joins can never be created at the top of an existing
            // join tree because it needs to be paired directly with the
            // other self-join factor
            if (selfJoin && isJoinTree(joinTree.getJoinTree())) {
                return null
            }

            // if the factor being added is null-generating, create the join
            // as a left outer join since it's being added to the RHS side of
            // the join; createJoinSubTree may swap the inputs and therefore
            // convert the left outer join to a right outer join; if the original
            // MultiJoin was a full outer join, these should be the only
            // factors in the join, so create the join as a full outer join
            val joinType: JoinRelType
            joinType = if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
                assert(multiJoin.getNumJoinFactors() === 2)
                JoinRelType.FULL
            } else if (multiJoin.isNullGenerating(factorToAdd)) {
                JoinRelType.LEFT
            } else {
                JoinRelType.INNER
            }
            val rightTree = LoptJoinTree(
                semiJoinOpt.getChosenSemiJoin(factorToAdd),
                factorToAdd
            )

            // in the case of a left or right outer join, use the specific
            // outer join condition
            val condition: RexNode?
            if (joinType === JoinRelType.LEFT || joinType === JoinRelType.RIGHT) {
                condition = requireNonNull(
                    multiJoin.getOuterJoinCond(factorToAdd),
                    "multiJoin.getOuterJoinCond(factorToAdd)"
                )
            } else {
                condition = addFilters(
                    multiJoin,
                    joinTree,
                    -1,
                    rightTree,
                    filtersToAdd,
                    false
                )
            }
            return createJoinSubtree(
                mq,
                relBuilder,
                multiJoin,
                joinTree,
                rightTree,
                condition,
                joinType,
                filtersToAdd,
                true,
                selfJoin
            )
        }

        /**
         * Determines which join filters can be added to the current join tree. Note
         * that the join filter still reflects the original join ordering. It will
         * only be adjusted to reflect the new join ordering if the "adjust"
         * parameter is set to true.
         *
         * @param multiJoin join factors being optimized
         * @param leftTree left subtree of the join tree
         * @param leftIdx if  0, only consider filters that reference leftIdx in
         * leftTree; otherwise, consider all filters that reference any factor in
         * leftTree
         * @param rightTree right subtree of the join tree
         * @param filtersToAdd remaining join filters that need to be added; those
         * that are added are removed from the list
         * @param adjust if true, adjust filter to reflect new join ordering
         *
         * @return AND'd expression of the join filters that can be added to the
         * current join tree
         */
        private fun addFilters(
            multiJoin: LoptMultiJoin,
            leftTree: LoptJoinTree?,
            leftIdx: Int,
            rightTree: LoptJoinTree?,
            filtersToAdd: List<RexNode>,
            adjust: Boolean
        ): RexNode? {
            // loop through the remaining filters to be added and pick out the
            // ones that reference only the factors in the new join tree
            val rexBuilder: RexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder()
            val childFactorBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            childFactorBuilder.addAll(rightTree!!.getTreeOrder())
            if (leftIdx >= 0) {
                childFactorBuilder.set(leftIdx)
            } else {
                childFactorBuilder.addAll(leftTree!!.getTreeOrder())
            }
            for (child in rightTree!!.getTreeOrder()) {
                childFactorBuilder.set(child)
            }
            val childFactor: ImmutableBitSet = childFactorBuilder.build()
            var condition: RexNode? = null
            val filterIter: ListIterator<RexNode> = filtersToAdd.listIterator()
            while (filterIter.hasNext()) {
                val joinFilter: RexNode = filterIter.next()
                val filterBitmap: ImmutableBitSet = multiJoin.getFactorsRefByJoinFilter(joinFilter)

                // if all factors in the join filter are in the join tree,
                // AND the filter to the current join condition
                if (childFactor.contains(filterBitmap)) {
                    condition = if (condition == null) {
                        joinFilter
                    } else {
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.AND,
                            condition,
                            joinFilter
                        )
                    }
                    filterIter.remove()
                }
            }
            if (adjust && condition != null) {
                val adjustments = IntArray(multiJoin.getNumTotalFields())
                if (needsAdjustment(
                        multiJoin,
                        adjustments,
                        leftTree,
                        rightTree,
                        false
                    )
                ) {
                    condition = condition.accept(
                        RexInputConverter(
                            rexBuilder,
                            multiJoin.getMultiJoinFields(),
                            leftTree!!.getJoinTree().getRowType().getFieldList(),
                            rightTree!!.getJoinTree().getRowType().getFieldList(),
                            adjustments
                        )
                    )
                }
            }
            if (condition == null) {
                condition = rexBuilder.makeLiteral(true)
            }
            return condition
        }

        /**
         * Adjusts a filter to reflect a newly added factor in the middle of an
         * existing join tree.
         *
         * @param multiJoin join factors being optimized
         * @param left left subtree of the join
         * @param right right subtree of the join
         * @param condition current join condition
         * @param factorAdded index corresponding to the newly added factor
         * @param origJoinOrder original join order, before factor was pushed into
         * the tree
         * @param origFields fields from the original join before the factor was
         * added
         *
         * @return modified join condition reflecting addition of the new factor
         */
        private fun adjustFilter(
            multiJoin: LoptMultiJoin,
            left: LoptJoinTree,
            right: LoptJoinTree,
            condition: RexNode,
            factorAdded: Int,
            origJoinOrder: List<Integer>,
            origFields: List<RelDataTypeField>
        ): RexNode {
            var condition: RexNode = condition
            val newJoinOrder: List<Integer> = ArrayList()
            left.getTreeOrder(newJoinOrder)
            right.getTreeOrder(newJoinOrder)
            val totalFields: Int = (left.getJoinTree().getRowType().getFieldCount()
                    + right.getJoinTree().getRowType().getFieldCount()
                    - multiJoin.getNumFieldsInJoinFactor(factorAdded))
            val adjustments = IntArray(totalFields)

            // go through each factor and adjust relative to the original
            // join order
            var needAdjust = false
            var nFieldsNew = 0
            for (newPos in 0 until newJoinOrder.size()) {
                var nFieldsOld = 0

                // no need to make any adjustments on the newly added factor
                val factor: Int = newJoinOrder[newPos]
                if (factor != factorAdded) {
                    // locate the position of the factor in the original join
                    // ordering
                    for (pos in origJoinOrder) {
                        if (factor == pos) {
                            break
                        }
                        nFieldsOld += multiJoin.getNumFieldsInJoinFactor(pos)
                    }

                    // fill in the adjustment array for this factor
                    if (remapJoinReferences(
                            multiJoin,
                            factor,
                            newJoinOrder,
                            newPos,
                            adjustments,
                            nFieldsOld,
                            nFieldsNew,
                            false
                        )
                    ) {
                        needAdjust = true
                    }
                }
                nFieldsNew += multiJoin.getNumFieldsInJoinFactor(factor)
            }
            if (needAdjust) {
                val rexBuilder: RexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder()
                condition = condition.accept(
                    RexInputConverter(
                        rexBuilder,
                        origFields,
                        left.getJoinTree().getRowType().getFieldList(),
                        right.getJoinTree().getRowType().getFieldList(),
                        adjustments
                    )
                )
            }
            return condition
        }

        /**
         * Sets an adjustment array based on where column references for a
         * particular factor end up as a result of a new join ordering.
         *
         *
         * If the factor is not the right factor in a removable self-join, then
         * it needs to be adjusted as follows:
         *
         *
         *  * First subtract, based on where the factor was in the original join
         * ordering.
         *  * Then add on the number of fields in the factors that now precede this
         * factor in the new join ordering.
         *
         *
         *
         * If the factor is the right factor in a removable self-join and its
         * column reference can be mapped to the left factor in the self-join, then:
         *
         *
         *  * First subtract, based on where the column reference is in the new
         * join ordering.
         *  * Then, add on the number of fields up to the start of the left factor
         * in the self-join in the new join ordering.
         *  * Then, finally add on the offset of the corresponding column from the
         * left factor.
         *
         *
         *
         * Note that this only applies if both factors in the self-join are in the
         * join ordering. If they are, then the left factor always precedes the
         * right factor in the join ordering.
         *
         * @param multiJoin join factors being optimized
         * @param factor the factor whose references are being adjusted
         * @param newJoinOrder the new join ordering containing the factor
         * @param newPos the position of the factor in the new join ordering
         * @param adjustments the adjustments array that will be set
         * @param offset the starting offset within the original join ordering for
         * the columns of the factor being adjusted
         * @param newOffset the new starting offset in the new join ordering for the
         * columns of the factor being adjusted
         * @param alwaysUseDefault always use the default adjustment value
         * regardless of whether the factor is the right factor in a removable
         * self-join
         *
         * @return true if at least one column from the factor requires adjustment
         */
        private fun remapJoinReferences(
            multiJoin: LoptMultiJoin,
            factor: Int,
            newJoinOrder: List<Integer>,
            newPos: Int,
            adjustments: IntArray,
            offset: Int,
            newOffset: Int,
            alwaysUseDefault: Boolean
        ): Boolean {
            var needAdjust = false
            val defaultAdjustment = -offset + newOffset
            if (!alwaysUseDefault
                && multiJoin.isRightFactorInRemovableSelfJoin(factor)
                && newPos != 0
                && newJoinOrder[newPos - 1].equals(
                    multiJoin.getOtherSelfJoinFactor(factor)
                )
            ) {
                val nLeftFields: Int = multiJoin.getNumFieldsInJoinFactor(
                    newJoinOrder[newPos - 1]
                )
                for (i in 0 until multiJoin.getNumFieldsInJoinFactor(factor)) {
                    val leftOffset: Integer = multiJoin.getRightColumnMapping(factor, i)

                    // if the left factor doesn't reference the column, then
                    // use the default adjustment value
                    if (leftOffset == null) {
                        adjustments[i + offset] = defaultAdjustment
                    } else {
                        adjustments[i + offset] = (-(offset + i) + (newOffset - nLeftFields)
                                + leftOffset)
                    }
                    if (adjustments[i + offset] != 0) {
                        needAdjust = true
                    }
                }
            } else {
                if (defaultAdjustment != 0) {
                    needAdjust = true
                    for (i in 0 until multiJoin.getNumFieldsInJoinFactor(
                        newJoinOrder[newPos]
                    )) {
                        adjustments[i + offset] = defaultAdjustment
                    }
                }
            }
            return needAdjust
        }

        /**
         * In the event that a dimension table does not need to be joined because of
         * a semijoin, this method creates a join tree that consists of a projection
         * on top of an existing join tree. The existing join tree must contain the
         * fact table in the semijoin that allows the dimension table to be removed.
         *
         *
         * The projection created on top of the join tree mimics a join of the
         * fact and dimension tables. In order for the dimension table to have been
         * removed, the only fields referenced from the dimension table are its
         * dimension keys. Therefore, we can replace these dimension fields with the
         * fields corresponding to the semijoin keys from the fact table in the
         * projection.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins for each factor
         * @param factTree existing join tree containing the fact table
         * @param dimIdx dimension table factor id
         * @param filtersToAdd filters remaining to be added; filters added to the
         * new join tree are removed from the list
         *
         * @return created join tree or null if the corresponding fact table has not
         * been joined in yet
         */
        @Nullable
        private fun createReplacementSemiJoin(
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            @Nullable factTree: LoptJoinTree?,
            dimIdx: Int,
            filtersToAdd: List<RexNode>
        ): LoptJoinTree? {
            // if the current join tree doesn't contain the fact table, then
            // don't bother trying to create the replacement join just yet
            if (factTree == null) {
                return null
            }
            val factIdx: Int = requireNonNull(
                multiJoin.getJoinRemovalFactor(dimIdx)
            ) { "multiJoin.getJoinRemovalFactor(dimIdx) for $dimIdx, $multiJoin" }
            val joinOrder: List<Integer> = factTree.getTreeOrder()
            assert(joinOrder.contains(factIdx))

            // figure out the position of the fact table in the current jointree
            var adjustment = 0
            for (factor in joinOrder) {
                if (factor == factIdx) {
                    break
                }
                adjustment += multiJoin.getNumFieldsInJoinFactor(factor)
            }

            // map the dimension keys to the corresponding keys from the fact
            // table, based on the fact table's position in the current jointree
            val dimFields: List<RelDataTypeField> = multiJoin.getJoinFactor(dimIdx).getRowType().getFieldList()
            val nDimFields: Int = dimFields.size()
            val replacementKeys: Array<Integer?> = arrayOfNulls<Integer>(nDimFields)
            val semiJoin: LogicalJoin = multiJoin.getJoinRemovalSemiJoin(dimIdx)
            val dimKeys: ImmutableIntList = semiJoin.analyzeCondition().leftKeys
            val factKeys: ImmutableIntList = semiJoin.analyzeCondition().rightKeys
            for (i in 0 until dimKeys.size()) {
                replacementKeys[dimKeys.get(i)] = factKeys.get(i) + adjustment
            }
            return createReplacementJoin(
                relBuilder,
                multiJoin,
                semiJoinOpt,
                factTree,
                factIdx,
                dimIdx,
                dimKeys,
                replacementKeys,
                filtersToAdd
            )
        }

        /**
         * Creates a replacement join, projecting either dummy columns or
         * replacement keys from the factor that doesn't actually need to be joined.
         *
         * @param multiJoin join factors being optimized
         * @param semiJoinOpt optimal semijoins for each factor
         * @param currJoinTree current join tree being added to
         * @param leftIdx if  0, when creating the replacement join, only consider
         * filters that reference leftIdx in currJoinTree; otherwise, consider all
         * filters that reference any factor in currJoinTree
         * @param factorToAdd new factor whose join can be removed
         * @param newKeys join keys that need to be replaced
         * @param replacementKeys the keys that replace the join keys; null if we're
         * removing the null generating factor in an outer join
         * @param filtersToAdd filters remaining to be added; filters added to the
         * new join tree are removed from the list
         *
         * @return created join tree with an appropriate projection for the factor
         * that can be removed
         */
        private fun createReplacementJoin(
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            semiJoinOpt: LoptSemiJoinOptimizer,
            currJoinTree: LoptJoinTree,
            leftIdx: Int,
            factorToAdd: Int,
            newKeys: ImmutableIntList,
            replacementKeys: @Nullable Array<Integer?>?,
            filtersToAdd: List<RexNode>
        ): LoptJoinTree {
            // create a projection, projecting the fields from the join tree
            // containing the current joinRel and the new factor; for fields
            // corresponding to join keys, replace them with the corresponding key
            // from the replacementKeys passed in; for other fields, just create a
            // null expression as a placeholder for the column; this is done so we
            // don't have to adjust the offsets of other expressions that reference
            // the new factor; the placeholder expression values should never be
            // referenced, so that's why it's ok to create these possibly invalid
            // expressions
            val currJoinRel: RelNode = currJoinTree.getJoinTree()
            val currFields: List<RelDataTypeField> = currJoinRel.getRowType().getFieldList()
            val nCurrFields: Int = currFields.size()
            val newFields: List<RelDataTypeField> = multiJoin.getJoinFactor(factorToAdd).getRowType().getFieldList()
            val nNewFields: Int = newFields.size()
            val projects: List<Pair<RexNode, String>> = ArrayList()
            val rexBuilder: RexBuilder = currJoinRel.getCluster().getRexBuilder()
            val typeFactory: RelDataTypeFactory = rexBuilder.getTypeFactory()
            for (i in 0 until nCurrFields) {
                projects.add(
                    Pair.of(
                        rexBuilder.makeInputRef(currFields[i].getType(), i),
                        currFields[i].getName()
                    )
                )
            }
            for (i in 0 until nNewFields) {
                var projExpr: RexNode
                var newType: RelDataType = newFields[i].getType()
                if (!newKeys.contains(i)) {
                    if (replacementKeys == null) {
                        // null generating factor in an outer join; so make the
                        // type nullable
                        newType = typeFactory.createTypeWithNullability(newType, true)
                    }
                    projExpr = rexBuilder.makeNullLiteral(newType)
                } else {
                    // TODO: is the above if (replacementKeys==null) check placed properly?
                    requireNonNull(replacementKeys, "replacementKeys")
                    val mappedField: RelDataTypeField = currFields[replacementKeys!![i]]
                    val mappedInput: RexNode = rexBuilder.makeInputRef(
                        mappedField.getType(),
                        replacementKeys[i]
                    )

                    // if the types aren't the same, create a cast
                    projExpr = if (mappedField.getType() === newType) {
                        mappedInput
                    } else {
                        rexBuilder.makeCast(
                            newFields[i].getType(),
                            mappedInput
                        )
                    }
                }
                projects.add(Pair.of(projExpr, newFields[i].getName()))
            }
            relBuilder.push(currJoinRel)
            relBuilder.project(Pair.left(projects), Pair.right(projects))

            // remove the join conditions corresponding to the join we're removing;
            // we don't actually need to use them, but we need to remove them
            // from the list since they're no longer needed
            val newTree = LoptJoinTree(
                semiJoinOpt.getChosenSemiJoin(factorToAdd),
                factorToAdd
            )
            addFilters(
                multiJoin,
                currJoinTree,
                leftIdx,
                newTree,
                filtersToAdd,
                false
            )

            // Filters referencing factors other than leftIdx and factorToAdd
            // still do need to be applied.  So, add them into a separate
            // LogicalFilter placed on top off the projection created above.
            if (leftIdx >= 0) {
                addAdditionalFilters(
                    relBuilder,
                    multiJoin,
                    currJoinTree,
                    newTree,
                    filtersToAdd
                )
            }

            // finally, create a join tree consisting of the current join's join
            // tree with the newly created projection; note that in the factor
            // tree, we act as if we're joining in the new factor, even
            // though we really aren't; this is needed so we can map the columns
            // from the new factor as we go up in the join tree
            return LoptJoinTree(
                relBuilder.build(),
                currJoinTree.getFactorTree(),
                newTree.getFactorTree()
            )
        }

        /**
         * Creates a LogicalJoin given left and right operands and a join condition.
         * Swaps the operands if beneficial.
         *
         * @param multiJoin join factors being optimized
         * @param left left operand
         * @param right right operand
         * @param condition join condition
         * @param joinType the join type
         * @param fullAdjust true if the join condition reflects the original join
         * ordering and therefore has not gone through any type of adjustment yet;
         * otherwise, the condition has already been partially adjusted and only
         * needs to be further adjusted if swapping is done
         * @param filtersToAdd additional filters that may be added on top of the
         * resulting LogicalJoin, if the join is a left or right outer join
         * @param selfJoin true if the join being created is a self-join that's
         * removable
         *
         * @return created LogicalJoin
         */
        private fun createJoinSubtree(
            mq: RelMetadataQuery,
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            left: LoptJoinTree?,
            right: LoptJoinTree?,
            condition: RexNode?,
            joinType: JoinRelType,
            filtersToAdd: List<RexNode>,
            fullAdjust: Boolean,
            selfJoin: Boolean
        ): LoptJoinTree {
            var left: LoptJoinTree? = left
            var right: LoptJoinTree? = right
            var condition: RexNode? = condition
            var joinType: JoinRelType = joinType
            val rexBuilder: RexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder()

            // swap the inputs if beneficial
            if (swapInputs(mq, multiJoin, left, right, selfJoin)) {
                val tmp: LoptJoinTree? = right
                right = left
                left = tmp
                if (!fullAdjust) {
                    condition = swapFilter(
                        rexBuilder,
                        multiJoin,
                        right,
                        left,
                        condition
                    )
                }
                if (joinType !== JoinRelType.INNER
                    && joinType !== JoinRelType.FULL
                ) {
                    joinType = if (joinType === JoinRelType.LEFT) JoinRelType.RIGHT else JoinRelType.LEFT
                }
            }
            if (fullAdjust) {
                val adjustments = IntArray(multiJoin.getNumTotalFields())
                if (needsAdjustment(
                        multiJoin,
                        adjustments,
                        left,
                        right,
                        selfJoin
                    )
                ) {
                    condition = condition.accept(
                        RexInputConverter(
                            rexBuilder,
                            multiJoin.getMultiJoinFields(),
                            left!!.getJoinTree().getRowType().getFieldList(),
                            right!!.getJoinTree().getRowType().getFieldList(),
                            adjustments
                        )
                    )
                }
            }
            relBuilder.push(left!!.getJoinTree())
                .push(right!!.getJoinTree())
                .join(joinType, condition)

            // if this is a left or right outer join, and additional filters can
            // be applied to the resulting join, then they need to be applied
            // as a filter on top of the outer join result
            if (joinType === JoinRelType.LEFT || joinType === JoinRelType.RIGHT) {
                assert(!selfJoin)
                addAdditionalFilters(
                    relBuilder,
                    multiJoin,
                    left,
                    right,
                    filtersToAdd
                )
            }
            return LoptJoinTree(
                relBuilder.build(),
                left.getFactorTree(),
                right.getFactorTree(),
                selfJoin
            )
        }

        /**
         * Determines whether any additional filters are applicable to a join tree.
         * If there are any, creates a filter node on top of the join tree with the
         * additional filters.
         *
         * @param relBuilder Builder holding current join tree
         * @param multiJoin join factors being optimized
         * @param left left side of join tree
         * @param right right side of join tree
         * @param filtersToAdd remaining filters
         */
        private fun addAdditionalFilters(
            relBuilder: RelBuilder,
            multiJoin: LoptMultiJoin,
            left: LoptJoinTree?,
            right: LoptJoinTree?,
            filtersToAdd: List<RexNode>
        ) {
            var filterCond: RexNode? = addFilters(multiJoin, left, -1, right, filtersToAdd, false)
            if (!filterCond.isAlwaysTrue()) {
                // adjust the filter to reflect the outer join output
                val adjustments = IntArray(multiJoin.getNumTotalFields())
                if (needsAdjustment(multiJoin, adjustments, left, right, false)) {
                    val rexBuilder: RexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder()
                    filterCond = filterCond.accept(
                        RexInputConverter(
                            rexBuilder,
                            multiJoin.getMultiJoinFields(),
                            relBuilder.peek().getRowType().getFieldList(),
                            adjustments
                        )
                    )
                    relBuilder.filter(filterCond)
                }
            }
        }

        /**
         * Swaps the operands to a join, so the smaller input is on the right. Or,
         * if this is a removable self-join, swap so the factor that should be
         * preserved when the self-join is removed is put on the left.
         *
         * @param multiJoin join factors being optimized
         * @param left left side of join tree
         * @param right right hand side of join tree
         * @param selfJoin true if the join is a removable self-join
         *
         * @return true if swapping should be done
         */
        private fun swapInputs(
            mq: RelMetadataQuery,
            multiJoin: LoptMultiJoin,
            left: LoptJoinTree?,
            right: LoptJoinTree?,
            selfJoin: Boolean
        ): Boolean {
            var swap = false
            if (selfJoin) {
                return !multiJoin.isLeftFactorInRemovableSelfJoin(
                    (left.getFactorTree() as LoptJoinTree.Leaf).getId()
                )
            }
            val leftRowCount: Double = mq.getRowCount(left!!.getJoinTree())
            val rightRowCount: Double = mq.getRowCount(right!!.getJoinTree())

            // The left side is smaller than the right if it has fewer rows,
            // or if it has the same number of rows as the right (excluding
            // roundoff), but fewer columns.
            if (leftRowCount != null
                && rightRowCount != null
                && (leftRowCount < rightRowCount
                        || (Math.abs(leftRowCount - rightRowCount) < RelOptUtil.EPSILON
                        && rowWidthCost(left!!.getJoinTree()) < rowWidthCost(
                    right!!.getJoinTree()
                )))
            ) {
                swap = true
            }
            return swap
        }

        /**
         * Adjusts a filter to reflect swapping of join inputs.
         *
         * @param rexBuilder rexBuilder
         * @param multiJoin join factors being optimized
         * @param origLeft original LHS of the join tree (before swap)
         * @param origRight original RHS of the join tree (before swap)
         * @param condition original join condition
         *
         * @return join condition reflect swap of join inputs
         */
        private fun swapFilter(
            rexBuilder: RexBuilder,
            multiJoin: LoptMultiJoin,
            origLeft: LoptJoinTree?,
            origRight: LoptJoinTree?,
            condition: RexNode?
        ): RexNode? {
            var condition: RexNode? = condition
            val nFieldsOnLeft: Int = origLeft!!.getJoinTree().getRowType().getFieldCount()
            val nFieldsOnRight: Int = origRight!!.getJoinTree().getRowType().getFieldCount()
            val adjustments = IntArray(nFieldsOnLeft + nFieldsOnRight)
            for (i in 0 until nFieldsOnLeft) {
                adjustments[i] = nFieldsOnRight
            }
            for (i in nFieldsOnLeft until nFieldsOnLeft + nFieldsOnRight) {
                adjustments[i] = -nFieldsOnLeft
            }
            condition = condition.accept(
                RexInputConverter(
                    rexBuilder,
                    multiJoin.getJoinFields(origLeft, origRight),
                    multiJoin.getJoinFields(origRight, origLeft),
                    adjustments
                )
            )
            return condition
        }

        /**
         * Sets an array indicating how much each factor in a join tree needs to be
         * adjusted to reflect the tree's join ordering.
         *
         * @param multiJoin join factors being optimized
         * @param adjustments array to be filled out
         * @param joinTree join tree
         * @param otherTree null unless joinTree only represents the left side of
         * the join tree
         * @param selfJoin true if no adjustments need to be made for self-joins
         *
         * @return true if some adjustment is required; false otherwise
         */
        private fun needsAdjustment(
            multiJoin: LoptMultiJoin,
            adjustments: IntArray,
            joinTree: LoptJoinTree?,
            otherTree: LoptJoinTree?,
            selfJoin: Boolean
        ): Boolean {
            var needAdjustment = false
            val joinOrder: List<Integer> = ArrayList()
            joinTree!!.getTreeOrder(joinOrder)
            if (otherTree != null) {
                otherTree.getTreeOrder(joinOrder)
            }
            var nFields = 0
            for (newPos in 0 until joinOrder.size()) {
                val origPos: Int = joinOrder[newPos]
                val joinStart: Int = multiJoin.getJoinStart(origPos)

                // Determine the adjustments needed for join references.  Note
                // that if the adjustment is being done for a self-join filter,
                // we always use the default adjustment value rather than
                // remapping the right factor to reference the left factor.
                // Otherwise, we have no way of later identifying that the join is
                // self-join.
                if (remapJoinReferences(
                        multiJoin,
                        origPos,
                        joinOrder,
                        newPos,
                        adjustments,
                        joinStart,
                        nFields,
                        selfJoin
                    )
                ) {
                    needAdjustment = true
                }
                nFields += multiJoin.getNumFieldsInJoinFactor(origPos)
            }
            return needAdjustment
        }

        /**
         * Determines whether a join is a removable self-join. It is if it's an
         * inner join between identical, simple factors and the equality portion of
         * the join condition consists of the same set of unique keys.
         *
         * @param joinRel the join
         *
         * @return true if the join is removable
         */
        fun isRemovableSelfJoin(joinRel: Join): Boolean {
            val left: RelNode = joinRel.getLeft()
            val right: RelNode = joinRel.getRight()
            if (joinRel.getJoinType().isOuterJoin()) {
                return false
            }

            // Make sure the join is between the same simple factor
            val mq: RelMetadataQuery = joinRel.getCluster().getMetadataQuery()
            val leftTable: RelOptTable = mq.getTableOrigin(left) ?: return false
            val rightTable: RelOptTable = mq.getTableOrigin(right) ?: return false
            return if (!leftTable.getQualifiedName().equals(rightTable.getQualifiedName())) {
                false
            } else areSelfJoinKeysUnique(
                mq,
                left,
                right,
                joinRel.getCondition()
            )

            // Determine if the join keys are identical and unique
        }

        /**
         * Determines if the equality portion of a self-join condition is between
         * identical keys that are unique.
         *
         * @param mq Metadata query
         * @param leftRel left side of the join
         * @param rightRel right side of the join
         * @param joinFilters the join condition
         *
         * @return true if the equality join keys are the same and unique
         */
        private fun areSelfJoinKeysUnique(
            mq: RelMetadataQuery,
            leftRel: RelNode, rightRel: RelNode, joinFilters: RexNode
        ): Boolean {
            val joinInfo: JoinInfo = JoinInfo.of(leftRel, rightRel, joinFilters)

            // Make sure each key on the left maps to the same simple column as the
            // corresponding key on the right
            for (pair in joinInfo.pairs()) {
                val leftOrigin: RelColumnOrigin = mq.getColumnOrigin(leftRel, pair.source)
                if (leftOrigin == null || !leftOrigin.isDerived()) {
                    return false
                }
                val rightOrigin: RelColumnOrigin = mq.getColumnOrigin(rightRel, pair.target)
                if (rightOrigin == null || !rightOrigin.isDerived()) {
                    return false
                }
                if (leftOrigin.getOriginColumnOrdinal()
                    !== rightOrigin.getOriginColumnOrdinal()
                ) {
                    return false
                }
            }

            // Now that we've verified that the keys are the same, see if they
            // are unique.  When removing self-joins, if needed, we'll later add an
            // IS NOT NULL filter on the join keys that are nullable.  Therefore,
            // it's ok if there are nulls in the unique key.
            return RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                mq, leftRel,
                joinInfo.leftSet()
            )
        }
    }
}
