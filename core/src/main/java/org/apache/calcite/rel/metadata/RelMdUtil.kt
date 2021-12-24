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
 * RelMdUtil provides utility methods used by the metadata provider methods.
 */
object RelMdUtil {
    //~ Static fields/initializers ---------------------------------------------
    val ARTIFICIAL_SELECTIVITY_FUNC: SqlFunction = SqlFunction(
        "ARTIFICIAL_SELECTIVITY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,  // returns boolean since we'll AND it
        null,
        OperandTypes.NUMERIC,  // takes a numeric param
        SqlFunctionCategory.SYSTEM
    )

    /**
     * Creates a RexNode that stores a selectivity value corresponding to the
     * selectivity of a semijoin. This can be added to a filter to simulate the
     * effect of the semijoin during costing, but should never appear in a real
     * plan since it has no physical implementation.
     *
     * @param rel the semijoin of interest
     * @return constructed rexnode
     */
    fun makeSemiJoinSelectivityRexNode(mq: RelMetadataQuery, rel: Join): RexNode {
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        val selectivity = computeSemiJoinSelectivity(mq, rel.getLeft(), rel.getRight(), rel)
        return rexBuilder.makeCall(
            ARTIFICIAL_SELECTIVITY_FUNC,
            rexBuilder.makeApproxLiteral(BigDecimal(selectivity))
        )
    }

    /**
     * Returns the selectivity value stored in a call.
     *
     * @param artificialSelectivityFuncNode Call containing the selectivity value
     * @return selectivity value
     */
    fun getSelectivityValue(
        artificialSelectivityFuncNode: RexNode
    ): Double {
        assert(artificialSelectivityFuncNode is RexCall)
        val call: RexCall = artificialSelectivityFuncNode as RexCall
        assert(call.getOperator() === ARTIFICIAL_SELECTIVITY_FUNC)
        val operand: RexNode = call.getOperands().get(0)
        return (operand as RexLiteral).getValueAs(
            Double::class.java
        )
    }

    /**
     * Computes the selectivity of a semijoin filter if it is applied on a fact
     * table. The computation is based on the selectivity of the dimension
     * table/columns and the number of distinct values in the fact table
     * columns.
     *
     * @param factRel fact table participating in the semijoin
     * @param dimRel  dimension table participating in the semijoin
     * @param rel     semijoin rel
     * @return calculated selectivity
     */
    fun computeSemiJoinSelectivity(
        mq: RelMetadataQuery,
        factRel: RelNode?, dimRel: RelNode?, rel: Join
    ): Double {
        return computeSemiJoinSelectivity(
            mq, factRel, dimRel, rel.analyzeCondition().leftKeys,
            rel.analyzeCondition().rightKeys
        )
    }

    /**
     * Computes the selectivity of a semijoin filter if it is applied on a fact
     * table. The computation is based on the selectivity of the dimension
     * table/columns and the number of distinct values in the fact table
     * columns.
     *
     * @param factRel     fact table participating in the semijoin
     * @param dimRel      dimension table participating in the semijoin
     * @param factKeyList LHS keys used in the filter
     * @param dimKeyList  RHS keys used in the filter
     * @return calculated selectivity
     */
    fun computeSemiJoinSelectivity(
        mq: RelMetadataQuery,
        factRel: RelNode?, dimRel: RelNode?, factKeyList: List<Integer?>,
        dimKeyList: List<Integer?>
    ): Double {
        val factKeys: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (factCol in factKeyList) {
            factKeys.set(factCol)
        }
        val dimKeyBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (dimCol in dimKeyList) {
            dimKeyBuilder.set(dimCol)
        }
        val dimKeys: ImmutableBitSet = dimKeyBuilder.build()
        var factPop: Double = mq.getPopulationSize(factRel, factKeys.build())
        if (factPop == null) {
            // use the dimension population if the fact population is
            // unavailable; since we're filtering the fact table, that's
            // the population we ideally want to use
            factPop = mq.getPopulationSize(dimRel, dimKeys)
        }

        // if cardinality and population are available, use them; otherwise
        // use percentage original rows
        var selectivity: Double
        val dimCard: Double = mq.getDistinctRowCount(
            dimRel,
            dimKeys,
            null
        )
        if (dimCard != null && factPop != null) {
            // to avoid division by zero
            if (factPop < 1.0) {
                factPop = 1.0
            }
            selectivity = dimCard / factPop
        } else {
            selectivity = mq.getPercentageOriginalRows(dimRel)
        }
        if (selectivity == null) {
            // set a default selectivity based on the number of semijoin keys
            selectivity = Math.pow(
                0.1,
                dimKeys.cardinality()
            )
        } else if (selectivity > 1.0) {
            selectivity = 1.0
        }
        return selectivity
    }

    /**
     * Returns true if the columns represented in a bit mask are definitely
     * known to form a unique column set.
     *
     * @param rel     the relational expression that the column mask corresponds
     * to
     * @param colMask bit mask containing columns that will be tested for
     * uniqueness
     * @return true if bit mask represents a unique column set; false if not (or
     * if no metadata is available)
     */
    fun areColumnsDefinitelyUnique(
        mq: RelMetadataQuery,
        rel: RelNode?, colMask: ImmutableBitSet?
    ): Boolean {
        val b: Boolean = mq.areColumnsUnique(rel, colMask, false)
        return b != null && b
    }

    @Nullable
    fun areColumnsUnique(
        mq: RelMetadataQuery, rel: RelNode?,
        columnRefs: List<RexInputRef?>
    ): Boolean {
        val colMask: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (columnRef in columnRefs) {
            colMask.set(columnRef.getIndex())
        }
        return mq.areColumnsUnique(rel, colMask.build())
    }

    fun areColumnsDefinitelyUnique(
        mq: RelMetadataQuery,
        rel: RelNode?, columnRefs: List<RexInputRef?>
    ): Boolean {
        val b = areColumnsUnique(mq, rel, columnRefs)
        return b != null && b
    }

    /**
     * Returns true if the columns represented in a bit mask are definitely
     * known to form a unique column set, when nulls have been filtered from
     * the columns.
     *
     * @param rel     the relational expression that the column mask corresponds
     * to
     * @param colMask bit mask containing columns that will be tested for
     * uniqueness
     * @return true if bit mask represents a unique column set; false if not (or
     * if no metadata is available)
     */
    fun areColumnsDefinitelyUniqueWhenNullsFiltered(
        mq: RelMetadataQuery, rel: RelNode?, colMask: ImmutableBitSet?
    ): Boolean {
        val b: Boolean = mq.areColumnsUnique(rel, colMask, true)
        return b != null && b
    }

    @Nullable
    fun areColumnsUniqueWhenNullsFiltered(
        mq: RelMetadataQuery,
        rel: RelNode?, columnRefs: List<RexInputRef?>
    ): Boolean {
        val colMask: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (columnRef in columnRefs) {
            colMask.set(columnRef.getIndex())
        }
        return mq.areColumnsUnique(rel, colMask.build(), true)
    }

    fun areColumnsDefinitelyUniqueWhenNullsFiltered(
        mq: RelMetadataQuery, rel: RelNode?, columnRefs: List<RexInputRef?>
    ): Boolean {
        val b = areColumnsUniqueWhenNullsFiltered(mq, rel, columnRefs)
        return b != null && b
    }

    /**
     * Separates a bit-mask representing a join into masks representing the left
     * and right inputs into the join.
     *
     * @param groupKey      original bit-mask
     * @param leftMask      left bit-mask to be set
     * @param rightMask     right bit-mask to be set
     * @param nFieldsOnLeft number of fields in the left input
     */
    fun setLeftRightBitmaps(
        groupKey: ImmutableBitSet,
        leftMask: ImmutableBitSet.Builder,
        rightMask: ImmutableBitSet.Builder,
        nFieldsOnLeft: Int
    ) {
        for (bit in groupKey) {
            if (bit < nFieldsOnLeft) {
                leftMask.set(bit)
            } else {
                rightMask.set(bit - nFieldsOnLeft)
            }
        }
    }

    /**
     * Returns the number of distinct values provided numSelected are selected
     * where there are domainSize distinct values.
     *
     *
     * Note that in the case where domainSize == numSelected, it's not true
     * that the return value should be domainSize. If you pick 100 random values
     * between 1 and 100, you'll most likely end up with fewer than 100 distinct
     * values, because you'll pick some values more than once.
     *
     *
     * The implementation is an unbiased estimation of the number of distinct
     * values by performing a number of selections (with replacement) from a
     * universe set.
     *
     * @param domainSize Size of the universe set
     * @param numSelected The number of selections
     *
     * @return the expected number of distinct values, or null if either argument
     * is null
     */
    @PolyNull
    fun numDistinctVals(
        @PolyNull domainSize: Double?,
        @PolyNull numSelected: Double?
    ): Double? {
        if (domainSize == null || numSelected == null) {
            return domainSize
        }

        // Cap the input sizes at MAX_VALUE to ensure that the calculations
        // using these values return meaningful values
        val dSize = capInfinity(domainSize)
        val numSel = capInfinity(numSelected)

        // The formula is derived as follows:
        //
        // Suppose we have N distinct values, and we select n from them (with replacement).
        // For any value i, we use C(i) = k to express the event that the value is selected exactly
        // k times in the n selections.
        //
        // It can be seen that, for any one selection, the probability of the value being selected
        // is 1/N. So the probability of being selected exactly k times is
        //
        // Pr{C(i) = k} = C(n, k) * (1 / N)^k * (1 - 1 / N)^(n - k),
        // where C(n, k) = n! / [k! * (n - k)!]
        //
        // The probability that the value is never selected is
        // Pr{C(i) = 0} = C(n, 0) * (1/N)^0 * (1 - 1 / N)^n = (1 - 1 / N)^n
        //
        // We define indicator random variable I(i), so that I(i) = 1 iff
        // value i is selected in at least one of the selections. We have
        // E[I(i)] = 1 * Pr{I(i) = 1} + 0 * Pr{I(i) = 0) = Pr{I(i) = 1}
        // = Pr{C(i) > 0} = 1 - Pr{C(i) = 0} = 1 - (1 - 1 / N)^n
        //
        // The expected number of distinct values in the overall n selections is:
        // E(I(1)] + E(I(2)] + ... + E(I(N)] = N * [1 - (1 - 1 / N)^n]
        var res = 0.0
        if (dSize > 0) {
            val expo: Double = numSel * Math.log(1.0 - 1.0 / dSize)
            res = (1.0 - Math.exp(expo)) * dSize
        }

        // fix the boundary cases
        if (res > dSize) {
            res = dSize
        }
        if (res > numSel) {
            res = numSel
        }
        if (res < 0) {
            res = 0.0
        }
        return res
    }

    /**
     * Caps a double value at Double.MAX_VALUE if it's currently infinity
     *
     * @param d the Double object
     * @return the double value if it's not infinity; else Double.MAX_VALUE
     */
    fun capInfinity(d: Double): Double {
        return if (d.isInfinite()) Double.MAX_VALUE else d
    }

    /**
     * Returns default estimates for selectivities, in the absence of stats.
     *
     * @param predicate predicate for which selectivity will be computed; null
     * means true, so gives selectity of 1.0
     * @return estimated selectivity
     */
    fun guessSelectivity(@Nullable predicate: RexNode?): Double {
        return guessSelectivity(predicate, false)
    }

    /**
     * Returns default estimates for selectivities, in the absence of stats.
     *
     * @param predicate      predicate for which selectivity will be computed;
     * null means true, so gives selectity of 1.0
     * @param artificialOnly return only the selectivity contribution from
     * artificial nodes
     * @return estimated selectivity
     */
    fun guessSelectivity(
        @Nullable predicate: RexNode?,
        artificialOnly: Boolean
    ): Double {
        var sel = 1.0
        if (predicate == null || predicate.isAlwaysTrue()) {
            return sel
        }
        var artificialSel = 1.0
        for (pred in RelOptUtil.conjunctions(predicate)) {
            if (pred.getKind() === SqlKind.IS_NOT_NULL) {
                sel *= .9
            } else if (pred is RexCall
                && ((pred as RexCall).getOperator()
                        === ARTIFICIAL_SELECTIVITY_FUNC)
            ) {
                artificialSel *= getSelectivityValue(pred)
            } else if (pred.isA(SqlKind.EQUALS)) {
                sel *= .15
            } else if (pred.isA(SqlKind.COMPARISON)) {
                sel *= .5
            } else {
                sel *= .25
            }
        }
        return if (artificialOnly) {
            artificialSel
        } else {
            sel * artificialSel
        }
    }

    /**
     * AND's two predicates together, either of which may be null, removing
     * redundant filters.
     *
     * @param rexBuilder rexBuilder used to construct AND'd RexNode
     * @param pred1      first predicate
     * @param pred2      second predicate
     * @return AND'd predicate or individual predicates if one is null
     */
    @Nullable
    fun unionPreds(
        rexBuilder: RexBuilder?,
        @Nullable pred1: RexNode?,
        @Nullable pred2: RexNode?
    ): RexNode {
        val unionList: Set<RexNode> = LinkedHashSet()
        unionList.addAll(RelOptUtil.conjunctions(pred1))
        unionList.addAll(RelOptUtil.conjunctions(pred2))
        return RexUtil.composeConjunction(rexBuilder, unionList, true)
    }

    /**
     * Takes the difference between two predicates, removing from the first any
     * predicates also in the second.
     *
     * @param rexBuilder rexBuilder used to construct AND'd RexNode
     * @param pred1      first predicate
     * @param pred2      second predicate
     * @return MINUS'd predicate list
     */
    @Nullable
    fun minusPreds(
        rexBuilder: RexBuilder?,
        @Nullable pred1: RexNode?,
        @Nullable pred2: RexNode?
    ): RexNode {
        val minusList: List<RexNode> = ArrayList(RelOptUtil.conjunctions(pred1))
        minusList.removeAll(RelOptUtil.conjunctions(pred2))
        return RexUtil.composeConjunction(rexBuilder, minusList, true)
    }

    /**
     * Takes a bitmap representing a set of input references and extracts the
     * ones that reference the group by columns in an aggregate.
     *
     * @param groupKey the original bitmap
     * @param aggRel   the aggregate
     * @param childKey sets bits from groupKey corresponding to group by columns
     */
    fun setAggChildKeys(
        groupKey: ImmutableBitSet,
        aggRel: Aggregate,
        childKey: ImmutableBitSet.Builder
    ) {
        val aggCalls: List<AggregateCall> = aggRel.getAggCallList()
        for (bit in groupKey) {
            if (bit < aggRel.getGroupCount()) {
                // group by column
                childKey.set(bit)
            } else {
                // aggregate column -- set a bit for each argument being
                // aggregated
                val agg: AggregateCall = aggCalls[bit - aggRel.getGroupCount()]
                for (arg in agg.getArgList()) {
                    childKey.set(arg)
                }
            }
        }
    }

    /**
     * Forms two bitmaps by splitting the columns in a bitmap according to
     * whether or not the column references the child input or is an expression.
     *
     * @param projExprs Project expressions
     * @param groupKey  Bitmap whose columns will be split
     * @param baseCols  Bitmap representing columns from the child input
     * @param projCols  Bitmap representing non-child columns
     */
    fun splitCols(
        projExprs: List<RexNode>,
        groupKey: ImmutableBitSet,
        baseCols: ImmutableBitSet.Builder,
        projCols: ImmutableBitSet.Builder
    ) {
        for (bit in groupKey) {
            val e: RexNode = projExprs[bit]
            if (e is RexInputRef) {
                baseCols.set((e as RexInputRef).getIndex())
            } else {
                projCols.set(bit)
            }
        }
    }

    /**
     * Computes the cardinality of a particular expression from the projection
     * list.
     *
     * @param rel  RelNode corresponding to the project
     * @param expr projection expression
     * @return cardinality
     */
    @Nullable
    fun cardOfProjExpr(
        mq: RelMetadataQuery, rel: Project,
        expr: RexNode
    ): Double {
        return expr.accept(CardOfProjExpr(mq, rel))
    }

    /**
     * Computes the population size for a set of keys returned from a join.
     *
     * @param join_  Join relational operator
     * @param groupKey Keys to compute the population for
     * @return computed population size
     */
    @Nullable
    fun getJoinPopulationSize(
        mq: RelMetadataQuery,
        join_: RelNode, groupKey: ImmutableBitSet
    ): Double? {
        val join: Join = join_ as Join
        if (!join.getJoinType().projectsRight()) {
            return mq.getPopulationSize(join.getLeft(), groupKey)
        }
        val leftMask: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val rightMask: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val left: RelNode = join.getLeft()
        val right: RelNode = join.getRight()

        // separate the mask into masks for the left and right
        setLeftRightBitmaps(
            groupKey, leftMask, rightMask, left.getRowType().getFieldCount()
        )
        val population: Double = multiply(
            mq.getPopulationSize(left, leftMask.build()),
            mq.getPopulationSize(right, rightMask.build())
        )
        return numDistinctVals(population, mq.getRowCount(join))
    }

    /** Add an epsilon to the value passed in.  */
    fun addEpsilon(d: Double): Double {
        var d = d
        assert(d >= 0.0)
        val d0 = d
        if (d < 10) {
            // For small d, adding 1 would change the value significantly.
            d *= 1.001
            if (d != d0) {
                return d
            }
        }
        // For medium d, add 1. Keeps integral values integral.
        ++d
        if (d != d0) {
            return d
        }
        // For large d, adding 1 might not change the value. Add .1%.
        // If d is NaN, this still will probably not change the value. That's OK.
        d *= 1.001
        return d
    }

    /**
     * Computes the number of distinct rows for a set of keys returned from a
     * semi-join.
     *
     * @param semiJoinRel RelNode representing the semi-join
     * @param mq          metadata query
     * @param groupKey    keys that the distinct row count will be computed for
     * @param predicate   join predicate
     * @return number of distinct rows
     */
    @Nullable
    fun getSemiJoinDistinctRowCount(
        semiJoinRel: Join, mq: RelMetadataQuery,
        groupKey: ImmutableBitSet, @Nullable predicate: RexNode?
    ): Double? {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1.0
            }
        }
        // create a RexNode representing the selectivity of the
        // semijoin filter and pass it to getDistinctRowCount
        var newPred: RexNode = makeSemiJoinSelectivityRexNode(mq, semiJoinRel)
        if (predicate != null) {
            val rexBuilder: RexBuilder = semiJoinRel.getCluster().getRexBuilder()
            newPred = rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                newPred,
                predicate
            )
        }
        return mq.getDistinctRowCount(semiJoinRel.getLeft(), groupKey, newPred)
    }

    /**
     * Computes the number of distinct rows for a set of keys returned from a
     * join. Also known as NDV (number of distinct values).
     *
     * @param joinRel   RelNode representing the join
     * @param joinType  type of join
     * @param groupKey  keys that the distinct row count will be computed for
     * @param predicate join predicate
     * @param useMaxNdv If true use formula `max(left NDV, right NDV)`,
     * otherwise use `left NDV * right NDV`.
     * @return number of distinct rows
     */
    @Nullable
    fun getJoinDistinctRowCount(
        mq: RelMetadataQuery,
        joinRel: RelNode, joinType: JoinRelType, groupKey: ImmutableBitSet,
        @Nullable predicate: RexNode?, useMaxNdv: Boolean
    ): Double? {
        if (predicate == null || predicate.isAlwaysTrue()) {
            if (groupKey.isEmpty()) {
                return 1.0
            }
        }
        val join: Join = joinRel as Join
        if (join.isSemiJoin()) {
            return getSemiJoinDistinctRowCount(join, mq, groupKey, predicate)
        }
        val distRowCount: Double
        val leftMask: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val rightMask: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val left: RelNode = joinRel.getInputs().get(0)
        val right: RelNode = joinRel.getInputs().get(1)
        setLeftRightBitmaps(
            groupKey,
            leftMask,
            rightMask,
            left.getRowType().getFieldCount()
        )

        // determine which filters apply to the left vs right
        var leftPred: RexNode? = null
        var rightPred: RexNode? = null
        if (predicate != null) {
            val leftFilters: List<RexNode> = ArrayList()
            val rightFilters: List<RexNode> = ArrayList()
            val joinFilters: List<RexNode> = ArrayList()
            val predList: List<RexNode> = RelOptUtil.conjunctions(predicate)
            RelOptUtil.classifyFilters(
                joinRel,
                predList,
                joinType.canPushIntoFromAbove(),
                joinType.canPushLeftFromAbove(),
                joinType.canPushRightFromAbove(),
                joinFilters,
                leftFilters,
                rightFilters
            )
            val rexBuilder: RexBuilder = joinRel.getCluster().getRexBuilder()
            leftPred = RexUtil.composeConjunction(rexBuilder, leftFilters, true)
            rightPred = RexUtil.composeConjunction(rexBuilder, rightFilters, true)
        }
        if (useMaxNdv) {
            distRowCount = NumberUtil.max(
                mq.getDistinctRowCount(left, leftMask.build(), leftPred),
                mq.getDistinctRowCount(right, rightMask.build(), rightPred)
            )
        } else {
            distRowCount = multiply(
                mq.getDistinctRowCount(left, leftMask.build(), leftPred),
                mq.getDistinctRowCount(right, rightMask.build(), rightPred)
            )
        }
        return numDistinctVals(distRowCount, mq.getRowCount(joinRel))
    }

    /** Returns an estimate of the number of rows returned by a [Union]
     * (before duplicates are eliminated).  */
    fun getUnionAllRowCount(mq: RelMetadataQuery, rel: Union): Double {
        var rowCount = 0.0
        for (input in rel.getInputs()) {
            rowCount += mq.getRowCount(input)
        }
        return rowCount
    }

    /** Returns an estimate of the number of rows returned by a [Minus].  */
    fun getMinusRowCount(mq: RelMetadataQuery, minus: Minus): Double {
        // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
        val inputs: List<RelNode> = minus.getInputs()
        var dRows: Double = mq.getRowCount(inputs[0])
        for (i in 1 until inputs.size()) {
            dRows -= 0.5 * mq.getRowCount(inputs[i])
        }
        if (dRows < 0) {
            dRows = 0.0
        }
        return dRows
    }

    /** Returns an estimate of the number of rows returned by a [Join].  */
    @Nullable
    fun getJoinRowCount(
        mq: RelMetadataQuery, join: Join,
        condition: RexNode?
    ): Double? {
        if (!join.getJoinType().projectsRight()) {
            // Create a RexNode representing the selectivity of the
            // semijoin filter and pass it to getSelectivity
            val semiJoinSelectivity: RexNode = makeSemiJoinSelectivityRexNode(mq, join)
            val selectivity: Double = mq.getSelectivity(join.getLeft(), semiJoinSelectivity) ?: return null
            return ((if (join.getJoinType() === JoinRelType.SEMI) selectivity else 1.0 - selectivity) // ANTI join
                    * mq.getRowCount(join.getLeft()))
        }
        // Row count estimates of 0 will be rounded up to 1.
        // So, use maxRowCount where the product is very small.
        val left: Double = mq.getRowCount(join.getLeft())
        val right: Double = mq.getRowCount(join.getRight())
        if (left == null || right == null) {
            return null
        }
        if (left <= 1.0 || right <= 1.0) {
            val max: Double = mq.getMaxRowCount(join)
            if (max != null && max <= 1.0) {
                return max
            }
        }
        val selectivity: Double = mq.getSelectivity(join, condition) ?: return null
        val innerRowCount = left * right * selectivity
        return when (join.getJoinType()) {
            INNER -> innerRowCount
            LEFT -> left * (1.0 - selectivity) + innerRowCount
            RIGHT -> right * (1.0 - selectivity) + innerRowCount
            FULL -> (left + right) * (1.0 - selectivity) + innerRowCount
            else -> throw Util.unexpected(join.getJoinType())
        }
    }

    fun estimateFilteredRows(
        child: RelNode?, program: RexProgram,
        mq: RelMetadataQuery?
    ): Double {
        // convert the program's RexLocalRef condition to an expanded RexNode
        val programCondition: RexLocalRef = program.getCondition()
        val condition: RexNode?
        condition = if (programCondition == null) {
            null
        } else {
            program.expandLocalRef(programCondition)
        }
        return estimateFilteredRows(child, condition, mq)
    }

    fun estimateFilteredRows(
        child: RelNode?, @Nullable condition: RexNode?,
        mq: RelMetadataQuery
    ): Double {
        return multiply(
            mq.getRowCount(child),
            mq.getSelectivity(child, condition)
        )
    }

    /** Returns a point on a line.
     *
     *
     * The result is always a value between `minY` and `maxY`,
     * even if `x` is not between `minX` and `maxX`.
     *
     *
     * Examples:
     *  * `linear(0, 0, 10, 100, 200`} returns 100 because 0 is minX
     *  * `linear(5, 0, 10, 100, 200`} returns 150 because 5 is
     * mid-way between minX and maxX
     *  * `linear(5, 0, 10, 100, 200`} returns 160
     *  * `linear(10, 0, 10, 100, 200`} returns 200 because 10 is maxX
     *  * `linear(-2, 0, 10, 100, 200`} returns 100 because -2 is
     * less than minX and is therefore treated as minX
     *  * `linear(12, 0, 10, 100, 200`} returns 100 because 12 is
     * greater than maxX and is therefore treated as maxX
     *
     */
    fun linear(x: Int, minX: Int, maxX: Int, minY: Double, maxY: Double): Double {
        Preconditions.checkArgument(minX < maxX)
        Preconditions.checkArgument(minY < maxY)
        if (x < minX) {
            return minY
        }
        return if (x > maxX) {
            maxY
        } else minY + (x - minX).toDouble() / (maxX - minX).toDouble() * (maxY - minY)
    }

    /** Returns whether a relational expression is already sorted and has fewer
     * rows than the sum of offset and limit.
     *
     *
     * If this is the case, it is safe to push down a
     * [org.apache.calcite.rel.core.Sort] with limit and optional offset.  */
    fun checkInputForCollationAndLimit(
        mq: RelMetadataQuery,
        input: RelNode, collation: RelCollation, @Nullable offset: RexNode?, @Nullable fetch: RexNode?
    ): Boolean {
        return alreadySorted(mq, input, collation) && alreadySmaller(mq, input, offset, fetch)
    }

    // Checks if the input is already sorted
    private fun alreadySorted(mq: RelMetadataQuery, input: RelNode, collation: RelCollation): Boolean {
        if (collation.getFieldCollations().isEmpty()) {
            return true
        }
        val collations: ImmutableList<RelCollation> = mq.collations(input)
            ?: // Cannot be determined
            return false
        for (inputCollation in collations) {
            if (inputCollation.satisfies(collation)) {
                return true
            }
        }
        return false
    }

    // Checks if we are not reducing the number of tuples
    private fun alreadySmaller(
        mq: RelMetadataQuery, input: RelNode,
        @Nullable offset: RexNode?, @Nullable fetch: RexNode?
    ): Boolean {
        if (fetch == null) {
            return true
        }
        val rowCount: Double = mq.getMaxRowCount(input)
            ?: // Cannot be determined
            return false
        val offsetVal = if (offset == null) 0 else RexLiteral.intValue(offset)
        val limit: Int = RexLiteral.intValue(fetch)
        return offsetVal.toDouble() + limit.toDouble() >= rowCount
    }

    /**
     * Validates whether a value represents a percentage number
     * (that is, a value in the interval [0.0, 1.0]) and returns the value.
     *
     *
     * Returns null if and only if `result` is null.
     *
     *
     * Throws if `result` is not null, not in range 0 to 1,
     * and assertions are enabled.
     */
    @PolyNull
    fun validatePercentage(@PolyNull result: Double): Double {
        assert(isPercentage(result, true))
        return result
    }

    private fun isPercentage(@Nullable result: Double, fail: Boolean): Boolean {
        if (result != null) {
            if (result < 0.0) {
                assert(!fail)
                return false
            }
            if (result > 1.0) {
                assert(!fail)
                return false
            }
        }
        return true
    }

    /**
     * Validates the `result` is valid.
     *
     *
     * Never let the result go below 1, as it will result in incorrect
     * calculations if the row-count is used as the denominator in a
     * division expression.  Also, cap the value at the max double value
     * to avoid calculations using infinity.
     *
     *
     * Returns null if and only if `result` is null.
     *
     *
     * Throws if `result` is not null, is negative,
     * and assertions are enabled.
     *
     * @return the corrected value from the `result`
     * @throws AssertionError if the `result` is negative
     */
    @PolyNull
    fun validateResult(@PolyNull result: Double?): Double? {
        var result = result ?: return null
        if (result.isInfinite()) {
            result = Double.MAX_VALUE
        }
        assert(isNonNegative(result, true))
        if (result < 1.0) {
            result = 1.0
        }
        return result
    }

    private fun isNonNegative(@Nullable result: Double, fail: Boolean): Boolean {
        if (result != null) {
            if (result < 0.0) {
                assert(!fail)
                return false
            }
        }
        return true
    }

    /**
     * Removes cached metadata values for specified RelNode.
     *
     * @param rel RelNode whose cached metadata should be removed
     * @return true if cache for the provided RelNode was not empty
     */
    fun clearCache(rel: RelNode): Boolean {
        return rel.getCluster().getMetadataQuery().clearCache(rel)
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Visitor that walks over a scalar expression and computes the
     * cardinality of its result.  */
    private class CardOfProjExpr internal constructor(mq: RelMetadataQuery, rel: Project) :
        RexVisitorImpl<Double?>(true) {
        private val mq: RelMetadataQuery
        private val rel: Project

        init {
            this.mq = mq
            this.rel = rel
        }

        @Override
        @Nullable
        fun visitInputRef(`var`: RexInputRef): Double? {
            val index: Int = `var`.getIndex()
            val col: ImmutableBitSet = ImmutableBitSet.of(index)
            val distinctRowCount: Double = mq.getDistinctRowCount(rel.getInput(), col, null)
            return if (distinctRowCount == null) {
                null
            } else {
                numDistinctVals(distinctRowCount, mq.getRowCount(rel))
            }
        }

        @Override
        @Nullable
        fun visitLiteral(literal: RexLiteral?): Double? {
            return numDistinctVals(1.0, mq.getRowCount(rel))
        }

        @Override
        @Nullable
        fun visitCall(call: RexCall): Double? {
            val distinctRowCount: Double
            val rowCount: Double = mq.getRowCount(rel)
            if (call.isA(SqlKind.MINUS_PREFIX)) {
                distinctRowCount = cardOfProjExpr(mq, rel, call.getOperands().get(0))
            } else if (call.isA(ImmutableList.of(SqlKind.PLUS, SqlKind.MINUS))) {
                val card0 = cardOfProjExpr(mq, rel, call.getOperands().get(0))
                    ?: return null
                val card1 = cardOfProjExpr(mq, rel, call.getOperands().get(1))
                    ?: return null
                distinctRowCount = Math.max(card0, card1)
            } else if (call.isA(ImmutableList.of(SqlKind.TIMES, SqlKind.DIVIDE))) {
                distinctRowCount = multiply(
                    cardOfProjExpr(mq, rel, call.getOperands().get(0)),
                    cardOfProjExpr(mq, rel, call.getOperands().get(1))
                )

                // TODO zfong 6/21/06 - Broadbase has code to handle date
                // functions like year, month, day; E.g., cardinality of Month()
                // is 12
            } else {
                distinctRowCount = if (call.getOperands().size() === 1) {
                    cardOfProjExpr(mq, rel, call.getOperands().get(0))
                } else {
                    rowCount / 10
                }
            }
            return numDistinctVals(distinctRowCount, rowCount)
        }
    }
}
