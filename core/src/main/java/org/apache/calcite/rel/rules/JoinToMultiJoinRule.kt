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

import org.apache.calcite.plan.RelOptRuleCall

/**
 * Planner rule to flatten a tree of
 * [org.apache.calcite.rel.logical.LogicalJoin]s
 * into a single [MultiJoin] with N inputs.
 *
 *
 * An input is not flattened if
 * the input is a null generating input in an outer join, i.e., either input in
 * a full outer join, the right hand side of a left outer join, or the left hand
 * side of a right outer join.
 *
 *
 * Join conditions are also pulled up from the inputs into the topmost
 * [MultiJoin],
 * unless the input corresponds to a null generating input in an outer join,
 *
 *
 * Outer join information is also stored in the [MultiJoin]. A
 * boolean flag indicates if the join is a full outer join, and in the case of
 * left and right outer joins, the join type and outer join conditions are
 * stored in arrays in the [MultiJoin]. This outer join information is
 * associated with the null generating input in the outer join. So, in the case
 * of a a left outer join between A and B, the information is associated with B,
 * not A.
 *
 *
 * Here are examples of the [MultiJoin]s constructed after this rule
 * has been applied on following join trees.
 *
 *
 *  * A JOIN B  MJ(A, B)
 *
 *  * A JOIN B JOIN C  MJ(A, B, C)
 *
 *  * A LEFT JOIN B  MJ(A, B), left outer join on input#1
 *
 *  * A RIGHT JOIN B  MJ(A, B), right outer join on input#0
 *
 *  * A FULL JOIN B  MJ[full](A, B)
 *
 *  * A LEFT JOIN (B JOIN C)  MJ(A, MJ(B, C))), left outer join on
 * input#1 in the outermost MultiJoin
 *
 *  * (A JOIN B) LEFT JOIN C  MJ(A, B, C), left outer join on input#2
 *
 *  * (A LEFT JOIN B) JOIN C  MJ(MJ(A, B), C), left outer join on input#1
 * of the inner MultiJoin        TODO
 *
 *  * A LEFT JOIN (B FULL JOIN C)  MJ(A, MJ[full](B, C)), left outer join
 * on input#1 in the outermost MultiJoin
 *
 *  * (A LEFT JOIN B) FULL JOIN (C RIGHT JOIN D)
 * MJ[full](MJ(A, B), MJ(C, D)), left outer join on input #1 in the first
 * inner MultiJoin and right outer join on input#0 in the second inner
 * MultiJoin
 *
 *
 *
 * The constructor is parameterized to allow any sub-class of
 * [org.apache.calcite.rel.core.Join], not just
 * [org.apache.calcite.rel.logical.LogicalJoin].
 *
 * @see org.apache.calcite.rel.rules.FilterMultiJoinMergeRule
 *
 * @see org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule
 *
 * @see CoreRules.JOIN_TO_MULTI_JOIN
 */
@Value.Enclosing
class JoinToMultiJoinRule
/** Creates a JoinToMultiJoinRule.  */
protected constructor(config: Config?) : RelRule<JoinToMultiJoinRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(clazz: Class<out Join?>?) : this(Config.DEFAULT.withOperandFor(clazz)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        joinClass: Class<out Join?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(joinClass)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val origJoin: Join = call.rel(0)
        return origJoin.getJoinType().projectsRight()
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val origJoin: Join = call.rel(0)
        val left: RelNode = call.rel(1)
        val right: RelNode = call.rel(2)

        // combine the children MultiJoin inputs into an array of inputs
        // for the new MultiJoin
        val projFieldsList: List<ImmutableBitSet> = ArrayList()
        val joinFieldRefCountsList: List<IntArray> = ArrayList()
        val newInputs: List<RelNode> = combineInputs(
            origJoin,
            left,
            right,
            projFieldsList,
            joinFieldRefCountsList
        )

        // combine the outer join information from the left and right
        // inputs, and include the outer join information from the current
        // join, if it's a left/right outer join
        val joinSpecs: List<Pair<JoinRelType, RexNode>> = ArrayList()
        combineOuterJoins(
            origJoin,
            newInputs,
            left,
            right,
            joinSpecs
        )

        // pull up the join filters from the children MultiJoinRels and
        // combine them with the join filter associated with this LogicalJoin to
        // form the join filter for the new MultiJoin
        val newJoinFilters: List<RexNode> = combineJoinFilters(origJoin, left, right)

        // add on the join field reference counts for the join condition
        // associated with this LogicalJoin
        val newJoinFieldRefCountsMap: ImmutableMap<Integer, ImmutableIntList> = addOnJoinFieldRefCounts(
            newInputs,
            origJoin.getRowType().getFieldCount(),
            origJoin.getCondition(),
            joinFieldRefCountsList
        )
        val newPostJoinFilters: List<RexNode> = combinePostJoinFilters(origJoin, left, right)
        val rexBuilder: RexBuilder = origJoin.getCluster().getRexBuilder()
        val multiJoin: RelNode = MultiJoin(
            origJoin.getCluster(),
            newInputs,
            RexUtil.composeConjunction(rexBuilder, newJoinFilters),
            origJoin.getRowType(),
            origJoin.getJoinType() === JoinRelType.FULL,
            Pair.right(joinSpecs),
            Pair.left(joinSpecs),
            projFieldsList,
            newJoinFieldRefCountsMap,
            RexUtil.composeConjunction(rexBuilder, newPostJoinFilters, true)
        )
        call.transformTo(multiJoin)
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Visitor that keeps a reference count of the inputs used by an expression.
     */
    private class InputReferenceCounter internal constructor(private val refCounts: IntArray) :
        RexVisitorImpl<Void?>(true) {
        @Override
        fun visitInputRef(inputRef: RexInputRef): Void? {
            refCounts[inputRef.getIndex()]++
            return null
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinToMultiJoinRule? {
            return JoinToMultiJoinRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(joinClass).inputs(
                    { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                ) { b2 -> b2.operand(RelNode::class.java).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableJoinToMultiJoinRule.Config.of()
                .withOperandFor(LogicalJoin::class.java)
        }
    }

    companion object {
        /**
         * Combines the inputs into a LogicalJoin into an array of inputs.
         *
         * @param join                   original join
         * @param left                   left input into join
         * @param right                  right input into join
         * @param projFieldsList         returns a list of the new combined projection
         * fields
         * @param joinFieldRefCountsList returns a list of the new combined join
         * field reference counts
         * @return combined left and right inputs in an array
         */
        private fun combineInputs(
            join: Join,
            left: RelNode,
            right: RelNode,
            projFieldsList: List<ImmutableBitSet>,
            joinFieldRefCountsList: List<IntArray>
        ): List<RelNode> {
            val newInputs: List<RelNode> = ArrayList()

            // leave the null generating sides of an outer join intact; don't
            // pull up those children inputs into the array we're constructing
            if (canCombine(left, join.getJoinType().generatesNullsOnLeft())) {
                val leftMultiJoin: MultiJoin = left as MultiJoin
                for (i in 0 until left.getInputs().size()) {
                    newInputs.add(leftMultiJoin.getInput(i))
                    projFieldsList.add(leftMultiJoin.getProjFields().get(i))
                    joinFieldRefCountsList.add(
                        leftMultiJoin.getJoinFieldRefCountsMap().get(i).toIntArray()
                    )
                }
            } else {
                newInputs.add(left)
                projFieldsList.add(null)
                joinFieldRefCountsList.add(IntArray(left.getRowType().getFieldCount()))
            }
            if (canCombine(right, join.getJoinType().generatesNullsOnRight())) {
                val rightMultiJoin: MultiJoin = right as MultiJoin
                for (i in 0 until right.getInputs().size()) {
                    newInputs.add(rightMultiJoin.getInput(i))
                    projFieldsList.add(
                        rightMultiJoin.getProjFields().get(i)
                    )
                    joinFieldRefCountsList.add(
                        rightMultiJoin.getJoinFieldRefCountsMap().get(i).toIntArray()
                    )
                }
            } else {
                newInputs.add(right)
                projFieldsList.add(null)
                joinFieldRefCountsList.add(IntArray(right.getRowType().getFieldCount()))
            }
            return newInputs
        }

        /**
         * Combines the outer join conditions and join types from the left and right
         * join inputs. If the join itself is either a left or right outer join,
         * then the join condition corresponding to the join is also set in the
         * position corresponding to the null-generating input into the join. The
         * join type is also set.
         *
         * @param joinRel        join rel
         * @param combinedInputs the combined inputs to the join
         * @param left           left child of the joinrel
         * @param right          right child of the joinrel
         * @param joinSpecs      the list where the join types and conditions will be
         * copied
         */
        private fun combineOuterJoins(
            joinRel: Join,
            @SuppressWarnings("unused") combinedInputs: List<RelNode>,
            left: RelNode,
            right: RelNode,
            joinSpecs: List<Pair<JoinRelType, RexNode>>
        ) {
            val joinType: JoinRelType = joinRel.getJoinType()
            val leftCombined = canCombine(left, joinType.generatesNullsOnLeft())
            val rightCombined = canCombine(right, joinType.generatesNullsOnRight())
            when (joinType) {
                LEFT -> {
                    if (leftCombined) {
                        copyOuterJoinInfo(
                            left as MultiJoin,
                            joinSpecs,
                            0,
                            null,
                            null
                        )
                    } else {
                        joinSpecs.add(Pair.of(JoinRelType.INNER, null as RexNode?))
                    }
                    joinSpecs.add(Pair.of(joinType, joinRel.getCondition()))
                }
                RIGHT -> {
                    joinSpecs.add(Pair.of(joinType, joinRel.getCondition()))
                    if (rightCombined) {
                        copyOuterJoinInfo(
                            right as MultiJoin,
                            joinSpecs,
                            left.getRowType().getFieldCount(),
                            right.getRowType().getFieldList(),
                            joinRel.getRowType().getFieldList()
                        )
                    } else {
                        joinSpecs.add(Pair.of(JoinRelType.INNER, null as RexNode?))
                    }
                }
                else -> {
                    if (leftCombined) {
                        copyOuterJoinInfo(
                            left as MultiJoin,
                            joinSpecs,
                            0,
                            null,
                            null
                        )
                    } else {
                        joinSpecs.add(Pair.of(JoinRelType.INNER, null as RexNode?))
                    }
                    if (rightCombined) {
                        copyOuterJoinInfo(
                            right as MultiJoin,
                            joinSpecs,
                            left.getRowType().getFieldCount(),
                            right.getRowType().getFieldList(),
                            joinRel.getRowType().getFieldList()
                        )
                    } else {
                        joinSpecs.add(Pair.of(JoinRelType.INNER, null as RexNode?))
                    }
                }
            }
        }

        /**
         * Copies outer join data from a source MultiJoin to a new set of arrays.
         * Also adjusts the conditions to reflect the new position of an input if
         * that input ends up being shifted to the right.
         *
         * @param multiJoin     the source MultiJoin
         * @param destJoinSpecs    the list where the join types and conditions will
         * be copied
         * @param adjustmentAmount if &gt; 0, the amount the RexInputRefs in the join
         * conditions need to be adjusted by
         * @param srcFields        the source fields that the original join conditions
         * are referencing
         * @param destFields       the destination fields that the new join conditions
         */
        private fun copyOuterJoinInfo(
            multiJoin: MultiJoin,
            destJoinSpecs: List<Pair<JoinRelType, RexNode>>,
            adjustmentAmount: Int,
            @Nullable srcFields: List<RelDataTypeField>?,
            @Nullable destFields: List<RelDataTypeField>?
        ) {
            val srcJoinSpecs: List<Pair<JoinRelType, RexNode>> = Pair.zip(
                multiJoin.getJoinTypes(),
                multiJoin.getOuterJoinConditions()
            )
            if (adjustmentAmount == 0) {
                destJoinSpecs.addAll(srcJoinSpecs)
            } else {
                assert(srcFields != null)
                assert(destFields != null)
                val nFields: Int = srcFields!!.size()
                val adjustments = IntArray(nFields)
                for (idx in 0 until nFields) {
                    adjustments[idx] = adjustmentAmount
                }
                for (src in srcJoinSpecs) {
                    destJoinSpecs.add(
                        Pair.of(
                            src.left,
                            if (src.right == null) null else src.right.accept(
                                RexInputConverter(
                                    multiJoin.getCluster().getRexBuilder(),
                                    srcFields, destFields, adjustments
                                )
                            )
                        )
                    )
                }
            }
        }

        /**
         * Combines the join filters from the left and right inputs (if they are
         * MultiJoinRels) with the join filter in the joinrel into a single AND'd
         * join filter, unless the inputs correspond to null generating inputs in an
         * outer join.
         *
         * @param join    Join
         * @param left    Left input of the join
         * @param right   Right input of the join
         * @return combined join filters AND-ed together
         */
        private fun combineJoinFilters(
            join: Join,
            left: RelNode,
            right: RelNode
        ): List<RexNode> {
            val joinType: JoinRelType = join.getJoinType()

            // AND the join condition if this isn't a left or right outer join;
            // in those cases, the outer join condition is already tracked
            // separately
            val filters: List<RexNode> = ArrayList()
            if (joinType !== JoinRelType.LEFT && joinType !== JoinRelType.RIGHT) {
                filters.add(join.getCondition())
            }
            if (canCombine(left, joinType.generatesNullsOnLeft())) {
                filters.add((left as MultiJoin).getJoinFilter())
            }
            // Need to adjust the RexInputs of the right child, since
            // those need to shift over to the right
            if (canCombine(right, joinType.generatesNullsOnRight())) {
                val multiJoin: MultiJoin = right as MultiJoin
                filters.add(
                    shiftRightFilter(
                        join, left, multiJoin,
                        multiJoin.getJoinFilter()
                    )
                )
            }
            return filters
        }

        /**
         * Returns whether an input can be merged into a given relational expression
         * without changing semantics.
         *
         * @param input          input into a join
         * @param nullGenerating true if the input is null generating
         * @return true if the input can be combined into a parent MultiJoin
         */
        private fun canCombine(input: RelNode, nullGenerating: Boolean): Boolean {
            return (input is MultiJoin
                    && !(input as MultiJoin).isFullOuterJoin()
                    && !(input as MultiJoin).containsOuter()
                    && !nullGenerating)
        }

        /**
         * Shifts a filter originating from the right child of the LogicalJoin to the
         * right, to reflect the filter now being applied on the resulting
         * MultiJoin.
         *
         * @param joinRel     the original LogicalJoin
         * @param left        the left child of the LogicalJoin
         * @param right       the right child of the LogicalJoin
         * @param rightFilter the filter originating from the right child
         * @return the adjusted right filter
         */
        @Nullable
        private fun shiftRightFilter(
            joinRel: Join,
            left: RelNode,
            right: MultiJoin,
            @Nullable rightFilter: RexNode
        ): RexNode? {
            var rightFilter: RexNode = rightFilter ?: return null
            val nFieldsOnLeft: Int = left.getRowType().getFieldList().size()
            val nFieldsOnRight: Int = right.getRowType().getFieldList().size()
            val adjustments = IntArray(nFieldsOnRight)
            for (i in 0 until nFieldsOnRight) {
                adjustments[i] = nFieldsOnLeft
            }
            rightFilter = rightFilter.accept(
                RexInputConverter(
                    joinRel.getCluster().getRexBuilder(),
                    right.getRowType().getFieldList(),
                    joinRel.getRowType().getFieldList(),
                    adjustments
                )
            )
            return rightFilter
        }

        /**
         * Adds on to the existing join condition reference counts the references
         * from the new join condition.
         *
         * @param multiJoinInputs          inputs into the new MultiJoin
         * @param nTotalFields             total number of fields in the MultiJoin
         * @param joinCondition            the new join condition
         * @param origJoinFieldRefCounts   existing join condition reference counts
         *
         * @return Map containing the new join condition
         */
        private fun addOnJoinFieldRefCounts(
            multiJoinInputs: List<RelNode>,
            nTotalFields: Int,
            joinCondition: RexNode,
            origJoinFieldRefCounts: List<IntArray>
        ): ImmutableMap<Integer, ImmutableIntList> {
            // count the input references in the join condition
            val joinCondRefCounts = IntArray(nTotalFields)
            joinCondition.accept(InputReferenceCounter(joinCondRefCounts))

            // first, make a copy of the ref counters
            val refCountsMap: Map<Integer, IntArray> = HashMap()
            val nInputs: Int = multiJoinInputs.size()
            var currInput = 0
            for (origRefCounts in origJoinFieldRefCounts) {
                refCountsMap.put(
                    currInput,
                    origRefCounts.clone()
                )
                currInput++
            }

            // add on to the counts for each input into the MultiJoin the
            // reference counts computed for the current join condition
            currInput = -1
            var startField = 0
            var nFields = 0
            for (i in 0 until nTotalFields) {
                if (joinCondRefCounts[i] == 0) {
                    continue
                }
                while (i >= startField + nFields) {
                    startField += nFields
                    currInput++
                    assert(currInput < nInputs)
                    nFields = multiJoinInputs[currInput].getRowType().getFieldCount()
                }
                val key = currInput
                val refCounts: IntArray = requireNonNull(
                    refCountsMap[key]
                ) { "refCountsMap.get(currInput) for $key" }
                refCounts[i - startField] += joinCondRefCounts[i]
            }
            val builder: ImmutableMap.Builder<Integer, ImmutableIntList> = ImmutableMap.builder()
            for (entry in refCountsMap.entrySet()) {
                builder.put(entry.getKey(), ImmutableIntList.of(entry.getValue()))
            }
            return builder.build()
        }

        /**
         * Combines the post-join filters from the left and right inputs (if they
         * are MultiJoinRels) into a single AND'd filter.
         *
         * @param joinRel the original LogicalJoin
         * @param left    left child of the LogicalJoin
         * @param right   right child of the LogicalJoin
         * @return combined post-join filters AND'd together
         */
        private fun combinePostJoinFilters(
            joinRel: Join,
            left: RelNode,
            right: RelNode
        ): List<RexNode> {
            val filters: List<RexNode> = ArrayList()
            if (right is MultiJoin) {
                val multiRight: MultiJoin = right as MultiJoin
                filters.add(
                    shiftRightFilter(
                        joinRel, left, multiRight,
                        multiRight.getPostJoinFilter()
                    )
                )
            }
            if (left is MultiJoin) {
                filters.add((left as MultiJoin).getPostJoinFilter())
            }
            return filters
        }
    }
}
