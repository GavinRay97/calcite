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
package org.apache.calcite.rel.core

import org.apache.calcite.linq4j.Ord

/**
 * Relational operator that eliminates
 * duplicates and computes totals.
 *
 *
 * It corresponds to the `GROUP BY` operator in a SQL query
 * statement, together with the aggregate functions in the `SELECT`
 * clause.
 *
 *
 * Rules:
 *
 *
 *  * [org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule]
 *  * [org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule]
 *  * [org.apache.calcite.rel.rules.AggregateReduceFunctionsRule].
 *
 */
abstract class Aggregate @SuppressWarnings("method.invocation.invalid") protected constructor(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    hints: List<RelHint?>?,
    input: RelNode,
    groupSet: ImmutableBitSet,
    @Nullable groupSets: List<ImmutableBitSet?>?,
    aggCalls: List<AggregateCall>
) : SingleRel(cluster, traitSet, input), Hintable {
    protected val hints: ImmutableList<RelHint>

    //~ Instance fields --------------------------------------------------------
    @Deprecated // unused field, to be removed before 2.0
    val indicator = false
    protected val aggCalls: List<AggregateCall>
    protected val groupSet: ImmutableBitSet
    val groupSets: ImmutableList<ImmutableBitSet>? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an Aggregate.
     *
     *
     * All members of `groupSets` must be sub-sets of `groupSet`.
     * For a simple `GROUP BY`, `groupSets` is a singleton list
     * containing `groupSet`.
     *
     *
     * It is allowed for `groupSet` to contain bits that are not in any
     * of the `groupSets`, even this does not correspond to valid SQL. See
     * discussion in
     * [org.apache.calcite.tools.RelBuilder.groupKey].
     *
     *
     * If `GROUP BY` is not specified,
     * or equivalently if `GROUP BY ()` is specified,
     * `groupSet` will be the empty set,
     * and `groupSets` will have one element, that empty set.
     *
     *
     * If `CUBE`, `ROLLUP` or `GROUPING SETS` are
     * specified, `groupSets` will have additional elements,
     * but they must each be a subset of `groupSet`,
     * and they must be sorted by inclusion:
     * `(0, 1, 2), (1), (0, 2), (0), ()`.
     *
     * @param cluster  Cluster
     * @param traitSet Trait set
     * @param hints    Hints of this relational expression
     * @param input    Input relational expression
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just `groupSet`
     * @param aggCalls Collection of calls to aggregate functions
     */
    init {
        this.hints = ImmutableList.copyOf(hints)
        this.aggCalls = ImmutableList.copyOf(aggCalls)
        this.groupSet = Objects.requireNonNull(groupSet, "groupSet")
        if (groupSets == null) {
            this.groupSets = ImmutableList.of(groupSet)
        } else {
            this.groupSets = ImmutableList.copyOf(groupSets)
            assert(ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets)) { groupSets }
            for (set in groupSets) {
                assert(groupSet.contains(set))
            }
        }
        assert(groupSet.length() <= input.getRowType().getFieldCount())
        for (aggCall in aggCalls) {
            assert(typeMatchesInferred(aggCall, Litmus.THROW))
            Preconditions.checkArgument(
                aggCall.filterArg < 0
                        || isPredicate(input, aggCall.filterArg),
                "filter must be BOOLEAN NOT NULL"
            )
        }
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        input: RelNode?,
        groupSet: ImmutableBitSet?,
        groupSets: List<ImmutableBitSet?>?,
        aggCalls: List<AggregateCall?>?
    ) : this(cluster, traitSet, ArrayList(), input, groupSet, groupSets, aggCalls) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?,
        traits: RelTraitSet?,
        child: RelNode?,
        indicator: Boolean,
        groupSet: ImmutableBitSet?,
        groupSets: List<ImmutableBitSet?>?,
        aggCalls: List<AggregateCall?>?
    ) : this(cluster, traits, ImmutableList.of(), child, groupSet, groupSets, aggCalls) {
        checkIndicator(indicator)
    }

    /**
     * Creates an Aggregate by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), ArrayList(),
        input.getInput(), input.getBitSet("group"),
        input.getBitSetList("groups"), input.getAggregateCalls("aggs")
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): RelNode {
        return copy(traitSet, sole(inputs), groupSet, groupSets, aggCalls)
    }

    /** Creates a copy of this aggregate.
     *
     * @param traitSet Traits
     * @param input Input
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just `groupSet`
     * @param aggCalls Collection of calls to aggregate functions
     * @return New `Aggregate` if any parameter differs from the value of
     * this `Aggregate`, or just `this` if all the parameters are
     * the same
     *
     * @see .copy
     */
    abstract fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        groupSet: ImmutableBitSet?,
        @Nullable groupSets: List<ImmutableBitSet>?, aggCalls: List<AggregateCall?>?
    ): Aggregate

    @Deprecated // to be removed before 2.0
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        indicator: Boolean, groupSet: ImmutableBitSet?,
        groupSets: List<ImmutableBitSet>?, aggCalls: List<AggregateCall?>?
    ): Aggregate {
        checkIndicator(indicator)
        return copy(traitSet, input, groupSet, groupSets, aggCalls)
    }

    /**
     * Returns a list of calls to aggregate functions.
     *
     * @return list of calls to aggregate functions
     */
    val aggCallList: List<org.apache.calcite.rel.core.AggregateCall>
        get() = aggCalls

    /**
     * Returns a list of calls to aggregate functions together with their output
     * field names.
     *
     * @return list of calls to aggregate functions and their output field names
     */
    val namedAggCalls: List<Any>
        get() {
            val offset = groupCount
            return Pair.zip(aggCalls, Util.skip(getRowType().getFieldNames(), offset))
        }

    /**
     * Returns the number of grouping fields.
     * These grouping fields are the leading fields in both the input and output
     * records.
     *
     *
     * NOTE: The [.getGroupSet] data structure allows for the
     * grouping fields to not be on the leading edge. New code should, if
     * possible, assume that grouping fields are in arbitrary positions in the
     * input relational expression.
     *
     * @return number of grouping fields
     */
    val groupCount: Int
        get() = groupSet.cardinality()

    /**
     * Returns the number of indicator fields.
     *
     *
     * Always zero.
     *
     * @return number of indicator fields, always zero
     */ // to be removed before 2.0
    @get:Deprecated
    val indicatorCount: Int
        get() = 0

    /**
     * Returns a bit set of the grouping fields.
     *
     * @return bit set of ordinals of grouping fields
     */
    fun getGroupSet(): ImmutableBitSet {
        return groupSet
    }

    /**
     * Returns the list of grouping sets computed by this Aggregate.
     *
     * @return List of all grouping sets
     */
    fun getGroupSets(): ImmutableList<ImmutableBitSet>? {
        return groupSets
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        // We skip the "groups" element if it is a singleton of "group".
        super.explainTerms(pw)
            .item("group", groupSet)
            .itemIf("groups", groupSets, groupType != Group.SIMPLE)
            .itemIf("aggs", aggCalls, pw.nest())
        if (!pw.nest()) {
            for (ord in Ord.zip(aggCalls)) {
                pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e)
            }
        }
        return pw
    }

    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        // Assume that each sort column has 50% of the value count.
        // Therefore one sort column has .5 * rowCount,
        // 2 sort columns give .75 * rowCount.
        // Zero sort columns yields 1 row (or 0 if the input is empty).
        val groupCount: Int = groupSet.cardinality()
        return if (groupCount == 0) {
            1
        } else {
            var rowCount: Double = super.estimateRowCount(mq)
            rowCount *= 1.0 - Math.pow(.5, groupCount)
            rowCount
        }
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        // REVIEW jvs 24-Aug-2008:  This is bogus, but no more bogus
        // than what's currently in Join.
        val rowCount: Double = mq.getRowCount(this)
        // Aggregates with more aggregate functions cost a bit more
        var multiplier = 1f + aggCalls.size() as Float * 0.125f
        for (aggCall in aggCalls) {
            if (aggCall.getAggregation().getName().equals("SUM")) {
                // Pretend that SUM costs a little bit more than $SUM0,
                // to make things deterministic.
                multiplier += 0.0125f
            }
        }
        return planner.getCostFactory().makeCost(rowCount * multiplier, 0, 0)
    }

    @Override
    protected fun deriveRowType(): RelDataType {
        return deriveRowType(
            getCluster().getTypeFactory(), getInput().getRowType(),
            false, groupSet, groupSets, aggCalls
        )
    }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        return (super.isValid(litmus, context)
                && litmus.check(
            Util.isDistinct(getRowType().getFieldNames()),
            "distinct field names: {}", getRowType()
        ))
    }

    /**
     * Returns whether the inferred type of an [AggregateCall] matches the
     * type it was given when it was created.
     *
     * @param aggCall Aggregate call
     * @param litmus What to do if an error is detected (types do not match)
     * @return Whether the inferred and declared types match
     */
    private fun typeMatchesInferred(
        aggCall: AggregateCall,
        litmus: Litmus
    ): Boolean {
        val aggFunction: SqlAggFunction = aggCall.getAggregation()
        val callBinding: AggCallBinding = aggCall.createBinding(this)
        val type: RelDataType = aggFunction.inferReturnType(callBinding)
        val expectedType: RelDataType = aggCall.type
        return RelOptUtil.eq(
            "aggCall type",
            expectedType,
            "inferred type",
            type,
            litmus
        )
    }

    /**
     * Returns whether any of the aggregates are DISTINCT.
     *
     * @return Whether any of the aggregates are DISTINCT
     */
    fun containsDistinctCall(): Boolean {
        for (call in aggCalls) {
            if (call.isDistinct()) {
                return true
            }
        }
        return false
    }

    @Override
    fun getHints(): ImmutableList<RelHint> {
        return hints
    }

    /**
     * Returns the type of roll-up.
     *
     * @return Type of roll-up
     */
    val groupType: Group
        get() = Group.induce(groupSet, groupSets)

    /** Describes the kind of roll-up.  */
    enum class Group {
        SIMPLE, ROLLUP, CUBE, OTHER;

        companion object {
            fun induce(
                groupSet: ImmutableBitSet?,
                groupSets: List<ImmutableBitSet>?
            ): Group {
                if (!ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets)) {
                    throw IllegalArgumentException("must be sorted: $groupSets")
                }
                if (groupSets!!.size() === 1 && groupSets!![0].equals(groupSet)) {
                    return SIMPLE
                }
                if (groupSets!!.size() === IntMath.pow(2, groupSet.cardinality())) {
                    return CUBE
                }
                return if (isRollup(groupSet, groupSets)) {
                    ROLLUP
                } else OTHER
            }

            /** Returns whether a list of sets is a rollup.
             *
             *
             * For example, if `groupSet` is `{2, 4, 5}`, then
             * `[{2, 4, 5], {2, 5}, {5}, {}]` is a rollup. The first item is
             * equal to `groupSet`, and each subsequent item is a subset with one
             * fewer bit than the previous.
             *
             * @see .getRollup
             */
            fun isRollup(
                groupSet: ImmutableBitSet?,
                groupSets: List<ImmutableBitSet?>?
            ): Boolean {
                if (groupSets!!.size() !== groupSet.cardinality() + 1) {
                    return false
                }
                var g: ImmutableBitSet? = null
                for (bitSet in groupSets) {
                    if (g == null) {
                        // First item must equal groupSet
                        if (!bitSet.equals(groupSet)) {
                            return false
                        }
                    } else {
                        // Each subsequent items must be a subset with one fewer bit than the
                        // previous item
                        if (!g.contains(bitSet)
                            || g.cardinality() - bitSet.cardinality() !== 1
                        ) {
                            return false
                        }
                    }
                    g = bitSet
                }
                assert(g != null) { "groupSet must not be empty" }
                assert(g.isEmpty())
                return true
            }

            /** Returns the ordered list of bits in a rollup.
             *
             *
             * For example, given a `groupSets` value
             * `[{2, 4, 5], {2, 5}, {5}, {}]`, returns the list
             * `[5, 2, 4]`, which are the succession of bits
             * added to each of the sets starting with the empty set.
             *
             * @see .isRollup
             */
            fun getRollup(groupSets: List<ImmutableBitSet?>): List<Integer> {
                val rollUpBits: List<Integer> = ArrayList(groupSets.size() - 1)
                var g: ImmutableBitSet? = null
                for (bitSet in groupSets) {
                    if (g == null) {
                        // First item must equal groupSet
                    } else {
                        // Each subsequent items must be a subset with one fewer bit than the
                        // previous item
                        val diff: ImmutableBitSet = g.except(bitSet)
                        assert(diff.cardinality() === 1)
                        rollUpBits.add(diff.nth(0))
                    }
                    g = bitSet
                }
                Collections.reverse(rollUpBits)
                return ImmutableList.copyOf(rollUpBits)
            }
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Implementation of the [SqlOperatorBinding] interface for an
     * [aggregate call][AggregateCall] applied to a set of operands in the
     * context of a [org.apache.calcite.rel.logical.LogicalAggregate].
     */
    class AggCallBinding(
        typeFactory: RelDataTypeFactory?,
        aggFunction: SqlAggFunction?, operands: List<RelDataType>?, groupCount: Int,
        filter: Boolean
    ) : SqlOperatorBinding(typeFactory, aggFunction) {
        private val operands: List<RelDataType>?

        @get:Override
        val groupCount: Int
        private val filter: Boolean

        /**
         * Creates an AggCallBinding.
         *
         * @param typeFactory  Type factory
         * @param aggFunction  Aggregate function
         * @param operands     Data types of operands
         * @param groupCount   Number of columns in the GROUP BY clause
         * @param filter       Whether the aggregate function has a FILTER clause
         */
        init {
            this.operands = operands
            this.groupCount = groupCount
            this.filter = filter
            assert(operands != null) { "operands of aggregate call should not be null" }
            assert(groupCount >= 0) {
                ("number of group by columns should be greater than zero in "
                        + "aggregate call. Got " + groupCount)
            }
        }

        @Override
        fun hasFilter(): Boolean {
            return filter
        }

        @get:Override
        val operandCount: Int
            get() = operands!!.size()

        @Override
        fun getOperandType(ordinal: Int): RelDataType {
            return operands!![ordinal]
        }

        @Override
        fun newError(
            e: Resources.ExInst<SqlValidatorException?>?
        ): CalciteException {
            return SqlUtil.newContextException(SqlParserPos.ZERO, e)
        }
    }

    companion object {
        fun isSimple(aggregate: Aggregate): Boolean {
            return aggregate.groupType == Group.SIMPLE
        }

        @SuppressWarnings("Guava")
        @Deprecated // to be converted to Java Predicate before 2.0
        val IS_SIMPLE: com.google.common.base.Predicate<Aggregate> =
            com.google.common.base.Predicate<Aggregate> { aggregate: Aggregate -> isSimple(aggregate) }

        @SuppressWarnings("Guava")
        @Deprecated // to be converted to Java Predicate before 2.0
        val NO_INDICATOR: com.google.common.base.Predicate<Aggregate> =
            com.google.common.base.Predicate<Aggregate> { aggregate: Aggregate? -> noIndicator(aggregate) }

        @SuppressWarnings("Guava")
        @Deprecated // to be converted to Java Predicate before 2.0
        val IS_NOT_GRAND_TOTAL: com.google.common.base.Predicate<Aggregate> =
            com.google.common.base.Predicate<Aggregate> { aggregate: Aggregate -> isNotGrandTotal(aggregate) }

        /** Used internally; will removed when [.indicator] is removed,
         * before 2.0.  */
        @Experimental
        fun checkIndicator(indicator: Boolean) {
            Preconditions.checkArgument(
                !indicator,
                "indicator is no longer supported; use GROUPING function instead"
            )
        }

        fun isNotGrandTotal(aggregate: Aggregate): Boolean {
            return aggregate.groupCount > 0
        }

        @Deprecated // to be removed before 2.0
        fun noIndicator(aggregate: Aggregate?): Boolean {
            return true
        }

        private fun isPredicate(input: RelNode, index: Int): Boolean {
            val type: RelDataType = input.getRowType().getFieldList().get(index).getType()
            return (type.getSqlTypeName() === SqlTypeName.BOOLEAN
                    && !type.isNullable())
        }

        /**
         * Computes the row type of an `Aggregate` before it exists.
         *
         * @param typeFactory Type factory
         * @param inputRowType Input row type
         * @param indicator Deprecated, always false
         * @param groupSet Bit set of grouping fields
         * @param groupSets List of all grouping sets; null for just `groupSet`
         * @param aggCalls Collection of calls to aggregate functions
         * @return Row type of the aggregate
         */
        fun deriveRowType(
            typeFactory: RelDataTypeFactory,
            inputRowType: RelDataType, indicator: Boolean,
            groupSet: ImmutableBitSet, @Nullable groupSets: List<ImmutableBitSet>?,
            aggCalls: List<AggregateCall?>?
        ): RelDataType {
            val groupList: List<Integer> = groupSet.asList()
            assert(groupList.size() === groupSet.cardinality())
            val builder: RelDataTypeFactory.Builder = typeFactory.builder()
            val fieldList: List<RelDataTypeField> = inputRowType.getFieldList()
            val containedNames: Set<String> = HashSet()
            for (groupKey in groupList) {
                val field: RelDataTypeField = fieldList[groupKey]
                containedNames.add(field.getName())
                builder.add(field)
                if (groupSets != null && !ImmutableBitSet.allContain(groupSets, groupKey)) {
                    builder.nullable(true)
                }
            }
            checkIndicator(indicator)
            for (aggCall in Ord.zip(aggCalls)) {
                val base: String
                base = if (aggCall.e.name != null) {
                    aggCall.e.name
                } else {
                    "\$f" + (groupList.size() + aggCall.i)
                }
                var name = base
                var i = 0
                while (containedNames.contains(name)) {
                    name = base + "_" + i++
                }
                containedNames.add(name)
                builder.add(name, aggCall.e.type)
            }
            return builder.build()
        }
    }
}
