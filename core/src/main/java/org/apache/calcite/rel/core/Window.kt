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
 * A relational expression representing a set of window aggregates.
 *
 *
 * A Window can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 *
 * Each [Window.Group] has a set of
 * [org.apache.calcite.rex.RexOver] objects.
 *
 *
 * Created by [org.apache.calcite.rel.rules.ProjectToWindowRule].
 */
abstract class Window protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
    constants: List<RexLiteral?>?, rowType: RelDataType, groups: List<Group?>?
) : SingleRel(cluster, traitSet, input) {
    val groups: ImmutableList<Group>
    val constants: ImmutableList<RexLiteral>

    /**
     * Creates a window relational expression.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param input   Input relational expression
     * @param constants List of constants that are additional inputs
     * @param rowType Output row type
     * @param groups Windows
     */
    init {
        this.constants = ImmutableList.copyOf(constants)
        assert(rowType != null)
        rowType = rowType
        this.groups = ImmutableList.copyOf(groups)
    }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        // In the window specifications, an aggregate call such as
        // 'SUM(RexInputRef #10)' refers to expression #10 of inputProgram.
        // (Not its projections.)
        val childRowType: RelDataType = getInput().getRowType()
        val childFieldCount: Int = childRowType.getFieldCount()
        val inputSize: Int = childFieldCount + constants.size()
        val inputTypes: List<RelDataType> = object : AbstractList<RelDataType?>() {
            @Override
            operator fun get(index: Int): RelDataType {
                return if (index < childFieldCount) childRowType.getFieldList().get(index).getType() else constants.get(
                    index - childFieldCount
                ).getType()
            }

            @Override
            fun size(): Int {
                return inputSize
            }
        }
        val checker = RexChecker(inputTypes, context, litmus)
        var count = 0
        for (group in groups) {
            for (over in group.aggCalls) {
                ++count
                if (!checker.isValid(over)) {
                    return litmus.fail(null)
                }
            }
        }
        return if (count == 0) {
            litmus.fail("empty")
        } else litmus.succeed()
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        for (window in Ord.zip(groups)) {
            pw.item("window#" + window.i, window.e.toString())
        }
        return pw
    }

    /**
     * Returns constants that are additional inputs of current relation.
     * @return constants that are additional inputs of current relation
     */
    fun getConstants(): List<RexLiteral> {
        return constants
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        // Cost is proportional to the number of rows and the number of
        // components (groups and aggregate functions). There is
        // no I/O cost.
        //
        // TODO #1. Add memory cost.
        // TODO #2. MIN and MAX have higher CPU cost than SUM and COUNT.
        val rowsIn: Double = mq.getRowCount(getInput())
        var count: Int = groups.size()
        for (group in groups) {
            count += group.aggCalls.size()
        }
        return planner.getCostFactory().makeCost(rowsIn, rowsIn * count, 0)
    }

    /**
     * Group of windowed aggregate calls that have the same window specification.
     *
     *
     * The specification is defined by an upper and lower bound, and
     * also has zero or more partitioning columns.
     *
     *
     * A window is either logical or physical. A physical window is measured
     * in terms of row count. A logical window is measured in terms of rows
     * within a certain distance from the current sort key.
     *
     *
     * For example:
     *
     *
     *  * `ROWS BETWEEN 10 PRECEDING and 5 FOLLOWING` is a physical
     * window with an upper and lower bound;
     *  * `RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND UNBOUNDED
     * FOLLOWING` is a logical window with only a lower bound;
     *  * `RANGE INTERVAL '10' MINUTES PRECEDING` (which is
     * equivalent to `RANGE BETWEEN INTERVAL '10' MINUTES PRECEDING AND
     * CURRENT ROW`) is a logical window with an upper and lower bound.
     *
     */
    class Group(
        keys: ImmutableBitSet?,
        isRows: Boolean,
        lowerBound: RexWindowBound?,
        upperBound: RexWindowBound?,
        orderKeys: RelCollation?,
        aggCalls: List<RexWinAggCall?>?
    ) {
        val keys: ImmutableBitSet
        val isRows: Boolean
        val lowerBound: RexWindowBound
        val upperBound: RexWindowBound
        val orderKeys: RelCollation
        private val digest: String

        /**
         * List of [Window.RexWinAggCall]
         * objects, each of which is a call to a
         * [org.apache.calcite.sql.SqlAggFunction].
         */
        val aggCalls: ImmutableList<RexWinAggCall>

        init {
            this.keys = Objects.requireNonNull(keys, "keys")
            this.isRows = isRows
            this.lowerBound = Objects.requireNonNull(lowerBound, "lowerBound")
            this.upperBound = Objects.requireNonNull(upperBound, "upperBound")
            this.orderKeys = Objects.requireNonNull(orderKeys, "orderKeys")
            this.aggCalls = ImmutableList.copyOf(aggCalls)
            digest = computeString()
        }

        @Override
        override fun toString(): String {
            return digest
        }

        @RequiresNonNull(["keys", "orderKeys", "lowerBound", "upperBound", "aggCalls"])
        private fun computeString(): String {
            val buf = StringBuilder("window(")
            val i: Int = buf.length()
            if (!keys.isEmpty()) {
                buf.append("partition ")
                buf.append(keys)
            }
            if (!orderKeys.getFieldCollations().isEmpty()) {
                buf.append(if (buf.length() === i) "order by " else " order by ")
                buf.append(orderKeys)
            }
            if (orderKeys.getFieldCollations().isEmpty()
                && lowerBound.isUnbounded()
                && lowerBound.isPreceding()
                && upperBound.isUnbounded()
                && upperBound.isFollowing()
            ) {
                // skip bracket if no ORDER BY, and if bracket is the default,
                // "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                // which is equivalent to
                // "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
            } else if (!orderKeys.getFieldCollations().isEmpty()
                && lowerBound.isUnbounded()
                && lowerBound.isPreceding()
                && upperBound.isCurrentRow()
                && !isRows
            ) {
                // skip bracket if there is ORDER BY, and if bracket is the default,
                // "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                // which is NOT equivalent to
                // "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            } else {
                buf.append(if (isRows) " rows " else " range ")
                buf.append("between ")
                buf.append(lowerBound)
                buf.append(" and ")
                buf.append(upperBound)
            }
            if (!aggCalls.isEmpty()) {
                buf.append(if (buf.length() === i) "aggs " else " aggs ")
                buf.append(aggCalls)
            }
            buf.append(")")
            return buf.toString()
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (this === obj
                    || obj is Group
                    && digest.equals((obj as Group).digest))
        }

        @Override
        override fun hashCode(): Int {
            return digest.hashCode()
        }

        fun collation(): RelCollation {
            return orderKeys
        }

        /**
         * Returns if the window is guaranteed to have rows.
         * This is useful to refine data type of window aggregates.
         * For instance sum(non-nullable) over (empty window) is NULL.
         * @return true when the window is non-empty
         * @see org.apache.calcite.sql.SqlWindow.isAlwaysNonEmpty
         * @see org.apache.calcite.sql.SqlOperatorBinding.getGroupCount
         * @see org.apache.calcite.sql.validate.SqlValidatorImpl.resolveWindow
         */
        val isAlwaysNonEmpty: Boolean
            get() {
                val lowerKey: Int = lowerBound.getOrderKey()
                val upperKey: Int = upperBound.getOrderKey()
                return lowerKey > -1 && lowerKey <= upperKey
            }

        /**
         * Presents a view of the [RexWinAggCall] list as a list of
         * [AggregateCall].
         */
        fun getAggregateCalls(windowRel: Window): List<AggregateCall> {
            val fieldNames: List<String> = Util.skip(
                windowRel.getRowType().getFieldNames(),
                windowRel.getInput().getRowType().getFieldCount()
            )
            return object : AbstractList<AggregateCall?>() {
                @Override
                fun size(): Int {
                    return aggCalls.size()
                }

                @Override
                operator fun get(index: Int): AggregateCall {
                    val aggCall: RexWinAggCall = aggCalls.get(index)
                    val op: SqlAggFunction = aggCall.getOperator() as SqlAggFunction
                    return AggregateCall.create(
                        op, aggCall.distinct, false,
                        aggCall.ignoreNulls, getProjectOrdinals(aggCall.getOperands()),
                        -1, null, RelCollations.EMPTY,
                        aggCall.getType(), fieldNames[aggCall.ordinal]
                    )
                }
            }
        }
    }

    /**
     * A call to a windowed aggregate function.
     *
     *
     * Belongs to a [Window.Group].
     *
     *
     * It's a bastard son of a [org.apache.calcite.rex.RexCall]; similar
     * enough that it gets visited by a [org.apache.calcite.rex.RexVisitor],
     * but it also has some extra data members.
     */
    class RexWinAggCall
    /**
     * Creates a RexWinAggCall.
     *
     * @param aggFun   Aggregate function
     * @param type     Result type
     * @param operands Operands to call
     * @param ordinal  Ordinal within its partition
     * @param distinct Eliminate duplicates before applying aggregate function
     */(
        aggFun: SqlAggFunction?,
        type: RelDataType?,
        operands: List<RexNode?>?,
        /**
         * Ordinal of this aggregate within its partition.
         */
        val ordinal: Int,
        /** Whether to eliminate duplicates before applying aggregate function.  */
        val distinct: Boolean,
        /** Whether to ignore nulls.  */
        val ignoreNulls: Boolean
    ) : RexCall(type, aggFun, operands) {
        @Deprecated // to be removed before 2.0
        constructor(
            aggFun: SqlAggFunction?,
            type: RelDataType?,
            operands: List<RexNode?>?,
            ordinal: Int,
            distinct: Boolean
        ) : this(aggFun, type, operands, ordinal, distinct, false) {
        }

        @Override
        override fun equals(@Nullable o: Object?): Boolean {
            if (this === o) {
                return true
            }
            if (o == null || getClass() !== o.getClass()) {
                return false
            }
            if (!super.equals(o)) {
                return false
            }
            val that = o as RexWinAggCall
            return ordinal == that.ordinal && distinct == that.distinct && ignoreNulls == that.ignoreNulls
        }

        @Override
        override fun hashCode(): Int {
            if (hash === 0) {
                hash = Objects.hash(super.hashCode(), ordinal, distinct, ignoreNulls)
            }
            return hash
        }

        @Override
        fun clone(type: RelDataType?, operands: List<RexNode?>?): RexCall {
            return super.clone(type, operands)
        }
    }

    companion object {
        fun getProjectOrdinals(exprs: List<RexNode>): ImmutableIntList {
            return ImmutableIntList.copyOf(
                object : AbstractList<Integer?>() {
                    @Override
                    operator fun get(index: Int): Integer {
                        return (exprs[index] as RexSlot).getIndex()
                    }

                    @Override
                    fun size(): Int {
                        return exprs.size()
                    }
                })
        }

        fun getCollation(
            collations: List<RexFieldCollation>
        ): RelCollation {
            return RelCollations.of(
                object : AbstractList<RelFieldCollation?>() {
                    @Override
                    operator fun get(index: Int): RelFieldCollation {
                        val collation: RexFieldCollation = collations[index]
                        return RelFieldCollation(
                            (collation.left as RexLocalRef).getIndex(),
                            collation.getDirection(),
                            collation.getNullDirection()
                        )
                    }

                    @Override
                    fun size(): Int {
                        return collations.size()
                    }
                })
        }
    }
}
