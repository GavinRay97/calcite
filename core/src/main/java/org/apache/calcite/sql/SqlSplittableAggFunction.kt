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
package org.apache.calcite.sql

import org.apache.calcite.rel.RelCollations

/**
 * Aggregate function that can be split into partial aggregates.
 *
 *
 * For example, `COUNT(x)` can be split into `COUNT(x)` on
 * subsets followed by `SUM` to combine those counts.
 */
interface SqlSplittableAggFunction {
    fun split(
        aggregateCall: AggregateCall?,
        mapping: Mappings.TargetMapping?
    ): AggregateCall?

    /** Called to generate an aggregate for the other side of the join
     * than the side aggregate call's arguments come from. Returns null if
     * no aggregate is required.  */
    @Nullable
    fun other(typeFactory: RelDataTypeFactory?, e: AggregateCall?): AggregateCall?

    /** Generates an aggregate call to merge sub-totals.
     *
     *
     * Most implementations will add a single aggregate call to
     * `aggCalls`, and return a [RexInputRef] that points to it.
     *
     * @param rexBuilder Rex builder
     * @param extra Place to define extra input expressions
     * @param offset Offset due to grouping columns (and indicator columns if
     * applicable)
     * @param inputRowType Input row type
     * @param aggregateCall Source aggregate call
     * @param leftSubTotal Ordinal of the sub-total coming from the left side of
     * the join, or -1 if there is no such sub-total
     * @param rightSubTotal Ordinal of the sub-total coming from the right side
     * of the join, or -1 if there is no such sub-total
     *
     * @return Aggregate call
     */
    fun topSplit(
        rexBuilder: RexBuilder?, extra: Registry<RexNode?>?,
        offset: Int, inputRowType: RelDataType?, aggregateCall: AggregateCall?,
        leftSubTotal: Int, rightSubTotal: Int
    ): AggregateCall?

    /** Generates an expression for the value of the aggregate function when
     * applied to a single row.
     *
     *
     * For example, if there is one row:
     *
     *  * `SUM(x)` is `x`
     *  * `MIN(x)` is `x`
     *  * `MAX(x)` is `x`
     *  * `COUNT(x)` is `CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END 1`
     * which can be simplified to `1` if `x` is never null
     *  * `COUNT(*)` is 1
     *
     *
     * @param rexBuilder Rex builder
     * @param inputRowType Input row type
     * @param aggregateCall Aggregate call
     *
     * @return Expression for single row
     */
    fun singleton(
        rexBuilder: RexBuilder?, inputRowType: RelDataType?,
        aggregateCall: AggregateCall?
    ): RexNode?

    /**
     * Merge top and bottom aggregate calls into a single aggregate call,
     * if they are legit to merge.
     *
     *
     * SUM of SUM becomes SUM; SUM of COUNT becomes COUNT;
     * MAX of MAX becomes MAX; MIN of MIN becomes MIN.
     * AVG of AVG would not match, nor would COUNT of COUNT.
     *
     * @param top top aggregate call
     * @param bottom bottom aggregate call
     * @return Merged aggregate call, null if fails to merge aggregate calls
     */
    @Nullable
    fun merge(top: AggregateCall?, bottom: AggregateCall?): AggregateCall?

    /** Collection in which one can register an element. Registering may return
     * a reference to an existing element.
     *
     * @param <E> element type
    </E> */
    interface Registry<E> {
        fun register(e: E): Int
    }

    /** Splitting strategy for `COUNT`.
     *
     *
     * COUNT splits into itself followed by SUM. (Actually
     * SUM0, because the total needs to be 0, not null, if there are 0 rows.)
     * This rule works for any number of arguments to COUNT, including COUNT(*).
     */
    class CountSplitter : SqlSplittableAggFunction {
        @Override
        override fun split(
            aggregateCall: AggregateCall,
            mapping: Mappings.TargetMapping?
        ): AggregateCall {
            return aggregateCall.transform(mapping)
        }

        @Override
        @Nullable
        override fun other(
            typeFactory: RelDataTypeFactory,
            e: AggregateCall?
        ): AggregateCall {
            return AggregateCall.create(
                SqlStdOperatorTable.COUNT, false, false,
                false, ImmutableIntList.of(), -1, null, RelCollations.EMPTY,
                typeFactory.createSqlType(SqlTypeName.BIGINT), null
            )
        }

        @Override
        override fun topSplit(
            rexBuilder: RexBuilder,
            extra: Registry<RexNode?>, offset: Int, inputRowType: RelDataType?,
            aggregateCall: AggregateCall, leftSubTotal: Int, rightSubTotal: Int
        ): AggregateCall {
            val merges: List<RexNode> = ArrayList()
            if (leftSubTotal >= 0) {
                merges.add(
                    rexBuilder.makeInputRef(aggregateCall.type, leftSubTotal)
                )
            }
            if (rightSubTotal >= 0) {
                merges.add(
                    rexBuilder.makeInputRef(aggregateCall.type, rightSubTotal)
                )
            }
            val node: RexNode
            node = when (merges.size()) {
                1 -> merges[0]
                2 -> rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges)
                else -> throw AssertionError("unexpected count $merges")
            }
            val ordinal = extra.register(node)
            return AggregateCall.create(
                SqlStdOperatorTable.SUM0, false, false,
                false, ImmutableList.of(ordinal), -1, aggregateCall.distinctKeys,
                aggregateCall.collation, aggregateCall.type, aggregateCall.name
            )
        }

        /**
         * {@inheritDoc}
         *
         *
         * `COUNT(*)`, and `COUNT` applied to all NOT NULL arguments,
         * become `1`; otherwise
         * `CASE WHEN arg0 IS NOT NULL THEN 1 ELSE 0 END`.
         */
        @Override
        override fun singleton(
            rexBuilder: RexBuilder, inputRowType: RelDataType,
            aggregateCall: AggregateCall
        ): RexNode {
            val predicates: List<RexNode> = ArrayList()
            for (arg in aggregateCall.getArgList()) {
                val type: RelDataType = inputRowType.getFieldList().get(arg).getType()
                if (type.isNullable()) {
                    predicates.add(
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.IS_NOT_NULL,
                            rexBuilder.makeInputRef(type, arg)
                        )
                    )
                }
            }
            val predicate: RexNode = RexUtil.composeConjunction(rexBuilder, predicates, true)
            val rexOne: RexNode = rexBuilder.makeExactLiteral(
                BigDecimal.ONE, aggregateCall.getType()
            )
            return if (predicate == null) {
                rexOne
            } else {
                rexBuilder.makeCall(
                    SqlStdOperatorTable.CASE, predicate, rexOne,
                    rexBuilder.makeExactLiteral(BigDecimal.ZERO, aggregateCall.getType())
                )
            }
        }

        @Override
        @Nullable
        override fun merge(top: AggregateCall, bottom: AggregateCall): AggregateCall? {
            return if (bottom.getAggregation().getKind() === SqlKind.COUNT
                && (top.getAggregation().getKind() === SqlKind.SUM
                        || top.getAggregation().getKind() === SqlKind.SUM0)
            ) {
                AggregateCall.create(
                    bottom.getAggregation(),
                    bottom.isDistinct(), bottom.isApproximate(), false,
                    bottom.getArgList(), bottom.filterArg,
                    bottom.distinctKeys, bottom.getCollation(),
                    bottom.getType(), top.getName()
                )
            } else {
                null
            }
        }

        companion object {
            val INSTANCE = CountSplitter()
        }
    }

    /** Aggregate function that splits into two applications of itself.
     *
     *
     * Examples are MIN and MAX.  */
    class SelfSplitter : SqlSplittableAggFunction {
        @Override
        override fun singleton(
            rexBuilder: RexBuilder,
            inputRowType: RelDataType, aggregateCall: AggregateCall
        ): RexNode {
            val arg: Int = aggregateCall.getArgList().get(0)
            val field: RelDataTypeField = inputRowType.getFieldList().get(arg)
            return rexBuilder.makeInputRef(field.getType(), arg)
        }

        @Override
        override fun split(
            aggregateCall: AggregateCall,
            mapping: Mappings.TargetMapping?
        ): AggregateCall {
            return aggregateCall.transform(mapping)
        }

        @Override
        @Nullable
        override fun other(
            typeFactory: RelDataTypeFactory?,
            e: AggregateCall?
        ): AggregateCall? {
            return null // no aggregate function required on other side
        }

        @Override
        override fun topSplit(
            rexBuilder: RexBuilder?,
            extra: Registry<RexNode?>?, offset: Int, inputRowType: RelDataType?,
            aggregateCall: AggregateCall, leftSubTotal: Int, rightSubTotal: Int
        ): AggregateCall {
            assert(leftSubTotal >= 0 != rightSubTotal >= 0)
            assert(aggregateCall.collation.getFieldCollations().isEmpty())
            val arg = if (leftSubTotal >= 0) leftSubTotal else rightSubTotal
            return aggregateCall.withArgList(ImmutableIntList.of(arg))
        }

        @Override
        @Nullable
        override fun merge(top: AggregateCall, bottom: AggregateCall): AggregateCall? {
            return if (top.getAggregation().getKind() === bottom.getAggregation().getKind()) {
                AggregateCall.create(
                    bottom.getAggregation(),
                    bottom.isDistinct(), bottom.isApproximate(), false,
                    bottom.getArgList(), bottom.filterArg,
                    bottom.distinctKeys, bottom.getCollation(),
                    bottom.getType(), top.getName()
                )
            } else {
                null
            }
        }

        companion object {
            val INSTANCE = SelfSplitter()
        }
    }

    /** Common splitting strategy for `SUM` and `SUM0` functions.  */
    abstract class AbstractSumSplitter : SqlSplittableAggFunction {
        @Override
        override fun singleton(
            rexBuilder: RexBuilder,
            inputRowType: RelDataType, aggregateCall: AggregateCall
        ): RexNode {
            val arg: Int = aggregateCall.getArgList().get(0)
            val field: RelDataTypeField = inputRowType.getFieldList().get(arg)
            val fieldType: RelDataType = field.getType()
            return rexBuilder.makeInputRef(fieldType, arg)
        }

        @Override
        override fun split(
            aggregateCall: AggregateCall,
            mapping: Mappings.TargetMapping?
        ): AggregateCall {
            return aggregateCall.transform(mapping)
        }

        @Override
        @Nullable
        override fun other(
            typeFactory: RelDataTypeFactory,
            e: AggregateCall?
        ): AggregateCall {
            return AggregateCall.create(
                SqlStdOperatorTable.COUNT, false, false,
                false, ImmutableIntList.of(), -1, null, RelCollations.EMPTY,
                typeFactory.createSqlType(SqlTypeName.BIGINT), null
            )
        }

        @Override
        override fun topSplit(
            rexBuilder: RexBuilder,
            extra: Registry<RexNode?>, offset: Int, inputRowType: RelDataType,
            aggregateCall: AggregateCall, leftSubTotal: Int, rightSubTotal: Int
        ): AggregateCall {
            val merges: List<RexNode> = ArrayList()
            val fieldList: List<RelDataTypeField> = inputRowType.getFieldList()
            if (leftSubTotal >= 0) {
                val type: RelDataType = fieldList[leftSubTotal].getType()
                merges.add(rexBuilder.makeInputRef(type, leftSubTotal))
            }
            if (rightSubTotal >= 0) {
                val type: RelDataType = fieldList[rightSubTotal].getType()
                merges.add(rexBuilder.makeInputRef(type, rightSubTotal))
            }
            var node: RexNode
            when (merges.size()) {
                1 -> node = merges[0]
                2 -> {
                    node = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges)
                    node = rexBuilder.makeAbstractCast(aggregateCall.type, node)
                }
                else -> throw AssertionError("unexpected count $merges")
            }
            val ordinal = extra.register(node)
            return AggregateCall.create(
                mergeAggFunctionOfTopSplit, false, false,
                false, ImmutableList.of(ordinal), -1,
                aggregateCall.distinctKeys, aggregateCall.collation,
                aggregateCall.type, aggregateCall.name
            )
        }

        @Override
        @Nullable
        override fun merge(top: AggregateCall, bottom: AggregateCall): AggregateCall? {
            val topKind: SqlKind = top.getAggregation().getKind()
            return if (topKind === bottom.getAggregation().getKind()
                && (topKind === SqlKind.SUM
                        || topKind === SqlKind.SUM0)
            ) {
                AggregateCall.create(
                    bottom.getAggregation(),
                    bottom.isDistinct(), bottom.isApproximate(), false,
                    bottom.getArgList(), bottom.filterArg,
                    bottom.distinctKeys, bottom.getCollation(),
                    bottom.getType(), top.getName()
                )
            } else {
                null
            }
        }

        protected abstract val mergeAggFunctionOfTopSplit: SqlAggFunction?
    }

    /** Splitting strategy for `SUM` function.  */
    class SumSplitter : AbstractSumSplitter() {
        @get:Override
        override val mergeAggFunctionOfTopSplit: SqlAggFunction
            get() = SqlStdOperatorTable.SUM

        companion object {
            val INSTANCE = SumSplitter()
        }
    }

    /** Splitting strategy for `SUM0` function.  */
    class Sum0Splitter : AbstractSumSplitter() {
        @get:Override
        override val mergeAggFunctionOfTopSplit: SqlAggFunction
            get() = SqlStdOperatorTable.SUM0

        @Override
        override fun singleton(
            rexBuilder: RexBuilder,
            inputRowType: RelDataType, aggregateCall: AggregateCall
        ): RexNode {
            val arg: Int = aggregateCall.getArgList().get(0)
            val type: RelDataType = inputRowType.getFieldList().get(arg).getType()
            val typeFactory: RelDataTypeFactory = rexBuilder.getTypeFactory()
            val type1: RelDataType = typeFactory.getTypeSystem().deriveSumType(typeFactory, type)
            val inputRef: RexNode = rexBuilder.makeInputRef(type1, arg)
            return if (type.isNullable()) {
                rexBuilder.makeCall(
                    SqlStdOperatorTable.COALESCE, inputRef,
                    rexBuilder.makeExactLiteral(BigDecimal.ZERO, type)
                )
            } else {
                inputRef
            }
        }

        companion object {
            val INSTANCE = Sum0Splitter()
        }
    }
}
