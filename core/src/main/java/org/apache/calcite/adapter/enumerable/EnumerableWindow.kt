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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.adapter.enumerable.impl.WinAggAddContextImpl

/** Implementation of [org.apache.calcite.rel.core.Window] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableWindow
/** Creates an EnumerableWindowRel.  */
internal constructor(
    cluster: RelOptCluster?, traits: RelTraitSet?, child: RelNode?,
    constants: List<RexLiteral?>?, rowType: RelDataType?, groups: List<Group?>?
) : Window(cluster, traits, child, constants, rowType, groups), EnumerableRel {
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return EnumerableWindow(
            getCluster(), traitSet, sole(inputs),
            constants, getRowType(), groups
        )
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner?,
        mq: RelMetadataQuery?
    ): RelOptCost? {
        val cost: RelOptCost = super.computeSelfCost(planner, mq) ?: return null
        return cost.multiplyBy(EnumerableConvention.COST_MULTIPLIER)
    }

    /** Implementation of [RexToLixTranslator.InputGetter]
     * suitable for generating implementations of windowed aggregate
     * functions.  */
    private class WindowRelInputGetter(
        row: Expression,
        rowPhysType: PhysType, actualInputFieldCount: Int,
        constants: List<Expression>
    ) : RexToLixTranslator.InputGetter {
        private val row: Expression
        private val rowPhysType: PhysType
        private val actualInputFieldCount: Int
        private val constants: List<Expression>

        init {
            this.row = row
            this.rowPhysType = rowPhysType
            this.actualInputFieldCount = actualInputFieldCount
            this.constants = constants
        }

        @Override
        override fun field(list: BlockBuilder, index: Int, @Nullable storageType: Type?): Expression {
            if (index < actualInputFieldCount) {
                val current: Expression = list.append("current", row)
                return rowPhysType.fieldReference(current, index, storageType)
            }
            return constants[index - actualInputFieldCount]
        }
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val typeFactory: JavaTypeFactory = implementor.getTypeFactory()
        val child: EnumerableRel = getInput() as EnumerableRel
        val builder = BlockBuilder()
        val result: Result = implementor.visitChild(this, 0, child, pref)
        var source_: Expression = builder.append("source", result.block)
        val translatedConstants: List<Expression> = ArrayList(constants.size())
        for (constant in constants) {
            translatedConstants.add(
                RexToLixTranslator.translateLiteral(
                    constant, constant.getType(),
                    typeFactory, RexImpTable.NullAs.NULL
                )
            )
        }
        var inputPhysType: PhysType = result.physType
        val prevStart: ParameterExpression =
            Expressions.parameter(Int::class.javaPrimitiveType, builder.newName("prevStart"))
        val prevEnd: ParameterExpression =
            Expressions.parameter(Int::class.javaPrimitiveType, builder.newName("prevEnd"))
        builder.add(Expressions.declare(0, prevStart, null))
        builder.add(Expressions.declare(0, prevEnd, null))
        for (windowIdx in 0 until groups.size()) {
            val group: Group = groups.get(windowIdx)
            // Comparator:
            // final Comparator<JdbcTest.Employee> comparator =
            //    new Comparator<JdbcTest.Employee>() {
            //      public int compare(JdbcTest.Employee o1,
            //          JdbcTest.Employee o2) {
            //        return Integer.compare(o1.empid, o2.empid);
            //      }
            //    };
            val comparator_: Expression = builder.append(
                "comparator",
                inputPhysType.generateComparator(
                    group.collation()
                )
            )
            val partitionIterator: Pair<Expression, Expression> = getPartitionIterator(
                builder, source_, inputPhysType, group,
                comparator_
            )
            val collectionExpr: Expression = partitionIterator.left
            val iterator_: Expression = partitionIterator.right
            val aggs: List<AggImpState> = ArrayList()
            val aggregateCalls: List<AggregateCall> = group.getAggregateCalls(this)
            for (aggIdx in 0 until aggregateCalls.size()) {
                val call: AggregateCall = aggregateCalls[aggIdx]
                if (call.ignoreNulls()) {
                    throw UnsupportedOperationException("IGNORE NULLS not supported")
                }
                aggs.add(AggImpState(aggIdx, call, true))
            }

            // The output from this stage is the input plus the aggregate functions.
            val typeBuilder: RelDataTypeFactory.Builder = typeFactory.builder()
            typeBuilder.addAll(inputPhysType.getRowType().getFieldList())
            for (agg in aggs) {
                // CALCITE-4326
                val name: String = requireNonNull(
                    agg.call.name
                ) { "agg.call.name for " + agg.call }
                typeBuilder.add(name, agg.call.type)
            }
            val outputRowType: RelDataType = typeBuilder.build()
            val outputPhysType: PhysType = PhysTypeImpl.of(
                typeFactory, outputRowType, pref.prefer(result.format)
            )
            val list_: Expression = builder.append(
                "list",
                Expressions.new_(
                    ArrayList::class.java,
                    Expressions.call(
                        collectionExpr, BuiltInMethod.COLLECTION_SIZE.method
                    )
                ),
                false
            )
            val collationKey: Pair<Expression, Expression> =
                getRowCollationKey(builder, inputPhysType, group, windowIdx)
            val keySelector: Expression = collationKey.left
            val keyComparator: Expression = collationKey.right
            val builder3 = BlockBuilder()
            val rows_: Expression = builder3.append(
                "rows",
                Expressions.convert_(
                    Expressions.call(
                        iterator_, BuiltInMethod.ITERATOR_NEXT.method
                    ),
                    Array<Object>::class.java
                ),
                false
            )
            builder3.add(
                Expressions.statement(
                    Expressions.assign(prevStart, Expressions.constant(-1))
                )
            )
            builder3.add(
                Expressions.statement(
                    Expressions.assign(
                        prevEnd,
                        Expressions.constant(Integer.MAX_VALUE)
                    )
                )
            )
            val builder4 = BlockBuilder()
            val i_: ParameterExpression = Expressions.parameter(Int::class.javaPrimitiveType, builder4.newName("i"))
            val row_: Expression = builder4.append(
                "row",
                EnumUtils.convert(
                    Expressions.arrayIndex(rows_, i_),
                    inputPhysType.getJavaRowType()
                )
            )
            val inputGetter: RexToLixTranslator.InputGetter = WindowRelInputGetter(
                row_, inputPhysType,
                result.physType.getRowType().getFieldCount(),
                translatedConstants
            )
            val translator: RexToLixTranslator = RexToLixTranslator.forAggregation(
                typeFactory, builder4,
                inputGetter, implementor.getConformance()
            )
            val outputRow: List<Expression> = ArrayList()
            val fieldCountWithAggResults: Int = inputPhysType.getRowType().getFieldCount()
            for (i in 0 until fieldCountWithAggResults) {
                outputRow.add(
                    inputPhysType.fieldReference(
                        row_, i,
                        outputPhysType.getJavaFieldType(i)
                    )
                )
            }
            declareAndResetState(
                typeFactory, builder, result, windowIdx, aggs,
                outputPhysType, outputRow
            )

            // There are assumptions that minX==0. If ever change this, look for
            // frameRowCount, bounds checking, etc
            val minX: Expression = Expressions.constant(0)
            val partitionRowCount: Expression = builder3.append("partRows", Expressions.field(rows_, "length"))
            val maxX: Expression = builder3.append(
                "maxX",
                Expressions.subtract(
                    partitionRowCount, Expressions.constant(1)
                )
            )
            val startUnchecked: Expression = builder4.append(
                "start",
                translateBound(
                    translator, i_, row_, minX, maxX, rows_,
                    group, true, inputPhysType, keySelector, keyComparator
                )
            )
            val endUnchecked: Expression = builder4.append(
                "end",
                translateBound(
                    translator, i_, row_, minX, maxX, rows_,
                    group, false, inputPhysType, keySelector, keyComparator
                )
            )
            val startX: Expression
            val endX: Expression
            val hasRows: Expression
            if (group.isAlwaysNonEmpty()) {
                startX = startUnchecked
                endX = endUnchecked
                hasRows = Expressions.constant(true)
            } else {
                val startTmp: Expression =
                    if (group.lowerBound.isUnbounded() || startUnchecked === i_) startUnchecked else builder4.append(
                        "startTmp",
                        Expressions.call(
                            null, BuiltInMethod.MATH_MAX.method,
                            startUnchecked, minX
                        )
                    )
                val endTmp: Expression =
                    if (group.upperBound.isUnbounded() || endUnchecked === i_) endUnchecked else builder4.append(
                        "endTmp",
                        Expressions.call(
                            null, BuiltInMethod.MATH_MIN.method,
                            endUnchecked, maxX
                        )
                    )
                val startPe: ParameterExpression = Expressions.parameter(
                    0, Int::class.javaPrimitiveType,
                    builder4.newName("startChecked")
                )
                val endPe: ParameterExpression = Expressions.parameter(
                    0, Int::class.javaPrimitiveType,
                    builder4.newName("endChecked")
                )
                builder4.add(Expressions.declare(Modifier.FINAL, startPe, null))
                builder4.add(Expressions.declare(Modifier.FINAL, endPe, null))
                hasRows = builder4.append(
                    "hasRows",
                    Expressions.lessThanOrEqual(startTmp, endTmp)
                )
                builder4.add(
                    Expressions.ifThenElse(
                        hasRows,
                        Expressions.block(
                            Expressions.statement(
                                Expressions.assign(startPe, startTmp)
                            ),
                            Expressions.statement(
                                Expressions.assign(endPe, endTmp)
                            )
                        ),
                        Expressions.block(
                            Expressions.statement(
                                Expressions.assign(startPe, Expressions.constant(-1))
                            ),
                            Expressions.statement(
                                Expressions.assign(endPe, Expressions.constant(-1))
                            )
                        )
                    )
                )
                startX = startPe
                endX = endPe
            }
            val builder5 = BlockBuilder(true, builder4)
            val rowCountWhenNonEmpty: BinaryExpression = Expressions.add(
                if (startX === minX) endX else Expressions.subtract(endX, startX),
                Expressions.constant(1)
            )
            val frameRowCount: Expression
            frameRowCount = if (hasRows.equals(Expressions.constant(true))) {
                builder4.append("totalRows", rowCountWhenNonEmpty)
            } else {
                builder4.append(
                    "totalRows",
                    Expressions.condition(
                        hasRows, rowCountWhenNonEmpty,
                        Expressions.constant(0)
                    )
                )
            }
            val actualStart: ParameterExpression = Expressions.parameter(
                0, Int::class.javaPrimitiveType, builder5.newName("actualStart")
            )
            val builder6 = BlockBuilder(true, builder5)
            builder6.add(
                Expressions.statement(Expressions.assign(actualStart, startX))
            )
            for (agg in aggs) {
                val aggState: List<Expression> = requireNonNull(agg.state, "agg.state")
                agg.implementor.implementReset(
                    requireNonNull(agg.context, "agg.context"),
                    WinAggResetContextImpl(
                        builder6, aggState, i_, startX, endX,
                        hasRows, frameRowCount, partitionRowCount
                    )
                )
            }
            val lowerBoundCanChange: Expression =
                if (group.lowerBound.isUnbounded() && group.lowerBound.isPreceding()) Expressions.constant(false) else Expressions.notEqual(
                    startX,
                    prevStart
                )
            val needRecomputeWindow: Expression = Expressions.orElse(
                lowerBoundCanChange,
                Expressions.lessThan(endX, prevEnd)
            )
            val resetWindowState: BlockStatement = builder6.toBlock()
            if (resetWindowState.statements.size() === 1) {
                builder5.add(
                    Expressions.declare(
                        0, actualStart,
                        Expressions.condition(
                            needRecomputeWindow, startX,
                            Expressions.add(prevEnd, Expressions.constant(1))
                        )
                    )
                )
            } else {
                builder5.add(
                    Expressions.declare(0, actualStart, null)
                )
                builder5.add(
                    Expressions.ifThenElse(
                        needRecomputeWindow,
                        resetWindowState,
                        Expressions.statement(
                            Expressions.assign(
                                actualStart,
                                Expressions.add(prevEnd, Expressions.constant(1))
                            )
                        )
                    )
                )
            }
            if (lowerBoundCanChange is BinaryExpression) {
                builder5.add(
                    Expressions.statement(Expressions.assign(prevStart, startX))
                )
            }
            builder5.add(
                Expressions.statement(Expressions.assign(prevEnd, endX))
            )
            val builder7 = BlockBuilder(true, builder5)
            val jDecl: DeclarationStatement = Expressions.declare(0, "j", actualStart)
            val inputPhysTypeFinal: PhysType = inputPhysType
            val resultContextBuilder: Function<BlockBuilder, WinAggFrameResultContext> =
                getBlockBuilderWinAggFrameResultContextFunction(
                    typeFactory,
                    implementor.getConformance(), result, translatedConstants,
                    comparator_, rows_, i_, startX, endX, minX, maxX,
                    hasRows, frameRowCount, partitionRowCount,
                    jDecl, inputPhysTypeFinal
                )
            val rexArguments: Function<AggImpState, List<RexNode>> = Function<AggImpState, List<RexNode>> { agg ->
                val argList: List<Integer> = agg.call.getArgList()
                val inputTypes: List<RelDataType> = EnumUtils.fieldRowTypes(
                    result.physType.getRowType(),
                    constants,
                    argList
                )
                val args: List<RexNode> = ArrayList(inputTypes.size())
                for (i in 0 until argList.size()) {
                    val idx: Integer = argList[i]
                    args.add(RexInputRef(idx, inputTypes[i]))
                }
                args
            }
            implementAdd(aggs, builder7, resultContextBuilder, rexArguments, jDecl)
            val forBlock: BlockStatement = builder7.toBlock()
            if (!forBlock.statements.isEmpty()) {
                // For instance, row_number does not use for loop to compute the value
                var forAggLoop: Statement = Expressions.for_(
                    Arrays.asList(jDecl),
                    Expressions.lessThanOrEqual(jDecl.parameter, endX),
                    Expressions.preIncrementAssign(jDecl.parameter),
                    forBlock
                )
                if (!hasRows.equals(Expressions.constant(true))) {
                    forAggLoop = Expressions.ifThen(hasRows, forAggLoop)
                }
                builder5.add(forAggLoop)
            }
            if (implementResult(
                    aggs, builder5, resultContextBuilder, rexArguments,
                    true
                )
            ) {
                builder4.add(
                    Expressions.ifThen(
                        Expressions.orElse(
                            lowerBoundCanChange,
                            Expressions.notEqual(endX, prevEnd)
                        ),
                        builder5.toBlock()
                    )
                )
            }
            implementResult(
                aggs, builder4, resultContextBuilder, rexArguments,
                false
            )
            builder4.add(
                Expressions.statement(
                    Expressions.call(
                        list_,
                        BuiltInMethod.COLLECTION_ADD.method,
                        outputPhysType.record(outputRow)
                    )
                )
            )
            builder3.add(
                Expressions.for_(
                    Expressions.declare(0, i_, Expressions.constant(0)),
                    Expressions.lessThan(
                        i_,
                        Expressions.field(rows_, "length")
                    ),
                    Expressions.preIncrementAssign(i_),
                    builder4.toBlock()
                )
            )
            builder.add(
                Expressions.while_(
                    Expressions.call(
                        iterator_,
                        BuiltInMethod.ITERATOR_HAS_NEXT.method
                    ),
                    builder3.toBlock()
                )
            )
            builder.add(
                Expressions.statement(
                    Expressions.call(
                        collectionExpr,
                        BuiltInMethod.MAP_CLEAR.method
                    )
                )
            )

            // We're not assigning to "source". For each group, create a new
            // final variable called "source" or "sourceN".
            source_ = builder.append(
                "source",
                Expressions.call(
                    BuiltInMethod.AS_ENUMERABLE.method, list_
                )
            )
            inputPhysType = outputPhysType
        }

        //   return Linq4j.asEnumerable(list);
        builder.add(
            Expressions.return_(null, source_)
        )
        return implementor.result(inputPhysType, builder.toBlock())
    }

    private fun declareAndResetState(
        typeFactory: JavaTypeFactory,
        builder: BlockBuilder, result: Result, windowIdx: Int,
        aggs: List<AggImpState>, outputPhysType: PhysType,
        outputRow: List<Expression>
    ) {
        for (agg in aggs) {
            agg.context = object : WinAggContext() {
                @Override
                fun aggregation(): SqlAggFunction {
                    return agg.call.getAggregation()
                }

                @Override
                fun returnRelType(): RelDataType {
                    return agg.call.type
                }

                @Override
                fun returnType(): Type {
                    return EnumUtils.javaClass(typeFactory, returnRelType())
                }

                @Override
                fun parameterTypes(): List<Type?> {
                    return EnumUtils.fieldTypes(
                        typeFactory,
                        parameterRelTypes()
                    )
                }

                @Override
                fun parameterRelTypes(): List<RelDataType?> {
                    return EnumUtils.fieldRowTypes(
                        result.physType.getRowType(),
                        constants, agg.call.getArgList()
                    )
                }

                @Override
                fun groupSets(): List<ImmutableBitSet> {
                    throw UnsupportedOperationException()
                }

                @Override
                fun keyOrdinals(): List<Integer> {
                    throw UnsupportedOperationException()
                }

                @Override
                fun keyRelTypes(): List<RelDataType?> {
                    throw UnsupportedOperationException()
                }

                @Override
                fun keyTypes(): List<Type?> {
                    throw UnsupportedOperationException()
                }
            }
            var aggName = "a" + agg.aggIdx
            if (CalciteSystemProperty.DEBUG.value()) {
                aggName = Util.toJavaId(agg.call.getAggregation().getName(), 0)
                    .substring("ID$0$".length()) + aggName
            }
            val state: List<Type> = agg.implementor.getStateType(agg.context)
            val decls: List<Expression> = ArrayList(state.size())
            for (i in 0 until state.size()) {
                val type: Type = state[i]
                val pe: ParameterExpression = Expressions.parameter(
                    type,
                    builder.newName(aggName + "s" + i + "w" + windowIdx)
                )
                builder.add(Expressions.declare(0, pe, null))
                decls.add(pe)
            }
            agg.state = decls
            var aggHolderType: Type = agg.context.returnType()
            val aggStorageType: Type = outputPhysType.getJavaFieldType(outputRow.size())
            if (Primitive.`is`(aggHolderType) && !Primitive.`is`(aggStorageType)) {
                aggHolderType = Primitive.box(aggHolderType)
            }
            val aggRes: ParameterExpression = Expressions.parameter(
                0,
                aggHolderType,
                builder.newName(aggName + "w" + windowIdx)
            )
            builder.add(
                Expressions.declare(0, aggRes,
                    Expressions.constant(
                        Optional.ofNullable(Primitive.of(aggRes.getType()))
                            .map { x -> x.defaultValue }
                            .orElse(null),
                        aggRes.getType())))
            agg.result = aggRes
            outputRow.add(aggRes)
            agg.implementor.implementReset(
                agg.context,
                WinAggResetContextImpl(
                    builder, agg.state,
                    castNonNull(null), castNonNull(null), castNonNull(null), castNonNull(null),
                    castNonNull(null), castNonNull(null)
                )
            )
        }
    }

    companion object {
        @SuppressWarnings(["unused", "nullness"])
        private fun sampleOfTheGeneratedWindowedAggregate() {
            // Here's overview of the generated code
            // For each list of rows that have the same partitioning key, evaluate
            // all of the windowed aggregate functions.

            // builder
            val iterator: Iterator<Array<Integer>>? = null

            // builder3
            val rows: Array<Integer> = iterator!!.next()
            var prevStart = -1
            var prevEnd = -1
            for (i in rows.indices) {
                // builder4
                val row: Integer = rows[i]
                val start = 0
                val end = 100
                if (start != prevStart || end != prevEnd) {
                    // builder5
                    var actualStart = 0
                    actualStart = if (start != prevStart || end < prevEnd) {
                        // builder6
                        // recompute
                        start
                        // implementReset
                    } else { // must be start == prevStart && end > prevEnd
                        prevEnd + 1
                    }
                    prevStart = start
                    prevEnd = end
                    if (start != -1) {
                        for (j in actualStart..end) {
                            // builder7
                            // implementAdd
                        }
                    }
                    // implementResult
                    // list.add(new Xxx(row.deptno, row.empid, sum, count));
                }
            }
            // multiMap.clear(); // allows gc
            // source = Linq4j.asEnumerable(list);
        }

        private fun getBlockBuilderWinAggFrameResultContextFunction(
            typeFactory: JavaTypeFactory, conformance: SqlConformance,
            result: Result, translatedConstants: List<Expression>,
            comparator_: Expression,
            rows_: Expression, i_: ParameterExpression,
            startX: Expression, endX: Expression,
            minX: Expression, maxX: Expression,
            hasRows: Expression, frameRowCount: Expression,
            partitionRowCount: Expression,
            jDecl: DeclarationStatement,
            inputPhysType: PhysType
        ): Function<BlockBuilder, WinAggFrameResultContext> {
            return label@ Function<BlockBuilder, WinAggFrameResultContext> { block ->
                object : WinAggFrameResultContext() {
                    @Override
                    override fun rowTranslator(rowIndex: Expression?): RexToLixTranslator {
                        val row: Expression = getRow(rowIndex)
                        val inputGetter: RexToLixTranslator.InputGetter = WindowRelInputGetter(
                            row, inputPhysType,
                            result.physType.getRowType().getFieldCount(),
                            translatedConstants
                        )
                        return@label RexToLixTranslator.forAggregation(
                            typeFactory,
                            block, inputGetter, conformance
                        )
                    }

                    @Override
                    fun computeIndex(
                        offset: Expression?,
                        seekType: WinAggImplementor.SeekType
                    ): Expression {
                        var index: Expression
                        index = if (seekType === WinAggImplementor.SeekType.AGG_INDEX) {
                            jDecl.parameter
                        } else if (seekType === WinAggImplementor.SeekType.SET) {
                            i_
                        } else if (seekType === WinAggImplementor.SeekType.START) {
                            startX
                        } else if (seekType === WinAggImplementor.SeekType.END) {
                            endX
                        } else {
                            throw IllegalArgumentException(
                                "SeekSet " + seekType
                                        + " is not supported"
                            )
                        }
                        if (!Expressions.constant(0).equals(offset)) {
                            index = block.append("idx", Expressions.add(index, offset))
                        }
                        return@label index
                    }

                    private fun checkBounds(
                        rowIndex: Expression,
                        minIndex: Expression, maxIndex: Expression
                    ): Expression {
                        if (rowIndex === i_ || rowIndex === startX || rowIndex === endX) {
                            // No additional bounds check required
                            return@label hasRows
                        }
                        return@label block.append(
                            "rowInFrame",
                            Expressions.foldAnd(
                                ImmutableList.of(
                                    hasRows,
                                    Expressions.greaterThanOrEqual(rowIndex, minIndex),
                                    Expressions.lessThanOrEqual(rowIndex, maxIndex)
                                )
                            )
                        )
                    }

                    @Override
                    override fun rowInFrame(rowIndex: Expression): Expression {
                        return@label checkBounds(rowIndex, startX, endX)
                    }

                    @Override
                    override fun rowInPartition(rowIndex: Expression): Expression {
                        return@label checkBounds(rowIndex, minX, maxX)
                    }

                    @Override
                    override fun compareRows(a: Expression?, b: Expression?): Expression {
                        return@label Expressions.call(
                            comparator_,
                            BuiltInMethod.COMPARATOR_COMPARE.method,
                            getRow(a), getRow(b)
                        )
                    }

                    fun getRow(rowIndex: Expression?): Expression {
                        return@label block.append(
                            "jRow",
                            EnumUtils.convert(
                                Expressions.arrayIndex(rows_, rowIndex),
                                inputPhysType.getJavaRowType()
                            )
                        )
                    }

                    @Override
                    override fun index(): Expression {
                        return@label i_
                    }

                    @Override
                    override fun startIndex(): Expression {
                        return@label startX
                    }

                    @Override
                    override fun endIndex(): Expression {
                        return@label endX
                    }

                    @Override
                    override fun hasRows(): Expression {
                        return@label hasRows
                    }

                    @get:Override
                    override val frameRowCount: Expression
                        get() {
                            return@label frameRowCount
                        }

                    @get:Override
                    override val partitionRowCount: Expression
                        get() {
                            return@label partitionRowCount
                        }
                }
            }
        }

        private fun getPartitionIterator(
            builder: BlockBuilder,
            source_: Expression,
            inputPhysType: PhysType,
            group: Group,
            comparator_: Expression
        ): Pair<Expression, Expression> {
            // Populate map of lists, one per partition
            //   final Map<Integer, List<Employee>> multiMap =
            //     new SortedMultiMap<Integer, List<Employee>>();
            //    source.foreach(
            //      new Function1<Employee, Void>() {
            //        public Void apply(Employee v) {
            //          final Integer k = v.deptno;
            //          multiMap.putMulti(k, v);
            //          return null;
            //        }
            //      });
            //   final List<Xxx> list = new ArrayList<Xxx>(multiMap.size());
            //   Iterator<Employee[]> iterator = multiMap.arrays(comparator);
            //
            if (group.keys.isEmpty()) {
                // If partition key is empty, no need to partition.
                //
                //   final List<Employee> tempList =
                //       source.into(new ArrayList<Employee>());
                //   Iterator<Employee[]> iterator =
                //       SortedMultiMap.singletonArrayIterator(comparator, tempList);
                //   final List<Xxx> list = new ArrayList<Xxx>(tempList.size());
                val tempList_: Expression = builder.append(
                    "tempList",
                    Expressions.convert_(
                        Expressions.call(
                            source_,
                            BuiltInMethod.INTO.method,
                            Expressions.new_(ArrayList::class.java)
                        ),
                        List::class.java
                    )
                )
                return Pair.of(
                    tempList_,
                    builder.append(
                        "iterator",
                        Expressions.call(
                            null,
                            BuiltInMethod.SORTED_MULTI_MAP_SINGLETON.method,
                            comparator_,
                            tempList_
                        )
                    )
                )
            }
            val multiMap_: Expression = builder.append(
                "multiMap", Expressions.new_(SortedMultiMap::class.java)
            )
            val builder2 = BlockBuilder()
            val v_: ParameterExpression = Expressions.parameter(
                inputPhysType.getJavaRowType(),
                builder2.newName("v")
            )
            val selector: Pair<Type, List<Expression>> =
                inputPhysType.selector(v_, group.keys.asList(), JavaRowFormat.CUSTOM)
            val key_: ParameterExpression
            if (selector.left is Types.RecordType) {
                val keyJavaType: Types.RecordType = selector.left as Types.RecordType
                val initExpressions: List<Expression> = selector.right
                key_ = Expressions.parameter(keyJavaType, "key")
                builder2.add(Expressions.declare(0, key_, null))
                builder2.add(
                    Expressions.statement(
                        Expressions.assign(key_, Expressions.new_(keyJavaType))
                    )
                )
                val fieldList: List<Types.RecordField> = keyJavaType.getRecordFields()
                for (i in 0 until initExpressions.size()) {
                    val right: Expression = initExpressions[i]
                    builder2.add(
                        Expressions.statement(
                            Expressions.assign(
                                Expressions.field(key_, fieldList[i]), right
                            )
                        )
                    )
                }
            } else {
                val declare: DeclarationStatement = Expressions.declare(0, "key", selector.right.get(0))
                builder2.add(declare)
                key_ = declare.parameter
            }
            builder2.add(
                Expressions.statement(
                    Expressions.call(
                        multiMap_,
                        BuiltInMethod.SORTED_MULTI_MAP_PUT_MULTI.method,
                        key_,
                        v_
                    )
                )
            )
            builder2.add(
                Expressions.return_(
                    null, Expressions.constant(null)
                )
            )
            builder.add(
                Expressions.statement(
                    Expressions.call(
                        source_,
                        BuiltInMethod.ENUMERABLE_FOREACH.method,
                        Expressions.lambda(
                            builder2.toBlock(), v_
                        )
                    )
                )
            )
            return Pair.of(
                multiMap_,
                builder.append(
                    "iterator",
                    Expressions.call(
                        multiMap_,
                        BuiltInMethod.SORTED_MULTI_MAP_ARRAYS.method,
                        comparator_
                    )
                )
            )
        }

        private fun getRowCollationKey(
            builder: BlockBuilder, inputPhysType: PhysType,
            group: Group, windowIdx: Int
        ): Pair<Expression, Expression> {
            return if (!(group.isRows
                        || group.upperBound.isUnbounded() && group.lowerBound.isUnbounded())
            ) {
                val pair: Pair<Expression, Expression> = inputPhysType.generateCollationKey(
                    group.collation().getFieldCollations()
                )
                // optimize=false to prevent inlining of object create into for-loops
                Pair.of(
                    builder.append("keySelector$windowIdx", pair.left, false),
                    builder.append("keyComparator$windowIdx", pair.right, false)
                )
            } else {
                Pair.of(null, null)
            }
        }

        private fun implementAdd(
            aggs: List<AggImpState>,
            builder7: BlockBuilder,
            frame: Function<BlockBuilder, WinAggFrameResultContext>,
            rexArguments: Function<AggImpState, List<RexNode>>,
            jDecl: DeclarationStatement
        ) {
            for (agg in aggs) {
                val addContext: WinAggAddContext =
                    object : WinAggAddContextImpl(builder7, requireNonNull(agg.state, "agg.state"), frame) {
                        @Override
                        fun currentPosition(): Expression {
                            return jDecl.parameter
                        }

                        @Override
                        fun rexArguments(): List<RexNode> {
                            return rexArguments.apply(agg)
                        }

                        @Override
                        @Nullable
                        fun rexFilterArgument(): RexNode? {
                            return null // REVIEW
                        }
                    }
                agg.implementor.implementAdd(requireNonNull(agg.context, "agg.context"), addContext)
            }
        }

        private fun implementResult(
            aggs: List<AggImpState>,
            builder: BlockBuilder,
            frame: Function<BlockBuilder, WinAggFrameResultContext>,
            rexArguments: Function<AggImpState, List<RexNode>>,
            cachedBlock: Boolean
        ): Boolean {
            var nonEmpty = false
            for (agg in aggs) {
                var needCache = true
                if (agg.implementor is WinAggImplementor) {
                    needCache = agg.implementor.needCacheWhenFrameIntact()
                }
                if (needCache xor cachedBlock) {
                    // Regular aggregates do not change when the windowing frame keeps
                    // the same. Ths
                    continue
                }
                nonEmpty = true
                val res: Expression = agg.implementor.implementResult(requireNonNull(agg.context, "agg.context"),
                    object : WinAggResultContextImpl(builder, requireNonNull(agg.state, "agg.state"), frame) {
                        @Override
                        fun rexArguments(): List<RexNode> {
                            return rexArguments.apply(agg)
                        }
                    })
                // Several count(a) and count(b) might share the result
                val result: Expression = requireNonNull(
                    agg.result
                ) { "agg.result for " + agg.call }
                val aggRes: Expression = builder.append(
                    "a" + agg.aggIdx.toString() + "res",
                    EnumUtils.convert(res, result.getType())
                )
                builder.add(
                    Expressions.statement(Expressions.assign(result, aggRes))
                )
            }
            return nonEmpty
        }

        private fun translateBound(
            translator: RexToLixTranslator,
            i_: ParameterExpression, row_: Expression, min_: Expression, max_: Expression,
            rows_: Expression, group: Group, lower: Boolean, physType: PhysType,
            @Nullable keySelector: Expression, @Nullable keyComparator: Expression
        ): Expression {
            val bound: RexWindowBound = if (lower) group.lowerBound else group.upperBound
            if (bound.isUnbounded()) {
                return if (bound.isPreceding()) min_ else max_
            }
            if (group.isRows) {
                if (bound.isCurrentRow()) {
                    return i_
                }
                val node: RexNode = bound.getOffset()
                var offs: Expression = translator.translate(node)
                // Floating offset does not make sense since we refer to array index.
                // Nulls do not make sense as well.
                offs = EnumUtils.convert(offs, Int::class.javaPrimitiveType)
                var b: Expression = i_
                b = if (bound.isFollowing()) {
                    Expressions.add(b, offs)
                } else {
                    Expressions.subtract(b, offs)
                }
                return b
            }
            var searchLower: Expression = min_
            var searchUpper: Expression = max_
            if (bound.isCurrentRow()) {
                if (lower) {
                    searchUpper = i_
                } else {
                    searchLower = i_
                }
            }
            val fieldCollations: List<RelFieldCollation> = group.collation().getFieldCollations()
            if (bound.isCurrentRow() && fieldCollations.size() !== 1) {
                return Expressions.call(
                    (if (lower) BuiltInMethod.BINARY_SEARCH5_LOWER else BuiltInMethod.BINARY_SEARCH5_UPPER).method,
                    rows_, row_, searchLower, searchUpper,
                    requireNonNull(keySelector, "keySelector"),
                    requireNonNull(keyComparator, "keyComparator")
                )
            }
            assert(fieldCollations.size() === 1) {
                ("When using range window specification, ORDER BY should have"
                        + " exactly one expression."
                        + " Actual collation is " + group.collation())
            }
            // isRange
            val orderKey: Int = fieldCollations[0].getFieldIndex()
            val keyType: RelDataType = physType.getRowType().getFieldList().get(orderKey).getType()
            var desiredKeyType: Type = translator.typeFactory.getJavaClass(keyType)
            if (bound.getOffset() == null) {
                desiredKeyType = Primitive.box(desiredKeyType)
            }
            var `val`: Expression = translator.translate(
                RexInputRef(orderKey, keyType), desiredKeyType
            )
            if (!bound.isCurrentRow()) {
                val node: RexNode = bound.getOffset()
                val offs: Expression = translator.translate(node)
                // TODO: support date + interval somehow
                `val` = if (bound.isFollowing()) {
                    Expressions.add(`val`, offs)
                } else {
                    Expressions.subtract(`val`, offs)
                }
            }
            return Expressions.call(
                (if (lower) BuiltInMethod.BINARY_SEARCH6_LOWER else BuiltInMethod.BINARY_SEARCH6_UPPER).method,
                rows_, `val`, searchLower, searchUpper,
                requireNonNull(keySelector, "keySelector"),
                requireNonNull(keyComparator, "keyComparator")
            )
        }
    }
}
