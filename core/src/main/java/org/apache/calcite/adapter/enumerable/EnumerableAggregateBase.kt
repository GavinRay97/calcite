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

import org.apache.calcite.adapter.enumerable.impl.AggAddContextImpl

/** Base class for EnumerableAggregate and EnumerableSortedAggregate.  */
abstract class EnumerableAggregateBase protected constructor(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    hints: List<RelHint?>?,
    input: RelNode?,
    groupSet: ImmutableBitSet?,
    @Nullable groupSets: List<ImmutableBitSet?>?,
    aggCalls: List<AggregateCall?>?
) : Aggregate(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls) {
    protected fun declareParentAccumulator(
        initExpressions: List<Expression?>,
        initBlock: BlockBuilder, accPhysType: PhysType
    ) {
        if (accPhysType.getJavaRowType() is JavaTypeFactoryImpl.SyntheticRecordType) {
            // We have to initialize the SyntheticRecordType instance this way, to
            // avoid using a class constructor with too many parameters.
            val synType: JavaTypeFactoryImpl.SyntheticRecordType =
                accPhysType.getJavaRowType() as JavaTypeFactoryImpl.SyntheticRecordType
            val record0_: ParameterExpression = Expressions.parameter(accPhysType.getJavaRowType(), "record0")
            initBlock.add(Expressions.declare(0, record0_, null))
            initBlock.add(
                Expressions.statement(
                    Expressions.assign(
                        record0_,
                        Expressions.new_(accPhysType.getJavaRowType())
                    )
                )
            )
            val fieldList: List<Types.RecordField> = synType.getRecordFields()
            for (i in 0 until initExpressions.size()) {
                val right: Expression? = initExpressions[i]
                initBlock.add(
                    Expressions.statement(
                        Expressions.assign(
                            Expressions.field(record0_, fieldList[i]), right
                        )
                    )
                )
            }
            initBlock.add(record0_)
        } else {
            initBlock.add(accPhysType.record(initExpressions))
        }
    }

    /**
     * Implements the [AggregateLambdaFactory].
     *
     *
     * Behavior depends upon ordering:
     *
     *
     *  * `hasOrderedCall == true` means there is at least one aggregate
     * call including sort spec. We use [LazyAggregateLambdaFactory]
     * implementation to implement sorted aggregates for that.
     *
     *  * `hasOrderedCall == false` indicates to use
     * [BasicAggregateLambdaFactory] to implement a non-sort
     * aggregate.
     *
     *
     */
    protected fun implementLambdaFactory(
        builder: BlockBuilder,
        inputPhysType: PhysType, aggs: List<AggImpState?>,
        accumulatorInitializer: Expression?, hasOrderedCall: Boolean,
        lambdaFactory: ParameterExpression?
    ) {
        if (hasOrderedCall) {
            val pe: ParameterExpression = Expressions.parameter(
                List::class.java,
                builder.newName("lazyAccumulators")
            )
            builder.add(
                Expressions.declare(0, pe, Expressions.new_(LinkedList::class.java))
            )
            for (agg in aggs) {
                if (agg.call.collation.equals(RelCollations.EMPTY)) {
                    // if the call does not require ordering, fallback to
                    // use a non-sorted lazy accumulator.
                    builder.add(
                        Expressions.statement(
                            Expressions.call(
                                pe,
                                BuiltInMethod.COLLECTION_ADD.method,
                                Expressions.new_(
                                    BuiltInMethod.BASIC_LAZY_ACCUMULATOR.constructor,
                                    requireNonNull(agg.accumulatorAdder, "agg.accumulatorAdder")
                                )
                            )
                        )
                    )
                    continue
                }
                val pair: Pair<Expression, Expression> = inputPhysType.generateCollationKey(
                    agg.call.collation.getFieldCollations()
                )
                builder.add(
                    Expressions.statement(
                        Expressions.call(
                            pe,
                            BuiltInMethod.COLLECTION_ADD.method,
                            Expressions.new_(
                                BuiltInMethod.SOURCE_SORTER.constructor,
                                requireNonNull(agg.accumulatorAdder, "agg.accumulatorAdder"),
                                pair.left, pair.right
                            )
                        )
                    )
                )
            }
            builder.add(
                Expressions.declare(
                    0, lambdaFactory,
                    Expressions.new_(
                        BuiltInMethod.LAZY_AGGREGATE_LAMBDA_FACTORY.constructor,
                        accumulatorInitializer, pe
                    )
                )
            )
        } else {
            // when hasOrderedCall == false
            val pe: ParameterExpression = Expressions.parameter(
                List::class.java,
                builder.newName("accumulatorAdders")
            )
            builder.add(
                Expressions.declare(0, pe, Expressions.new_(LinkedList::class.java))
            )
            for (agg in aggs) {
                builder.add(
                    Expressions.statement(
                        Expressions.call(
                            pe, BuiltInMethod.COLLECTION_ADD.method,
                            requireNonNull(agg.accumulatorAdder, "agg.accumulatorAdder")
                        )
                    )
                )
            }
            builder.add(
                Expressions.declare(
                    0, lambdaFactory,
                    Expressions.new_(
                        BuiltInMethod.BASIC_AGGREGATE_LAMBDA_FACTORY.constructor,
                        accumulatorInitializer, pe
                    )
                )
            )
        }
    }

    /** An implementation of [AggContext].  */
    protected inner class AggContextImpl internal constructor(agg: AggImpState, typeFactory: JavaTypeFactory) :
        AggContext {
        private val agg: AggImpState
        private val typeFactory: JavaTypeFactory

        init {
            this.agg = agg
            this.typeFactory = typeFactory
        }

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
        fun parameterRelTypes(): List<RelDataType?> {
            return EnumUtils.fieldRowTypes(
                getInput().getRowType(), null,
                agg.call.getArgList()
            )
        }

        @Override
        fun parameterTypes(): List<Type?> {
            return EnumUtils.fieldTypes(
                typeFactory,
                parameterRelTypes()
            )
        }

        @Override
        fun groupSets(): List<ImmutableBitSet> {
            return groupSets
        }

        @Override
        fun keyOrdinals(): List<Integer> {
            return groupSet.asList()
        }

        @Override
        fun keyRelTypes(): List<RelDataType?> {
            return EnumUtils.fieldRowTypes(
                getInput().getRowType(), null,
                groupSet.asList()
            )
        }

        @Override
        fun keyTypes(): List<Type?> {
            return EnumUtils.fieldTypes(typeFactory, keyRelTypes())
        }
    }

    protected fun createAccumulatorAdders(
        inParameter: ParameterExpression?,
        aggs: List<AggImpState>,
        accPhysType: PhysType,
        accExpr: ParameterExpression?,
        inputPhysType: PhysType,
        builder: BlockBuilder,
        implementor: EnumerableRelImplementor,
        typeFactory: JavaTypeFactory?
    ) {
        var i = 0
        var stateOffset = 0
        while (i < aggs.size()) {
            val builder2 = BlockBuilder()
            val agg: AggImpState = aggs[i]
            val stateSize: Int = requireNonNull(agg.state, "agg.state").size()
            val accumulator: List<Expression> = ArrayList(stateSize)
            for (j in 0 until stateSize) {
                accumulator.add(accPhysType.fieldReference(accExpr, j + stateOffset))
            }
            agg.state = accumulator
            stateOffset += stateSize
            val addContext: AggAddContext = object : AggAddContextImpl(builder2, accumulator) {
                @Override
                fun rexArguments(): List<RexNode> {
                    val inputTypes: List<RelDataTypeField> = inputPhysType.getRowType().getFieldList()
                    val args: List<RexNode> = ArrayList()
                    for (index in agg.call.getArgList()) {
                        args.add(RexInputRef.of(index, inputTypes))
                    }
                    return args
                }

                @Override
                @Nullable
                fun rexFilterArgument(): RexNode? {
                    return if (agg.call.filterArg < 0) null else RexInputRef.of(
                        agg.call.filterArg,
                        inputPhysType.getRowType()
                    )
                }

                @Override
                fun rowTranslator(): RexToLixTranslator {
                    return RexToLixTranslator.forAggregation(
                        typeFactory,
                        currentBlock(),
                        InputGetterImpl(
                            inParameter,
                            inputPhysType
                        ),
                        implementor.getConformance()
                    )
                }
            }
            agg.implementor.implementAdd(requireNonNull(agg.context, "agg.context"), addContext)
            builder2.add(accExpr)
            agg.accumulatorAdder = builder.append(
                "accumulatorAdder",
                Expressions.lambda(
                    Function2::class.java, builder2.toBlock(), accExpr,
                    inParameter
                )
            )
            i++
        }
    }

    protected fun createAggStateTypes(
        initExpressions: List<Expression?>,
        initBlock: BlockBuilder,
        aggs: List<AggImpState?>,
        typeFactory: JavaTypeFactory?
    ): List<Type> {
        val aggStateTypes: List<Type> = ArrayList()
        for (agg in aggs) {
            agg.context = AggContextImpl(agg, typeFactory)
            val state: List<Type> = agg.implementor.getStateType(agg.context)
            if (state.isEmpty()) {
                agg.state = ImmutableList.of()
                continue
            }
            aggStateTypes.addAll(state)
            val decls: List<Expression> = ArrayList(state.size())
            for (i in 0 until state.size()) {
                var aggName = "a" + agg.aggIdx
                if (CalciteSystemProperty.DEBUG.value()) {
                    aggName = Util.toJavaId(agg.call.getAggregation().getName(), 0)
                        .substring("ID$0$".length()) + aggName
                }
                val type: Type = state[i]
                val pe: ParameterExpression = Expressions.parameter(
                    type,
                    initBlock.newName(aggName + "s" + i)
                )
                initBlock.add(Expressions.declare(0, pe, null))
                decls.add(pe)
            }
            agg.state = decls
            initExpressions.addAll(decls)
            agg.implementor.implementReset(
                agg.context,
                AggResultContextImpl(initBlock, agg.call, decls, null, null)
            )
        }
        return aggStateTypes
    }

    companion object {
        protected fun hasOrderedCall(aggs: List<AggImpState?>): Boolean {
            for (agg in aggs) {
                if (!agg.call.collation.equals(RelCollations.EMPTY)) {
                    return true
                }
            }
            return false
        }
    }
}
