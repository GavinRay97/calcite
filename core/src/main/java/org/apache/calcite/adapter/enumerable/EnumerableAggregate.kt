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

import org.apache.calcite.adapter.enumerable.impl.AggResultContextImpl

/** Implementation of [org.apache.calcite.rel.core.Aggregate] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableAggregate(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    input: RelNode?,
    groupSet: ImmutableBitSet?,
    @Nullable groupSets: List<ImmutableBitSet?>?,
    aggCalls: List<AggregateCall?>
) : EnumerableAggregateBase(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls),
    EnumerableRel {
    init {
        assert(getConvention() is EnumerableConvention)
        for (aggCall in aggCalls) {
            if (aggCall.isDistinct()) {
                throw InvalidRelException(
                    "distinct aggregation not supported"
                )
            }
            if (aggCall.distinctKeys != null) {
                throw InvalidRelException(
                    "within-distinct aggregation not supported"
                )
            }
            val implementor2: AggImplementor = RexImpTable.INSTANCE.get(aggCall.getAggregation(), false)
                ?: throw InvalidRelException("aggregation " + aggCall.getAggregation().toString() + " not supported")
        }
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, indicator: Boolean, groupSet: ImmutableBitSet?,
        groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>
    ) : this(cluster, traitSet, input, groupSet, groupSets, aggCalls) {
        checkIndicator(indicator)
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        groupSet: ImmutableBitSet?,
        @Nullable groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>
    ): EnumerableAggregate {
        return try {
            EnumerableAggregate(
                getCluster(), traitSet, input,
                groupSet, groupSets, aggCalls
            )
        } catch (e: InvalidRelException) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw AssertionError(e)
        }
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val typeFactory: JavaTypeFactory = implementor.getTypeFactory()
        val builder = BlockBuilder()
        val child: EnumerableRel = getInput() as EnumerableRel
        val result: Result = implementor.visitChild(this, 0, child, pref)
        val childExp: Expression = builder.append(
            "child",
            result.block
        )
        val physType: PhysType = PhysTypeImpl.of(
            typeFactory, getRowType(), pref.preferCustom()
        )

        // final Enumerable<Employee> child = <<child adapter>>;
        // Function1<Employee, Integer> keySelector =
        //     new Function1<Employee, Integer>() {
        //         public Integer apply(Employee a0) {
        //             return a0.deptno;
        //         }
        //     };
        // Function1<Employee, Object[]> accumulatorInitializer =
        //     new Function1<Employee, Object[]>() {
        //         public Object[] apply(Employee a0) {
        //             return new Object[] {0, 0};
        //         }
        //     };
        // Function2<Object[], Employee, Object[]> accumulatorAdder =
        //     new Function2<Object[], Employee, Object[]>() {
        //         public Object[] apply(Object[] a1, Employee a0) {
        //              a1[0] = ((Integer) a1[0]) + 1;
        //              a1[1] = ((Integer) a1[1]) + a0.salary;
        //             return a1;
        //         }
        //     };
        // Function2<Integer, Object[], Object[]> resultSelector =
        //     new Function2<Integer, Object[], Object[]>() {
        //         public Object[] apply(Integer a0, Object[] a1) {
        //             return new Object[] { a0, a1[0], a1[1] };
        //         }
        //     };
        // return childEnumerable
        //     .groupBy(
        //        keySelector, accumulatorInitializer, accumulatorAdder,
        //        resultSelector);
        //
        // or, if key has 0 columns,
        //
        // return childEnumerable
        //     .aggregate(
        //       accumulatorInitializer.apply(),
        //       accumulatorAdder,
        //       resultSelector);
        //
        // with a slightly different resultSelector; or if there are no aggregate
        // functions
        //
        // final Enumerable<Employee> child = <<child adapter>>;
        // Function1<Employee, Integer> keySelector =
        //     new Function1<Employee, Integer>() {
        //         public Integer apply(Employee a0) {
        //             return a0.deptno;
        //         }
        //     };
        // EqualityComparer<Employee> equalityComparer =
        //     new EqualityComparer<Employee>() {
        //         boolean equal(Employee a0, Employee a1) {
        //             return a0.deptno;
        //         }
        //     };
        // return child
        //     .distinct(equalityComparer);
        val inputPhysType: PhysType = result.physType
        val parameter: ParameterExpression = Expressions.parameter(inputPhysType.getJavaRowType(), "a0")
        val keyPhysType: PhysType = inputPhysType.project(
            groupSet.asList(), getGroupType() !== Group.SIMPLE,
            JavaRowFormat.LIST
        )
        val groupCount: Int = getGroupCount()
        val aggs: List<AggImpState> = ArrayList(aggCalls.size())
        for (call in Ord.zip(aggCalls)) {
            aggs.add(AggImpState(call.i, call.e, false))
        }

        // Function0<Object[]> accumulatorInitializer =
        //     new Function0<Object[]>() {
        //         public Object[] apply() {
        //             return new Object[] {0, 0};
        //         }
        //     };
        val initExpressions: List<Expression> = ArrayList()
        val initBlock = BlockBuilder()
        val aggStateTypes: List<Type> = createAggStateTypes(
            initExpressions, initBlock, aggs, typeFactory
        )
        val accPhysType: PhysType = PhysTypeImpl.of(
            typeFactory,
            typeFactory.createSyntheticType(aggStateTypes)
        )
        declareParentAccumulator(initExpressions, initBlock, accPhysType)
        val accumulatorInitializer: Expression = builder.append(
            "accumulatorInitializer",
            Expressions.lambda(
                Function0::class.java,
                initBlock.toBlock()
            )
        )

        // Function2<Object[], Employee, Object[]> accumulatorAdder =
        //     new Function2<Object[], Employee, Object[]>() {
        //         public Object[] apply(Object[] acc, Employee in) {
        //              acc[0] = ((Integer) acc[0]) + 1;
        //              acc[1] = ((Integer) acc[1]) + in.salary;
        //             return acc;
        //         }
        //     };
        val inParameter: ParameterExpression = Expressions.parameter(inputPhysType.getJavaRowType(), "in")
        val acc_: ParameterExpression = Expressions.parameter(accPhysType.getJavaRowType(), "acc")
        createAccumulatorAdders(
            inParameter, aggs, accPhysType, acc_, inputPhysType, builder, implementor, typeFactory
        )
        val lambdaFactory: ParameterExpression = Expressions.parameter(
            AggregateLambdaFactory::class.java,
            builder.newName("lambdaFactory")
        )
        implementLambdaFactory(
            builder, inputPhysType, aggs, accumulatorInitializer,
            hasOrderedCall(aggs), lambdaFactory
        )
        // Function2<Integer, Object[], Object[]> resultSelector =
        //     new Function2<Integer, Object[], Object[]>() {
        //         public Object[] apply(Integer key, Object[] acc) {
        //             return new Object[] { key, acc[0], acc[1] };
        //         }
        //     };
        val resultBlock = BlockBuilder()
        val results: List<Expression> = Expressions.list()
        val key_: ParameterExpression?
        if (groupCount == 0) {
            key_ = null
        } else {
            val keyType: Type = keyPhysType.getJavaRowType()
            key_ = Expressions.parameter(keyType, "key")
            for (j in 0 until groupCount) {
                val ref: Expression = keyPhysType.fieldReference(key_, j)
                if (getGroupType() === Group.SIMPLE) {
                    results.add(ref)
                } else {
                    results.add(
                        Expressions.condition(
                            keyPhysType.fieldReference(key_, groupCount + j),
                            Expressions.constant(null),
                            Expressions.box(ref)
                        )
                    )
                }
            }
        }
        for (agg in aggs) {
            results.add(
                agg.implementor.implementResult(
                    requireNonNull(agg.context, "agg.context"),
                    AggResultContextImpl(
                        resultBlock, agg.call,
                        requireNonNull(agg.state, "agg.state"), key_,
                        keyPhysType
                    )
                )
            )
        }
        resultBlock.add(physType.record(results))
        if (getGroupType() !== Group.SIMPLE) {
            val list: List<Expression> = ArrayList()
            for (set in groupSets) {
                list.add(
                    inputPhysType.generateSelector(
                        parameter, groupSet.asList(),
                        set.asList(), keyPhysType.getFormat()
                    )
                )
            }
            val keySelectors_: Expression = builder.append(
                "keySelectors",
                Expressions.call(
                    BuiltInMethod.ARRAYS_AS_LIST.method,
                    list
                )
            )
            val resultSelector: Expression = builder.append(
                "resultSelector",
                Expressions.lambda(
                    Function2::class.java,
                    resultBlock.toBlock(),
                    requireNonNull(key_, "key_"),
                    acc_
                )
            )
            builder.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        BuiltInMethod.GROUP_BY_MULTIPLE.method,
                        Expressions.list(
                            childExp,
                            keySelectors_,
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method
                            ),
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method
                            ),
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method,
                                resultSelector
                            )
                        )
                            .appendIfNotNull(keyPhysType.comparer())
                    )
                )
            )
        } else if (groupCount == 0) {
            val resultSelector: Expression = builder.append(
                "resultSelector",
                Expressions.lambda(
                    Function1::class.java,
                    resultBlock.toBlock(),
                    acc_
                )
            )
            builder.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        BuiltInMethod.SINGLETON_ENUMERABLE.method,
                        Expressions.call(
                            childExp,
                            BuiltInMethod.AGGREGATE.method,
                            Expressions.call(
                                Expressions.call(
                                    lambdaFactory,
                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method
                                ),
                                BuiltInMethod.FUNCTION0_APPLY.method
                            ),
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method
                            ),
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_SINGLE_GROUP_RESULT_SELECTOR.method,
                                resultSelector
                            )
                        )
                    )
                )
            )
        } else if (aggCalls.isEmpty()
            && groupSet.equals(
                ImmutableBitSet.range(child.getRowType().getFieldCount())
            )
        ) {
            builder.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        inputPhysType.convertTo(childExp, physType.getFormat()),
                        BuiltInMethod.DISTINCT.method,
                        Expressions.< Expression > list < Expression ? > ()
                            .appendIfNotNull(physType.comparer())
                    )
                )
            )
        } else {
            val keySelector_: Expression = builder.append(
                "keySelector",
                inputPhysType.generateSelector(
                    parameter,
                    groupSet.asList(),
                    keyPhysType.getFormat()
                )
            )
            val resultSelector_: Expression = builder.append(
                "resultSelector",
                Expressions.lambda(
                    Function2::class.java,
                    resultBlock.toBlock(),
                    requireNonNull(key_, "key_"),
                    acc_
                )
            )
            builder.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        childExp,
                        BuiltInMethod.GROUP_BY2.method,
                        Expressions.list(
                            keySelector_,
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method
                            ),
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method
                            ),
                            Expressions.call(
                                lambdaFactory,
                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method,
                                resultSelector_
                            )
                        )
                            .appendIfNotNull(keyPhysType.comparer())
                    )
                )
            )
        }
        return implementor.result(physType, builder.toBlock())
    }
}
