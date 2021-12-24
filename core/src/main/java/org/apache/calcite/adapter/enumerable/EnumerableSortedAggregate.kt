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

/** Sort based physical implementation of [Aggregate] in
 * [enumerable calling convention][EnumerableConvention].  */
class EnumerableSortedAggregate(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    input: RelNode?,
    groupSet: ImmutableBitSet?,
    @Nullable groupSets: List<ImmutableBitSet?>?,
    aggCalls: List<AggregateCall?>?
) : EnumerableAggregateBase(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls),
    EnumerableRel {
    init {
        assert(getConvention() is EnumerableConvention)
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        groupSet: ImmutableBitSet?,
        @Nullable groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>?
    ): EnumerableSortedAggregate {
        return EnumerableSortedAggregate(
            getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls
        )
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        if (!isSimple(this)) {
            return null
        }
        val inputTraits: RelTraitSet = getInput().getTraitSet()
        val collation: RelCollation = requireNonNull(
            required.getCollation()
        ) { "collation trait is null, required traits are $required" }
        val requiredKeys: ImmutableBitSet = ImmutableBitSet.of(RelCollations.ordinals(collation))
        val groupKeys: ImmutableBitSet = ImmutableBitSet.range(groupSet.cardinality())
        val mapping: Mappings.TargetMapping = Mappings.source(
            groupSet.toList(),
            input.getRowType().getFieldCount()
        )
        if (requiredKeys.equals(groupKeys)) {
            val inputCollation: RelCollation = RexUtil.apply(mapping, collation)
            return Pair.of(required, ImmutableList.of(inputTraits.replace(inputCollation)))
        } else if (groupKeys.contains(requiredKeys)) {
            // group by a,b,c order by c,b
            val list: List<RelFieldCollation> = ArrayList(collation.getFieldCollations())
            groupKeys.except(requiredKeys).forEach { k -> list.add(RelFieldCollation(k)) }
            val aggCollation: RelCollation = RelCollations.of(list)
            val inputCollation: RelCollation = RexUtil.apply(mapping, aggCollation)
            return Pair.of(
                traitSet.replace(aggCollation),
                ImmutableList.of(inputTraits.replace(inputCollation))
            )
        }

        // Group keys doesn't contain all the required keys, e.g.
        // group by a,b order by a,b,c
        // nothing we can do to propagate traits to child nodes.
        return null
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        if (!Aggregate.isSimple(this)) {
            throw Util.needToImplement("EnumerableSortedAggregate")
        }
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
            false, lambdaFactory
        )
        val resultBlock = BlockBuilder()
        val results: List<Expression> = Expressions.list()
        val key_: ParameterExpression
        val keyType: Type = keyPhysType.getJavaRowType()
        key_ = Expressions.parameter(keyType, "key")
        for (j in 0 until groupCount) {
            val ref: Expression = keyPhysType.fieldReference(key_, j)
            results.add(ref)
        }
        for (agg in aggs) {
            results.add(
                agg.implementor.implementResult(
                    requireNonNull(agg.context) { "agg.context is null for $agg" },
                    AggResultContextImpl(
                        resultBlock, agg.call,
                        requireNonNull(agg.state) { "agg.state is null for $agg" },
                        key_,
                        keyPhysType
                    )
                )
            )
        }
        resultBlock.add(physType.record(results))
        val keySelector_: Expression = builder.append(
            "keySelector",
            inputPhysType.generateSelector(
                parameter,
                groupSet.asList(),
                keyPhysType.getFormat()
            )
        )
        // Generate the appropriate key Comparator. In the case of NULL values
        // in group keys, the comparator must be able to support NULL values by giving a
        // consistent sort ordering.
        val comparator: Expression = keyPhysType.generateComparator(
            requireNonNull(
                getTraitSet().getCollation()
            ) { "getTraitSet().getCollation() is null, current traits are " + getTraitSet() })
        val resultSelector_: Expression = builder.append(
            "resultSelector",
            Expressions.lambda(
                Function2::class.java,
                resultBlock.toBlock(),
                key_,
                acc_
            )
        )
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    childExp,
                    BuiltInMethod.SORTED_GROUP_BY.method,
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
                        ), comparator
                    )
                )
            )
        )
        return implementor.result(physType, builder.toBlock())
    }
}
