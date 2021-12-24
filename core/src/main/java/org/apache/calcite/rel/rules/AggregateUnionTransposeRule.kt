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

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.core.Union
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.rel.metadata.RelMdUtil
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.`fun`.SqlAnyValueAggFunction
import org.apache.calcite.sql.`fun`.SqlBitOpAggFunction
import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlMinMaxAggFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.`fun`.SqlSumAggFunction
import org.apache.calcite.sql.`fun`.SqlSumEmptyIsZeroAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.Mapping
import org.apache.calcite.util.mapping.MappingType
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.util.ArrayList
import java.util.IdentityHashMap
import java.util.List

/**
 * Planner rule that pushes an
 * [org.apache.calcite.rel.core.Aggregate]
 * past a non-distinct [org.apache.calcite.rel.core.Union].
 *
 * @see CoreRules.AGGREGATE_UNION_TRANSPOSE
 */
@Value.Enclosing
class AggregateUnionTransposeRule
/** Creates an AggregateUnionTransposeRule.  */
protected constructor(config: Config?) : RelRule<AggregateUnionTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        unionClass: Class<out Union?>?, relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(aggregateClass, unionClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        aggregateFactory: RelFactories.AggregateFactory?,
        unionClass: Class<out Union?>?,
        setOpFactory: RelFactories.SetOpFactory?
    ) : this(
        aggregateClass, unionClass,
        RelBuilder.proto(aggregateFactory, setOpFactory)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggRel: Aggregate = call.rel(0)
        val union: Union = call.rel(1)
        if (!union.all) {
            // This transformation is only valid for UNION ALL.
            // Consider t1(i) with rows (5), (5) and t2(i) with
            // rows (5), (10), and the query
            // select sum(i) from (select i from t1) union (select i from t2).
            // The correct answer is 15.  If we apply the transformation,
            // we get
            // select sum(i) from
            // (select sum(i) as i from t1) union (select sum(i) as i from t2)
            // which yields 25 (incorrect).
            return
        }
        val groupCount: Int = aggRel.getGroupSet().cardinality()
        val transformedAggCalls: List<Any> = transformAggCalls(
            aggRel.copy(
                aggRel.getTraitSet(), aggRel.getInput(),
                aggRel.getGroupSet(), null, aggRel.getAggCallList()
            ),
            groupCount, aggRel.getAggCallList()
        )
            ?: // we've detected the presence of something like AVG,
            // which we can't handle
            return
        var hasUniqueKeyInAllInputs = true
        val mq: RelMetadataQuery = call.getMetadataQuery()
        for (input in union.getInputs()) {
            val alreadyUnique: Boolean = RelMdUtil.areColumnsDefinitelyUnique(
                mq, input,
                aggRel.getGroupSet()
            )
            if (!alreadyUnique) {
                hasUniqueKeyInAllInputs = false
                break
            }
        }
        if (hasUniqueKeyInAllInputs) {
            // none of the children could benefit from the push-down,
            // so bail out (preventing the infinite loop to which most
            // planners would succumb)
            return
        }

        // create corresponding aggregates on top of each union child
        val relBuilder: RelBuilder = call.builder()
        for (input in union.getInputs()) {
            relBuilder.push(input)
            relBuilder.aggregate(
                relBuilder.groupKey(aggRel.getGroupSet()),
                aggRel.getAggCallList()
            )
        }

        // create a new union whose children are the aggregates created above
        relBuilder.union(true, union.getInputs().size())

        // Create the top aggregate. We must adjust group key indexes of the
        // original aggregate. E.g., if the original tree was:
        //
        // Aggregate[groupSet=$1, ...]
        //   Union[...]
        //
        // Then the new tree should be:
        // Aggregate[groupSet=$0, ...]
        //   Union[...]
        //     Aggregate[groupSet=$1, ...]
        val groupSet: ImmutableBitSet = aggRel.getGroupSet()
        val topGroupMapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            union.getRowType().getFieldCount(),
            aggRel.getGroupCount()
        )
        for (i in 0 until groupSet.cardinality()) {
            topGroupMapping.set(groupSet.nth(i), i)
        }
        val topGroupSet: ImmutableBitSet = Mappings.apply(topGroupMapping, groupSet)
        val topGroupSets: ImmutableList<ImmutableBitSet> = Mappings.apply2(topGroupMapping, aggRel.getGroupSets())
        relBuilder.aggregate(
            relBuilder.groupKey(topGroupSet, topGroupSets),
            transformedAggCalls
        )
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateUnionTransposeRule? {
            return AggregateUnionTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            unionClass: Class<out Union?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass).oneInput { b1 -> b1.operand(unionClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateUnionTransposeRule.Config.of()
                .withOperandFor(LogicalAggregate::class.java, LogicalUnion::class.java)
        }
    }

    companion object {
        private val SUPPORTED_AGGREGATES: IdentityHashMap<Class<out SqlAggFunction?>, Boolean> = IdentityHashMap()

        init {
            SUPPORTED_AGGREGATES.put(
                SqlMinMaxAggFunction::class.java, true
            )
            SUPPORTED_AGGREGATES.put(
                SqlCountAggFunction::class.java, true
            )
            SUPPORTED_AGGREGATES.put(
                SqlSumAggFunction::class.java, true
            )
            SUPPORTED_AGGREGATES.put(
                SqlSumEmptyIsZeroAggFunction::class.java, true
            )
            SUPPORTED_AGGREGATES.put(
                SqlAnyValueAggFunction::class.java, true
            )
            SUPPORTED_AGGREGATES.put(
                SqlBitOpAggFunction::class.java, true
            )
        }

        @Nullable
        private fun transformAggCalls(
            input: RelNode, groupCount: Int,
            origCalls: List<AggregateCall>
        ): List<AggregateCall>? {
            val newCalls: List<AggregateCall> = ArrayList()
            for (ord in Ord.zip(origCalls)) {
                val origCall: AggregateCall = ord.e
                if (origCall.isDistinct()
                    || !SUPPORTED_AGGREGATES.containsKey(
                        origCall.getAggregation()
                            .getClass()
                    )
                ) {
                    return null
                }
                val aggFun: SqlAggFunction
                val aggType: RelDataType?
                if (origCall.getAggregation() === SqlStdOperatorTable.COUNT) {
                    aggFun = SqlStdOperatorTable.SUM0
                    // count(any) is always not null, however nullability of sum might
                    // depend on the number of columns in GROUP BY.
                    // Here we use SUM0 since we are sure we will not face nullable
                    // inputs nor we'll face empty set.
                    aggType = null
                } else {
                    aggFun = origCall.getAggregation()
                    aggType = origCall.getType()
                }
                val newCall: AggregateCall = AggregateCall.create(
                    aggFun, origCall.isDistinct(),
                    origCall.isApproximate(), origCall.ignoreNulls(),
                    ImmutableList.of(groupCount + ord.i), -1,
                    origCall.distinctKeys, origCall.collation,
                    groupCount, input, aggType, origCall.getName()
                )
                newCalls.add(newCall)
            }
            return newCalls
        }
    }
}
