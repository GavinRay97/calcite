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

import org.apache.calcite.plan.RelOptCluster

/** Rule to convert a [LogicalJoin] to an [EnumerableBatchNestedLoopJoin].
 * You may provide a custom config to convert other nodes that extend [Join].
 *
 * @see EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE
 */
@Value.Enclosing
class EnumerableBatchNestedLoopJoinRule
/** Creates an EnumerableBatchNestedLoopJoinRule.  */
protected constructor(config: Config?) : RelRule<EnumerableBatchNestedLoopJoinRule.Config?>(config) {
    @Deprecated // to be removed before 2.0
    protected constructor(
        clazz: Class<out Join?>?,
        relBuilderFactory: RelBuilderFactory?, batchSize: Int
    ) : this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.operand(clazz).anyInputs() }
        .`as`(Config::class.java)
        .withBatchSize(batchSize)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        relBuilderFactory: RelBuilderFactory?,
        batchSize: Int
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withBatchSize(batchSize)
    ) {
    }

    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val join: Join = call.rel(0)
        val joinType: JoinRelType = join.getJoinType()
        return joinType === JoinRelType.INNER || joinType === JoinRelType.LEFT || joinType === JoinRelType.ANTI || joinType === JoinRelType.SEMI
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)
        val leftFieldCount: Int = join.getLeft().getRowType().getFieldCount()
        val cluster: RelOptCluster = join.getCluster()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        val relBuilder: RelBuilder = call.builder()
        val correlationIds: Set<CorrelationId> = HashSet()
        val corrVarList: List<RexNode> = ArrayList()
        val batchSize: Int = config.batchSize()
        for (i in 0 until batchSize) {
            val correlationId: CorrelationId = cluster.createCorrel()
            correlationIds.add(correlationId)
            corrVarList.add(
                rexBuilder.makeCorrel(
                    join.getLeft().getRowType(),
                    correlationId
                )
            )
        }
        val corrVar0: RexNode = corrVarList[0]
        val requiredColumns: ImmutableBitSet.Builder = ImmutableBitSet.builder()

        // Generate first condition
        val condition: RexNode = join.getCondition().accept(object : RexShuttle() {
            @Override
            fun visitInputRef(input: RexInputRef): RexNode {
                val field: Int = input.getIndex()
                if (field >= leftFieldCount) {
                    return rexBuilder.makeInputRef(
                        input.getType(),
                        input.getIndex() - leftFieldCount
                    )
                }
                requiredColumns.set(field)
                return rexBuilder.makeFieldAccess(corrVar0, field)
            }
        })
        val conditionList: List<RexNode> = ArrayList()
        conditionList.add(condition)

        // Add batchSize-1 other conditions
        for (i in 1 until batchSize) {
            val corrIndex: Int = i
            val condition2: RexNode = condition.accept(object : RexShuttle() {
                @Override
                fun visitCorrelVariable(variable: RexCorrelVariable): RexNode {
                    return if (variable.equals(corrVar0)) corrVarList[corrIndex] else variable
                }
            })
            conditionList.add(condition2)
        }

        // Push a filter with batchSize disjunctions
        relBuilder.push(join.getRight()).filter(relBuilder.or(conditionList))
        val right: RelNode = relBuilder.build()
        call.transformTo(
            EnumerableBatchNestedLoopJoin.create(
                convert(
                    join.getLeft(), join.getLeft().getTraitSet()
                        .replace(EnumerableConvention.INSTANCE)
                ),
                convert(
                    right, right.getTraitSet()
                        .replace(EnumerableConvention.INSTANCE)
                ),
                join.getCondition(),
                requiredColumns.build(),
                correlationIds,
                join.getJoinType()
            )
        )
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): EnumerableBatchNestedLoopJoinRule {
            return EnumerableBatchNestedLoopJoinRule(this)
        }

        /** Batch size.
         *
         *
         * Warning: if the batch size is around or bigger than 1000 there
         * can be an error because the generated code exceeds the size limit.  */
        @Value.Default
        fun batchSize(): Int {
            return 100
        }

        /** Sets [.batchSize].  */
        fun withBatchSize(batchSize: Int): Config?

        companion object {
            val DEFAULT: Config = ImmutableEnumerableBatchNestedLoopJoinRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalJoin::class.java).anyInputs() }
                .withDescription("EnumerableBatchNestedLoopJoinRule")
        }
    }
}
