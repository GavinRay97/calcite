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
 * Planner rule that pushes a [org.apache.calcite.rel.core.Sort] past a
 * [org.apache.calcite.rel.core.Join].
 *
 *
 * At the moment, we only consider left/right outer joins.
 * However, an extension for full outer joins for this rule could be envisioned.
 * Special attention should be paid to null values for correctness issues.
 *
 * @see CoreRules.SORT_JOIN_TRANSPOSE
 */
@Value.Enclosing
class SortJoinTransposeRule
/** Creates a SortJoinTransposeRule.  */
protected constructor(config: Config?) : RelRule<SortJoinTransposeRule.Config?>(config), TransformationRule {
    /** Creates a SortJoinTransposeRule.  */
    @Deprecated // to be removed before 2.0
    constructor(
        sortClass: Class<out Sort?>?,
        joinClass: Class<out Join?>?
    ) : this(
        Config.DEFAULT.withOperandFor(sortClass, joinClass)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        sortClass: Class<out Sort?>?,
        joinClass: Class<out Join?>?, relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withOperandFor(sortClass, joinClass)
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val sort: Sort = call.rel(0)
        val join: Join = call.rel(1)
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val joinInfo: JoinInfo = JoinInfo.of(
            join.getLeft(), join.getRight(), join.getCondition()
        )

        // 1) If join is not a left or right outer, we bail out
        // 2) If sort is not a trivial order-by, and if there is
        // any sort column that is not part of the input where the
        // sort is pushed, we bail out
        // 3) If sort has an offset, and if the non-preserved side
        // of the join is not count-preserving against the join
        // condition, we bail out
        if (join.getJoinType() === JoinRelType.LEFT) {
            if (sort.getCollation() !== RelCollations.EMPTY) {
                for (relFieldCollation in sort.getCollation().getFieldCollations()) {
                    if (relFieldCollation.getFieldIndex()
                        >= join.getLeft().getRowType().getFieldCount()
                    ) {
                        return false
                    }
                }
            }
            if (sort.offset != null
                && !RelMdUtil.areColumnsDefinitelyUnique(
                    mq, join.getRight(), joinInfo.rightSet()
                )
            ) {
                return false
            }
        } else if (join.getJoinType() === JoinRelType.RIGHT) {
            if (sort.getCollation() !== RelCollations.EMPTY) {
                for (relFieldCollation in sort.getCollation().getFieldCollations()) {
                    if (relFieldCollation.getFieldIndex()
                        < join.getLeft().getRowType().getFieldCount()
                    ) {
                        return false
                    }
                }
            }
            if (sort.offset != null
                && !RelMdUtil.areColumnsDefinitelyUnique(
                    mq, join.getLeft(), joinInfo.leftSet()
                )
            ) {
                return false
            }
        } else {
            return false
        }
        return true
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        val join: Join = call.rel(1)

        // We create a new sort operator on the corresponding input
        val newLeftInput: RelNode
        val newRightInput: RelNode
        val mq: RelMetadataQuery = call.getMetadataQuery()
        if (join.getJoinType() === JoinRelType.LEFT) {
            // If the input is already sorted and we are not reducing the number of tuples,
            // we bail out
            if (RelMdUtil.checkInputForCollationAndLimit(
                    mq, join.getLeft(),
                    sort.getCollation(), sort.offset, sort.fetch
                )
            ) {
                return
            }
            newLeftInput = sort.copy(
                sort.getTraitSet(), join.getLeft(), sort.getCollation(),
                sort.offset, sort.fetch
            )
            newRightInput = join.getRight()
        } else {
            val rightCollation: RelCollation = RelCollationTraitDef.INSTANCE.canonize(
                RelCollations.shift(
                    sort.getCollation(),
                    -join.getLeft().getRowType().getFieldCount()
                )
            )
            // If the input is already sorted and we are not reducing the number of tuples,
            // we bail out
            if (RelMdUtil.checkInputForCollationAndLimit(
                    mq, join.getRight(),
                    rightCollation, sort.offset, sort.fetch
                )
            ) {
                return
            }
            newLeftInput = join.getLeft()
            newRightInput = sort.copy(
                sort.getTraitSet().replace(rightCollation),
                join.getRight(), rightCollation, sort.offset, sort.fetch
            )
        }
        // We copy the join and the top sort operator
        val joinCopy: RelNode = join.copy(
            join.getTraitSet(), join.getCondition(), newLeftInput,
            newRightInput, join.getJoinType(), join.isSemiJoinDone()
        )
        val sortCopy: RelNode = sort.copy(
            sort.getTraitSet(), joinCopy, sort.getCollation(),
            sort.offset, sort.fetch
        )
        call.transformTo(sortCopy)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SortJoinTransposeRule? {
            return SortJoinTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            sortClass: Class<out Sort?>?,
            joinClass: Class<out Join?>?
        ): Config {
            return withOperandSupplier { b0 ->
                b0.operand(sortClass).oneInput { b1 -> b1.operand(joinClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableSortJoinTransposeRule.Config.of()
                .withOperandFor(LogicalSort::class.java, LogicalJoin::class.java)
        }
    }
}
