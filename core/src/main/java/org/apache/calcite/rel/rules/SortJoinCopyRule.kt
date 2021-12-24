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
 * Planner rule that copies a [org.apache.calcite.rel.core.Sort] past a
 * [org.apache.calcite.rel.core.Join] without its limit and offset. The
 * original [org.apache.calcite.rel.core.Sort] is preserved but can be
 * potentially removed by [org.apache.calcite.rel.rules.SortRemoveRule] if
 * redundant.
 *
 *
 * Some examples where [org.apache.calcite.rel.rules.SortJoinCopyRule]
 * can be useful: allowing a [org.apache.calcite.rel.core.Sort] to be
 * incorporated in an index scan; facilitating the use of operators requiring
 * sorted inputs; and allowing the sort to be performed on a possibly smaller
 * result.
 *
 * @see CoreRules.SORT_JOIN_COPY
 */
@Value.Enclosing
class SortJoinCopyRule
/** Creates a SortJoinCopyRule.  */
protected constructor(config: Config?) : RelRule<SortJoinCopyRule.Config?>(config), TransformationRule {
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

    //~ Methods -----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        val join: Join = call.rel(1)
        val metadataQuery: RelMetadataQuery = call.getMetadataQuery()
        val newLeftInput: RelNode
        val newRightInput: RelNode
        val leftFieldCollation: List<RelFieldCollation> = ArrayList()
        val rightFieldCollation: List<RelFieldCollation> = ArrayList()

        // Decompose sort collations into left and right collations
        for (relFieldCollation in sort.getCollation().getFieldCollations()) {
            if (relFieldCollation.getFieldIndex() >= join.getLeft().getRowType().getFieldCount()) {
                rightFieldCollation.add(relFieldCollation)
            } else {
                leftFieldCollation.add(relFieldCollation)
            }
        }

        // Add sort to new left node only if sort collations
        // contained fields from left table
        newLeftInput = if (leftFieldCollation.isEmpty()) {
            join.getLeft()
        } else {
            val leftCollation: RelCollation = RelCollationTraitDef.INSTANCE.canonize(
                RelCollations.of(leftFieldCollation)
            )
            // If left table already sorted don't add a sort
            if (RelMdUtil.checkInputForCollationAndLimit(
                    metadataQuery,
                    join.getLeft(),
                    leftCollation,
                    null,
                    null
                )
            ) {
                join.getLeft()
            } else {
                sort.copy(
                    sort.getTraitSet().replaceIf(
                        RelCollationTraitDef.INSTANCE
                    ) { leftCollation },
                    join.getLeft(),
                    leftCollation,
                    null,
                    null
                )
            }
        }
        // Add sort to new right node only if sort collations
        // contained fields from right table
        newRightInput = if (rightFieldCollation.isEmpty()) {
            join.getRight()
        } else {
            val rightCollation: RelCollation = RelCollationTraitDef.INSTANCE.canonize(
                RelCollations.shift(
                    RelCollations.of(rightFieldCollation),
                    -join.getLeft().getRowType().getFieldCount()
                )
            )
            // If right table already sorted don't add a sort
            if (RelMdUtil.checkInputForCollationAndLimit(
                    metadataQuery,
                    join.getRight(),
                    rightCollation,
                    null,
                    null
                )
            ) {
                join.getRight()
            } else {
                sort.copy(
                    sort.getTraitSet().replaceIf(
                        RelCollationTraitDef.INSTANCE
                    ) { rightCollation },
                    join.getRight(),
                    rightCollation,
                    null,
                    null
                )
            }
        }
        // If no change was made no need to apply the rule
        if (newLeftInput === join.getLeft() && newRightInput === join.getRight()) {
            return
        }
        val joinCopy: RelNode = join.copy(
            join.getTraitSet(),
            join.getCondition(),
            newLeftInput,
            newRightInput,
            join.getJoinType(),
            join.isSemiJoinDone()
        )
        val sortCopy: RelNode = sort.copy(
            sort.getTraitSet(),
            joinCopy,
            sort.getCollation(),
            sort.offset,
            sort.fetch
        )
        call.transformTo(sortCopy)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SortJoinCopyRule? {
            return SortJoinCopyRule(this)
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
            val DEFAULT: Config = ImmutableSortJoinCopyRule.Config.of()
                .withOperandFor(LogicalSort::class.java, LogicalJoin::class.java)
        }
    }
}
