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
 * Planner rule that pushes a [semi-join][Join.isSemiJoin]
 * down in a tree past a [org.apache.calcite.rel.core.Join]
 * in order to trigger other rules that will convert `SemiJoin`s.
 *
 *
 *  * SemiJoin(LogicalJoin(X, Y), Z)  LogicalJoin(SemiJoin(X, Z), Y)
 *  * SemiJoin(LogicalJoin(X, Y), Z)  LogicalJoin(X, SemiJoin(Y, Z))
 *
 *
 *
 * Whether this
 * first or second conversion is applied depends on which operands actually
 * participate in the semi-join.
 *
 * @see CoreRules.SEMI_JOIN_JOIN_TRANSPOSE
 */
@Value.Enclosing
class SemiJoinJoinTransposeRule
/** Creates a SemiJoinJoinTransposeRule.  */
protected constructor(config: Config?) : RelRule<SemiJoinJoinTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val semiJoin: Join = call.rel(0)
        val join: Join = call.rel(1)
        if (join.isSemiJoin()) {
            return
        }
        val leftKeys: ImmutableIntList = semiJoin.analyzeCondition().leftKeys

        // X is the left child of the join below the semi-join
        // Y is the right child of the join below the semi-join
        // Z is the right child of the semi-join
        val nFieldsX: Int = join.getLeft().getRowType().getFieldList().size()
        val nFieldsY: Int = join.getRight().getRowType().getFieldList().size()
        val nFieldsZ: Int = semiJoin.getRight().getRowType().getFieldList().size()
        val nTotalFields = nFieldsX + nFieldsY + nFieldsZ
        val fields: List<RelDataTypeField> = ArrayList()

        // create a list of fields for the full join result; note that
        // we can't simply use the fields from the semi-join because the
        // row-type of a semi-join only includes the left hand side fields
        var joinFields: List<RelDataTypeField?> = semiJoin.getRowType().getFieldList()
        for (i in 0 until nFieldsX + nFieldsY) {
            fields.add(joinFields[i])
        }
        joinFields = semiJoin.getRight().getRowType().getFieldList()
        for (i in 0 until nFieldsZ) {
            fields.add(joinFields[i])
        }

        // determine which operands below the semi-join are the actual
        // Rels that participate in the semi-join
        var nKeysFromX = 0
        for (leftKey in leftKeys) {
            if (leftKey < nFieldsX) {
                nKeysFromX++
            }
        }
        assert(nKeysFromX == 0 || nKeysFromX == leftKeys.size())

        // need to convert the semi-join condition and possibly the keys
        val newSemiJoinFilter: RexNode
        val adjustments = IntArray(nTotalFields)
        newSemiJoinFilter = if (nKeysFromX > 0) {
            // (X, Y, Z) --> (X, Z, Y)
            // semiJoin(X, Z)
            // pass 0 as Y's adjustment because there shouldn't be any
            // references to Y in the semi-join filter
            setJoinAdjustments(
                adjustments,
                nFieldsX,
                nFieldsY,
                nFieldsZ,
                0,
                -nFieldsY
            )
            semiJoin.getCondition().accept(
                RexInputConverter(
                    semiJoin.getCluster().getRexBuilder(),
                    fields,
                    adjustments
                )
            )
        } else {
            // (X, Y, Z) --> (X, Y, Z)
            // semiJoin(Y, Z)
            setJoinAdjustments(
                adjustments,
                nFieldsX,
                nFieldsY,
                nFieldsZ,
                -nFieldsX,
                -nFieldsX
            )
            semiJoin.getCondition().accept(
                RexInputConverter(
                    semiJoin.getCluster().getRexBuilder(),
                    fields,
                    adjustments
                )
            )
        }

        // create the new join
        val leftSemiJoinOp: RelNode
        leftSemiJoinOp = if (nKeysFromX > 0) {
            join.getLeft()
        } else {
            join.getRight()
        }
        val newSemiJoin: LogicalJoin = LogicalJoin.create(
            leftSemiJoinOp,
            semiJoin.getRight(),  // No need to copy the hints, the framework would try to do that.
            ImmutableList.of(),
            newSemiJoinFilter,
            ImmutableSet.of(),
            JoinRelType.SEMI
        )
        val left: RelNode
        val right: RelNode
        if (nKeysFromX > 0) {
            left = newSemiJoin
            right = join.getRight()
        } else {
            left = join.getLeft()
            right = newSemiJoin
        }
        val newJoin: RelNode = join.copy(
            join.getTraitSet(),
            join.getCondition(),
            left,
            right,
            join.getJoinType(),
            join.isSemiJoinDone()
        )
        call.transformTo(newJoin)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SemiJoinJoinTransposeRule? {
            return SemiJoinJoinTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            joinClass: Class<out Join?>?,
            join2Class: Class<out Join?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(joinClass).predicate(Join::isSemiJoin).inputs { b1 -> b1.operand(join2Class).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableSemiJoinJoinTransposeRule.Config.of()
                .withOperandFor(LogicalJoin::class.java, Join::class.java)
        }
    }

    companion object {
        /**
         * Sets an array to reflect how much each index corresponding to a field
         * needs to be adjusted. The array corresponds to fields in a 3-way join
         * between (X, Y, and Z). X remains unchanged, but Y and Z need to be
         * adjusted by some fixed amount as determined by the input.
         *
         * @param adjustments array to be filled out
         * @param nFieldsX    number of fields in X
         * @param nFieldsY    number of fields in Y
         * @param nFieldsZ    number of fields in Z
         * @param adjustY     the amount to adjust Y by
         * @param adjustZ     the amount to adjust Z by
         */
        private fun setJoinAdjustments(
            adjustments: IntArray,
            nFieldsX: Int,
            nFieldsY: Int,
            nFieldsZ: Int,
            adjustY: Int,
            adjustZ: Int
        ) {
            for (i in 0 until nFieldsX) {
                adjustments[i] = 0
            }
            for (i in nFieldsX until nFieldsX + nFieldsY) {
                adjustments[i] = adjustY
            }
            for (i in nFieldsX + nFieldsY until nFieldsX + nFieldsY + nFieldsZ) {
                adjustments[i] = adjustZ
            }
        }
    }
}
