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

import org.apache.calcite.plan.Contexts

/**
 * Planner rule that permutes the inputs to a
 * [org.apache.calcite.rel.core.Join].
 *
 *
 * Permutation of outer joins can be turned on/off by specifying the
 * swapOuter flag in the constructor.
 *
 *
 * To preserve the order of columns in the output row, the rule adds a
 * [org.apache.calcite.rel.core.Project].
 *
 * @see CoreRules.JOIN_COMMUTE
 *
 * @see CoreRules.JOIN_COMMUTE_OUTER
 */
@Value.Enclosing
class JoinCommuteRule
/** Creates a JoinCommuteRule.  */
protected constructor(config: Config?) : RelRule<JoinCommuteRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out Join?>?,
        relBuilderFactory: RelBuilderFactory?, swapOuter: Boolean
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(clazz)
            .withSwapOuter(swapOuter)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out Join?>?,
        projectFactory: ProjectFactory?
    ) : this(clazz, RelBuilder.proto(Contexts.of(projectFactory)), false) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out Join?>?,
        projectFactory: ProjectFactory?, swapOuter: Boolean
    ) : this(clazz, RelBuilder.proto(Contexts.of(projectFactory)), swapOuter) {
    }

    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val join: Join = call.rel(0)
        // SEMI and ANTI join cannot be swapped.
        return if (!join.getJoinType().projectsRight()) {
            false
        } else config.isAllowAlwaysTrueCondition()
                || !join.getCondition().isAlwaysTrue()

        // Suppress join with "true" condition (that is, cartesian joins).
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)
        val swapped: RelNode = swap(join, config.isSwapOuter(), call.builder())
            ?: return

        // The result is either a Project or, if the project is trivial, a
        // raw Join.
        val newJoin: Join = if (swapped is Join) swapped as Join else swapped.getInput(0) as Join
        call.transformTo(swapped)

        // We have converted join='a join b' into swapped='select
        // a0,a1,a2,b0,b1 from b join a'. Now register that project='select
        // b0,b1,a0,a1,a2 from (select a0,a1,a2,b0,b1 from b join a)' is the
        // same as 'b join a'. If we didn't do this, the swap join rule
        // would fire on the new join, ad infinitum.
        val relBuilder: RelBuilder = call.builder()
        val exps: List<RexNode> = RelOptUtil.createSwappedJoinExprs(newJoin, join, false)
        relBuilder.push(swapped)
            .project(exps, newJoin.getRowType().getFieldNames())
        call.getPlanner().ensureRegistered(relBuilder.build(), newJoin)
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Walks over an expression, replacing references to fields of the left and
     * right inputs.
     *
     *
     * If the field index is less than leftFieldCount, it must be from the
     * left, and so has rightFieldCount added to it; if the field index is
     * greater than leftFieldCount, it must be from the right, so we subtract
     * leftFieldCount from it.
     */
    private class VariableReplacer internal constructor(
        rexBuilder: RexBuilder,
        leftType: RelDataType,
        rightType: RelDataType
    ) : RexShuttle() {
        private val rexBuilder: RexBuilder
        private val leftFields: List<RelDataTypeField>
        private val rightFields: List<RelDataTypeField>

        init {
            this.rexBuilder = rexBuilder
            leftFields = leftType.getFieldList()
            rightFields = rightType.getFieldList()
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            var index: Int = inputRef.getIndex()
            if (index < leftFields.size()) {
                // Field came from left side of join. Move it to the right.
                return rexBuilder.makeInputRef(
                    leftFields[index].getType(),
                    rightFields.size() + index
                )
            }
            index -= leftFields.size()
            if (index < rightFields.size()) {
                // Field came from right side of join. Move it to the left.
                return rexBuilder.makeInputRef(
                    rightFields[index].getType(),
                    index
                )
            }
            throw AssertionError(
                "Bad field offset: index=" + inputRef.getIndex()
                    .toString() + ", leftFieldCount=" + leftFields.size()
                    .toString() + ", rightFieldCount=" + rightFields.size()
            )
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinCommuteRule? {
            return JoinCommuteRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b ->
                b.operand(joinClass) // FIXME Enable this rule for joins with system fields
                    .predicate { j ->
                        (j.getLeft().getId() !== j.getRight().getId()
                                && j.getSystemFieldList().isEmpty())
                    }
                    .anyInputs()
            }
                .`as`(Config::class.java)
        }

        /** Whether to swap outer joins; default false.  */
        @get:Value.Default
        val isSwapOuter: Boolean
            get() = false

        /** Sets [.isSwapOuter].  */
        fun withSwapOuter(swapOuter: Boolean): Config?

        /** Whether to emit the new join tree if the join condition is `TRUE`
         * (that is, cartesian joins); default true.  */
        @get:Value.Default
        val isAllowAlwaysTrueCondition: Boolean
            get() = true

        /** Sets [.isAllowAlwaysTrueCondition].  */
        fun withAllowAlwaysTrueCondition(allowAlwaysTrueCondition: Boolean): Config?

        companion object {
            val DEFAULT: Config = ImmutableJoinCommuteRule.Config.of()
                .withOperandFor(LogicalJoin::class.java)
                .withSwapOuter(false)
        }
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        @Deprecated // to be removed before 2.0
        @Nullable
        fun swap(join: Join): RelNode? {
            return swap(
                join, false,
                RelFactories.LOGICAL_BUILDER.create(join.getCluster(), null)
            )
        }

        @Deprecated // to be removed before 2.0
        @Nullable
        fun swap(join: Join, swapOuterJoins: Boolean): RelNode? {
            return swap(
                join, swapOuterJoins,
                RelFactories.LOGICAL_BUILDER.create(join.getCluster(), null)
            )
        }

        /**
         * Returns a relational expression with the inputs switched round. Does not
         * modify `join`. Returns null if the join cannot be swapped (for
         * example, because it is an outer join).
         *
         * @param join              join to be swapped
         * @param swapOuterJoins    whether outer joins should be swapped
         * @param relBuilder        Builder for relational expressions
         * @return swapped join if swapping possible; else null
         */
        @Nullable
        fun swap(
            join: Join, swapOuterJoins: Boolean,
            relBuilder: RelBuilder
        ): RelNode? {
            val joinType: JoinRelType = join.getJoinType()
            if (!swapOuterJoins && joinType !== JoinRelType.INNER) {
                return null
            }
            val rexBuilder: RexBuilder = join.getCluster().getRexBuilder()
            val leftRowType: RelDataType = join.getLeft().getRowType()
            val rightRowType: RelDataType = join.getRight().getRowType()
            val variableReplacer = VariableReplacer(rexBuilder, leftRowType, rightRowType)
            val oldCondition: RexNode = join.getCondition()
            val condition: RexNode = variableReplacer.apply(oldCondition)

            // NOTE jvs 14-Mar-2006: We preserve attribute semiJoinDone after the
            // swap.  This way, we will generate one semijoin for the original
            // join, and one for the swapped join, and no more.  This
            // doesn't prevent us from seeing any new combinations assuming
            // that the planner tries the desired order (semi-joins after swaps).
            val newJoin: Join = join.copy(
                join.getTraitSet(), condition, join.getRight(),
                join.getLeft(), joinType.swap(), join.isSemiJoinDone()
            )
            val exps: List<RexNode> = RelOptUtil.createSwappedJoinExprs(newJoin, join, true)
            return relBuilder.push(newJoin)
                .project(exps, join.getRowType().getFieldNames())
                .build()
        }
    }
}
