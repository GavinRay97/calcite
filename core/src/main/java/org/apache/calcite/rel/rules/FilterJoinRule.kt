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
 * Planner rule that pushes filters above and
 * within a join node into the join node and/or its children nodes.
 *
 * @param <C> Configuration type
</C> */
abstract class FilterJoinRule<C : FilterJoinRule.Config?>
/** Creates a FilterJoinRule.  */
protected constructor(config: C) : RelRule<C>(config), TransformationRule {
    //~ Methods ----------------------------------------------------------------
    protected fun perform(
        call: RelOptRuleCall, @Nullable filter: Filter?,
        join: Join
    ) {
        val joinFilters: List<RexNode> = RelOptUtil.conjunctions(join.getCondition())
        val origJoinFilters: List<RexNode> = ImmutableList.copyOf(joinFilters)

        // If there is only the joinRel,
        // make sure it does not match a cartesian product joinRel
        // (with "true" condition), otherwise this rule will be applied
        // again on the new cartesian product joinRel.
        if (filter == null && joinFilters.isEmpty()) {
            return
        }
        val aboveFilters: List<RexNode> = if (filter != null) getConjunctions(filter) else ArrayList()
        val origAboveFilters: ImmutableList<RexNode> = ImmutableList.copyOf(aboveFilters)

        // Simplify Outer Joins
        var joinType: JoinRelType = join.getJoinType()
        if (config.isSmart()
            && !origAboveFilters.isEmpty()
            && join.getJoinType() !== JoinRelType.INNER
        ) {
            joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType)
        }
        val leftFilters: List<RexNode> = ArrayList()
        val rightFilters: List<RexNode> = ArrayList()

        // TODO - add logic to derive additional filters.  E.g., from
        // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
        // derive table filters:
        // (t1.a = 1 OR t1.b = 3)
        // (t2.a = 2 OR t2.b = 4)

        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        var filterPushed = false
        if (RelOptUtil.classifyFilters(
                join,
                aboveFilters,
                joinType.canPushIntoFromAbove(),
                joinType.canPushLeftFromAbove(),
                joinType.canPushRightFromAbove(),
                joinFilters,
                leftFilters,
                rightFilters
            )
        ) {
            filterPushed = true
        }

        // Move join filters up if needed
        validateJoinFilters(aboveFilters, joinFilters, join, joinType)

        // If no filter got pushed after validate, reset filterPushed flag
        if (leftFilters.isEmpty()
            && rightFilters.isEmpty()
            && joinFilters.size() === origJoinFilters.size() && aboveFilters.size() === origAboveFilters.size()
        ) {
            if (Sets.newHashSet(joinFilters)
                    .equals(Sets.newHashSet(origJoinFilters))
            ) {
                filterPushed = false
            }
        }

        // Try to push down filters in ON clause. A ON clause filter can only be
        // pushed down if it does not affect the non-matching set, i.e. it is
        // not on the side which is preserved.

        // Anti-join on conditions can not be pushed into left or right, e.g. for plan:
        //
        //     Join(condition=[AND(cond1, $2)], joinType=[anti])
        //     :  - prj(f0=[$0], f1=[$1], f2=[$2])
        //     :  - prj(f0=[$0])
        //
        // The semantic would change if join condition $2 is pushed into left,
        // that is, the result set may be smaller. The right can not be pushed
        // into for the same reason.
        if (RelOptUtil.classifyFilters(
                join,
                joinFilters,
                false,
                joinType.canPushLeftFromWithin(),
                joinType.canPushRightFromWithin(),
                joinFilters,
                leftFilters,
                rightFilters
            )
        ) {
            filterPushed = true
        }

        // if nothing actually got pushed and there is nothing leftover,
        // then this rule is a no-op
        if ((!filterPushed
                    && joinType === join.getJoinType())
            || (joinFilters.isEmpty()
                    && leftFilters.isEmpty()
                    && rightFilters.isEmpty())
        ) {
            return
        }

        // create Filters on top of the children if any filters were
        // pushed to them
        val rexBuilder: RexBuilder = join.getCluster().getRexBuilder()
        val relBuilder: RelBuilder = call.builder()
        val leftRel: RelNode = relBuilder.push(join.getLeft()).filter(leftFilters).build()
        val rightRel: RelNode = relBuilder.push(join.getRight()).filter(rightFilters).build()

        // create the new join node referencing the new children and
        // containing its new join filters (if there are any)
        val fieldTypes: ImmutableList<RelDataType> = ImmutableList.< RelDataType > builder < RelDataType ? > ()
            .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
            .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build()
        val joinFilter: RexNode = RexUtil.composeConjunction(
            rexBuilder,
            RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes)
        )

        // If nothing actually got pushed and there is nothing leftover,
        // then this rule is a no-op
        if (joinFilter.isAlwaysTrue()
            && leftFilters.isEmpty()
            && rightFilters.isEmpty()
            && joinType === join.getJoinType()
        ) {
            return
        }
        val newJoinRel: RelNode = join.copy(
            join.getTraitSet(),
            joinFilter,
            leftRel,
            rightRel,
            joinType,
            join.isSemiJoinDone()
        )
        call.getPlanner().onCopy(join, newJoinRel)
        if (!leftFilters.isEmpty() && filter != null) {
            call.getPlanner().onCopy(filter, leftRel)
        }
        if (!rightFilters.isEmpty() && filter != null) {
            call.getPlanner().onCopy(filter, rightRel)
        }
        relBuilder.push(newJoinRel)

        // Create a project on top of the join if some of the columns have become
        // NOT NULL due to the join-type getting stricter.
        relBuilder.convert(join.getRowType(), false)

        // create a FilterRel on top of the join if needed
        relBuilder.filter(
            RexUtil.fixUp(
                rexBuilder, aboveFilters,
                RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())
            )
        )
        call.transformTo(relBuilder.build())
    }

    /**
     * Validates that target execution framework can satisfy join filters.
     *
     *
     * If the join filter cannot be satisfied (for example, if it is
     * `l.c1 > r.c2` and the join only supports equi-join), removes the
     * filter from `joinFilters` and adds it to `aboveFilters`.
     *
     *
     * The default implementation does nothing; i.e. the join can handle all
     * conditions.
     *
     * @param aboveFilters Filter above Join
     * @param joinFilters Filters in join condition
     * @param join Join
     * @param joinType JoinRelType could be different from type in Join due to
     * outer join simplification.
     */
    protected fun validateJoinFilters(
        aboveFilters: List<RexNode?>,
        joinFilters: List<RexNode?>, join: Join?, joinType: JoinRelType
    ) {
        val filterIter: Iterator<RexNode?> = joinFilters.iterator()
        while (filterIter.hasNext()) {
            val exp: RexNode? = filterIter.next()
            // Do not pull up filter conditions for semi/anti join.
            if (!config.getPredicate().apply(join, joinType, exp)
                && joinType.projectsRight()
            ) {
                aboveFilters.add(exp)
                filterIter.remove()
            }
        }
    }

    /** Rule that pushes parts of the join condition to its inputs.  */
    class JoinConditionPushRule
    /** Creates a JoinConditionPushRule.  */
    protected constructor(config: JoinConditionPushRuleConfig) :
        FilterJoinRule<JoinConditionPushRule.JoinConditionPushRuleConfig?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            relBuilderFactory: RelBuilderFactory?,
            predicate: Predicate?
        ) : this(ImmutableJoinConditionPushRuleConfig.of(predicate)
            .withRelBuilderFactory(relBuilderFactory)
            .withOperandSupplier { b -> b.operand(Join::class.java).anyInputs() }
            .withDescription("FilterJoinRule:no-filter")
            .withSmart(true)) {
        }

        @Deprecated // to be removed before 2.0
        constructor(
            filterFactory: FilterFactory?,
            projectFactory: RelFactories.ProjectFactory?, predicate: Predicate?
        ) : this(RelBuilder.proto(filterFactory, projectFactory), predicate) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val join: Join = call.rel(0)
            perform(call, null, join)
        }

        /** Rule configuration.  */
        @Value.Immutable(singleton = false)
        interface JoinConditionPushRuleConfig : Config {
            @Override
            fun toRule(): JoinConditionPushRule {
                return JoinConditionPushRule(this)
            }

            companion object {
                val DEFAULT: JoinConditionPushRuleConfig = ImmutableJoinConditionPushRuleConfig
                    .of { join, joinType, exp -> true }
                    .withOperandSupplier { b -> b.operand(Join::class.java).anyInputs() }
                    .withSmart(true)
            }
        }
    }

    /** Rule that tries to push filter expressions into a join
     * condition and into the inputs of the join.
     *
     * @see CoreRules.FILTER_INTO_JOIN
     */
    class FilterIntoJoinRule
    /** Creates a FilterIntoJoinRule.  */
    protected constructor(config: FilterIntoJoinRuleConfig) :
        FilterJoinRule<FilterIntoJoinRule.FilterIntoJoinRuleConfig?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            smart: Boolean,
            relBuilderFactory: RelBuilderFactory?, predicate: Predicate?
        ) : this(ImmutableFilterIntoJoinRuleConfig.of(predicate)
            .withRelBuilderFactory(relBuilderFactory)
            .withOperandSupplier { b0 ->
                b0.operand(Filter::class.java).oneInput { b1 -> b1.operand(Join::class.java).anyInputs() }
            }
            .withDescription("FilterJoinRule:filter")
            .withSmart(smart)) {
        }

        @Deprecated // to be removed before 2.0
        constructor(
            smart: Boolean,
            filterFactory: FilterFactory?,
            projectFactory: RelFactories.ProjectFactory?,
            predicate: Predicate?
        ) : this(ImmutableFilterIntoJoinRuleConfig.of(predicate)
            .withRelBuilderFactory(
                RelBuilder.proto(filterFactory, projectFactory)
            )
            .withOperandSupplier { b0 ->
                b0.operand(Filter::class.java).oneInput { b1 -> b1.operand(Join::class.java).anyInputs() }
            }
            .withDescription("FilterJoinRule:filter")
            .withSmart(smart)) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val filter: Filter = call.rel(0)
            val join: Join = call.rel(1)
            perform(call, filter, join)
        }

        /** Rule configuration.  */
        @Value.Immutable(singleton = false)
        interface FilterIntoJoinRuleConfig : Config {
            @Override
            fun toRule(): FilterIntoJoinRule {
                return FilterIntoJoinRule(this)
            }

            companion object {
                val DEFAULT: FilterIntoJoinRuleConfig =
                    ImmutableFilterIntoJoinRuleConfig.of { join, joinType, exp -> true }
                        .withOperandSupplier { b0 ->
                            b0.operand(Filter::class.java).oneInput { b1 -> b1.operand(Join::class.java).anyInputs() }
                        }
                        .withSmart(true)
            }
        }
    }

    /** Predicate that returns whether a filter is valid in the ON clause of a
     * join for this particular kind of join. If not, Calcite will push it back to
     * above the join.  */
    @FunctionalInterface
    interface Predicate {
        fun apply(join: Join?, joinType: JoinRelType?, exp: RexNode?): Boolean
    }

    /**
     * Rule configuration.
     */
    interface Config : RelRule.Config {
        /** Whether to try to strengthen join-type, default false.  */
        @get:Value.Default
        val isSmart: Boolean
            get() = false

        /** Sets [.isSmart].  */
        fun withSmart(smart: Boolean): Config?

        /** Predicate that returns whether a filter is valid in the ON clause of a
         * join for this particular kind of join. If not, Calcite will push it back to
         * above the join.  */
        @get:Value.Parameter
        val predicate: Predicate?

        /** Sets [.getPredicate] ()}.  */
        fun withPredicate(predicate: Predicate?): Config?
    }

    companion object {
        /** Predicate that always returns true. With this predicate, every filter
         * will be pushed into the ON clause.  */
        @Deprecated // to be removed before 2.0
        val TRUE_PREDICATE: Predicate = Predicate { join: Join?, joinType: JoinRelType?, exp: RexNode? -> true }

        /**
         * Get conjunctions of filter's condition but with collapsed
         * `IS NOT DISTINCT FROM` expressions if needed.
         *
         * @param filter filter containing condition
         * @return condition conjunctions with collapsed `IS NOT DISTINCT FROM`
         * expressions if any
         * @see RelOptUtil.conjunctions
         */
        private fun getConjunctions(filter: Filter): List<RexNode> {
            val conjunctions: List<RexNode> = conjunctions(filter.getCondition())
            val rexBuilder: RexBuilder = filter.getCluster().getRexBuilder()
            for (i in 0 until conjunctions.size()) {
                val node: RexNode = conjunctions[i]
                if (node is RexCall) {
                    conjunctions.set(
                        i,
                        RelOptUtil.collapseExpandedIsNotDistinctFromExpr(node as RexCall, rexBuilder)
                    )
                }
            }
            return conjunctions
        }
    }
}
