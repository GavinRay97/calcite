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
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.`fun`.SqlSumEmptyIsZeroAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.Optionality
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import org.immutables.value.Value
import java.util.ArrayList
import java.util.Collection
import java.util.HashMap
import java.util.HashSet
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.NavigableSet
import java.util.Set
import java.util.TreeSet
import java.util.stream.Collectors
import java.util.stream.Stream
import java.util.Objects.requireNonNull

/**
 * Planner rule that expands distinct aggregates
 * (such as `COUNT(DISTINCT x)`) from a
 * [org.apache.calcite.rel.core.Aggregate].
 *
 *
 * How this is done depends upon the arguments to the function. If all
 * functions have the same argument
 * (e.g. `COUNT(DISTINCT x), SUM(DISTINCT x)` both have the argument
 * `x`) then one extra [org.apache.calcite.rel.core.Aggregate] is
 * sufficient.
 *
 *
 * If there are multiple arguments
 * (e.g. `COUNT(DISTINCT x), COUNT(DISTINCT y)`)
 * the rule creates separate `Aggregate`s and combines using a
 * [org.apache.calcite.rel.core.Join].
 *
 * @see CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES
 *
 * @see CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN
 */
@Value.Enclosing
class AggregateExpandDistinctAggregatesRule
/** Creates an AggregateExpandDistinctAggregatesRule.  */
internal constructor(config: Config?) : RelRule<AggregateExpandDistinctAggregatesRule.Config?>(config),
    TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out Aggregate?>?,
        useGroupingSets: Boolean,
        relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.operand(clazz).anyInputs() }
        .`as`(Config::class.java)
        .withUsingGroupingSets(useGroupingSets)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out LogicalAggregate?>?,
        useGroupingSets: Boolean,
        joinFactory: RelFactories.JoinFactory?
    ) : this(clazz, useGroupingSets, RelBuilder.proto(Contexts.of(joinFactory))) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out LogicalAggregate?>?,
        joinFactory: RelFactories.JoinFactory?
    ) : this(clazz, false, RelBuilder.proto(Contexts.of(joinFactory))) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        if (!aggregate.containsDistinctCall()) {
            return
        }

        // Find all of the agg expressions. We use a LinkedHashSet to ensure determinism.
        val aggCalls: List<AggregateCall> = aggregate.getAggCallList()
        // Find all aggregate calls with distinct
        val distinctAggCalls: List<AggregateCall> = aggCalls.stream()
            .filter(AggregateCall::isDistinct).collect(Collectors.toList())
        // Find all aggregate calls without distinct
        val nonDistinctAggCalls: List<AggregateCall> = aggCalls.stream()
            .filter { aggCall -> !aggCall.isDistinct() }.collect(Collectors.toList())
        val filterCount: Long = aggCalls.stream()
            .filter { aggCall -> aggCall.filterArg >= 0 }.count()
        val unsupportedNonDistinctAggCallCount: Long = nonDistinctAggCalls.stream()
            .filter { aggCall ->
                val aggCallKind: SqlKind = aggCall.getAggregation().getKind()
                when (aggCallKind) {
                    COUNT, SUM, SUM0, MIN, MAX -> return@filter false
                    else -> return@filter true
                }
            }.count()
        // Argument list of distinct agg calls.
        val distinctCallArgLists: Set<Pair<List<Integer>, Integer>> = distinctAggCalls.stream()
            .map { aggCall -> Pair.of(aggCall.getArgList(), aggCall.filterArg) }
            .collect(Collectors.toCollection { LinkedHashSet() })
        Preconditions.checkState(
            distinctCallArgLists.size() > 0,
            "containsDistinctCall lied"
        )

        // If all of the agg expressions are distinct and have the same
        // arguments then we can use a more efficient form.

        // MAX, MIN, BIT_AND, BIT_OR always ignore distinct attribute,
        // when they are mixed in with other distinct agg calls,
        // we can still use this promotion.

        // Treat the agg expression with Optionality.IGNORED as distinct and
        // re-statistic the non-distinct agg call count and the distinct agg
        // call arguments.
        val nonDistinctAggCallsOfIgnoredOptionality: List<AggregateCall> = nonDistinctAggCalls.stream()
            .filter { aggCall -> aggCall.getAggregation().getDistinctOptionality() === Optionality.IGNORED }
            .collect(Collectors.toList())
        // Different with distinctCallArgLists, this list also contains args that come from
        // agg call which can ignore the distinct constraint.
        val distinctCallArgLists2: Set<Pair<List<Integer>, Integer>> =
            Stream.of(distinctAggCalls, nonDistinctAggCallsOfIgnoredOptionality)
                .flatMap(Collection::stream)
                .map { aggCall -> Pair.of(aggCall.getArgList(), aggCall.filterArg) }
                .collect(Collectors.toCollection { LinkedHashSet() })
        if (nonDistinctAggCalls.size() - nonDistinctAggCallsOfIgnoredOptionality.size() === 0 && distinctCallArgLists2.size() === 1 && aggregate.getGroupType() === Group.SIMPLE) {
            val pair: Pair<List<Integer>, Integer> = Iterables.getOnlyElement(distinctCallArgLists2)
            val relBuilder: RelBuilder = call.builder()
            convertMonopole(relBuilder, aggregate, pair.left, pair.right)
            call.transformTo(relBuilder.build())
            return
        }
        if ((config as Config).isUsingGroupingSets) {
            rewriteUsingGroupingSets(call, aggregate)
            return
        }

        // If only one distinct aggregate and one or more non-distinct aggregates,
        // we can generate multi-phase aggregates
        if (distinctAggCalls.size() === 1 // one distinct aggregate
            && filterCount == 0L && unsupportedNonDistinctAggCallCount == 0L && nonDistinctAggCalls.size() > 0
        ) { // one or more non-distinct aggregates
            val relBuilder: RelBuilder = call.builder()
            convertSingletonDistinct(relBuilder, aggregate, distinctCallArgLists)
            call.transformTo(relBuilder.build())
            return
        }

        // Create a list of the expressions which will yield the final result.
        // Initially, the expressions point to the input field.
        val aggFields: List<RelDataTypeField> = aggregate.getRowType().getFieldList()
        val refs: List<RexInputRef> = ArrayList()
        val fieldNames: List<String> = aggregate.getRowType().getFieldNames()
        val groupSet: ImmutableBitSet = aggregate.getGroupSet()
        val groupCount: Int = aggregate.getGroupCount()
        for (i in Util.range(groupCount)) {
            refs.add(RexInputRef.of(i, aggFields))
        }

        // Aggregate the original relation, including any non-distinct aggregates.
        val newAggCallList: List<AggregateCall> = ArrayList()
        var i = -1
        for (aggCall in aggregate.getAggCallList()) {
            ++i
            if (aggCall.isDistinct()) {
                refs.add(null)
                continue
            }
            refs.add(
                RexInputRef(
                    groupCount + newAggCallList.size(),
                    aggFields[groupCount + i].getType()
                )
            )
            newAggCallList.add(aggCall)
        }

        // In the case where there are no non-distinct aggregates (regardless of
        // whether there are group bys), there's no need to generate the
        // extra aggregate and join.
        val relBuilder: RelBuilder = call.builder()
        relBuilder.push(aggregate.getInput())
        var n = 0
        if (!newAggCallList.isEmpty()) {
            val groupKey: RelBuilder.GroupKey = relBuilder.groupKey(groupSet, aggregate.getGroupSets())
            relBuilder.aggregate(groupKey, newAggCallList)
            ++n
        }

        // For each set of operands, find and rewrite all calls which have that
        // set of operands.
        for (argList in distinctCallArgLists) {
            doRewrite(relBuilder, aggregate, n++, argList.left, argList.right, refs)
        }
        // It is assumed doRewrite above replaces nulls in refs
        @SuppressWarnings("assignment.type.incompatible") val nonNullRefs: List<RexInputRef> = refs
        relBuilder.project(nonNullRefs, fieldNames)
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateExpandDistinctAggregatesRule? {
            return AggregateExpandDistinctAggregatesRule(this)
        }

        /** Whether to use GROUPING SETS, default true.  */
        @get:Value.Default
        val isUsingGroupingSets: Boolean
            get() = true

        /** Sets [.isUsingGroupingSets].  */
        fun withUsingGroupingSets(usingGroupingSets: Boolean): Config

        companion object {
            val DEFAULT: Config = ImmutableAggregateExpandDistinctAggregatesRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalAggregate::class.java).anyInputs() }
            val JOIN = DEFAULT.withUsingGroupingSets(false)
        }
    }

    companion object {
        /**
         * Converts an aggregate with one distinct aggregate and one or more
         * non-distinct aggregates to multi-phase aggregates (see reference example
         * below).
         *
         * @param relBuilder Contains the input relational expression
         * @param aggregate  Original aggregate
         * @param argLists   Arguments and filters to the distinct aggregate function
         */
        private fun convertSingletonDistinct(
            relBuilder: RelBuilder,
            aggregate: Aggregate, argLists: Set<Pair<List<Integer>, Integer>>
        ): RelBuilder {

            // In this case, we are assuming that there is a single distinct function.
            // So make sure that argLists is of size one.
            Preconditions.checkArgument(argLists.size() === 1)

            // For example,
            //    SELECT deptno, COUNT(*), SUM(bonus), MIN(DISTINCT sal)
            //    FROM emp
            //    GROUP BY deptno
            //
            // becomes
            //
            //    SELECT deptno, SUM(cnt), SUM(bonus), MIN(sal)
            //    FROM (
            //          SELECT deptno, COUNT(*) as cnt, SUM(bonus), sal
            //          FROM EMP
            //          GROUP BY deptno, sal)            // Aggregate B
            //    GROUP BY deptno                        // Aggregate A
            relBuilder.push(aggregate.getInput())
            val originalAggCalls: List<AggregateCall> = aggregate.getAggCallList()
            val originalGroupSet: ImmutableBitSet = aggregate.getGroupSet()

            // Add the distinct aggregate column(s) to the group-by columns,
            // if not already a part of the group-by
            val bottomGroups: NavigableSet<Integer> = TreeSet(aggregate.getGroupSet().asList())
            for (aggCall in originalAggCalls) {
                if (aggCall.isDistinct()) {
                    bottomGroups.addAll(aggCall.getArgList())
                    break // since we only have single distinct call
                }
            }
            val bottomGroupSet: ImmutableBitSet = ImmutableBitSet.of(bottomGroups)

            // Generate the intermediate aggregate B, the one on the bottom that converts
            // a distinct call to group by call.
            // Bottom aggregate is the same as the original aggregate, except that
            // the bottom aggregate has converted the DISTINCT aggregate to a group by clause.
            val bottomAggregateCalls: List<AggregateCall> = ArrayList()
            for (aggCall in originalAggCalls) {
                // Project the column corresponding to the distinct aggregate. Project
                // as-is all the non-distinct aggregates
                if (!aggCall.isDistinct()) {
                    val newCall: AggregateCall = AggregateCall.create(
                        aggCall.getAggregation(), false,
                        aggCall.isApproximate(), aggCall.ignoreNulls(),
                        aggCall.getArgList(), -1, aggCall.distinctKeys,
                        aggCall.collation, bottomGroupSet.cardinality(),
                        relBuilder.peek(), null, aggCall.name
                    )
                    bottomAggregateCalls.add(newCall)
                }
            }
            // Generate the aggregate B (see the reference example above)
            relBuilder.push(
                aggregate.copy(
                    aggregate.getTraitSet(), relBuilder.build(),
                    bottomGroupSet, null, bottomAggregateCalls
                )
            )

            // Add aggregate A (see the reference example above), the top aggregate
            // to handle the rest of the aggregation that the bottom aggregate hasn't handled
            val topAggregateCalls: List<AggregateCall> = ArrayList()
            // Use the remapped arguments for the (non)distinct aggregate calls
            var nonDistinctAggCallProcessedSoFar = 0
            for (aggCall in originalAggCalls) {
                val newCall: AggregateCall
                if (aggCall.isDistinct()) {
                    val newArgList: List<Integer> = ArrayList()
                    for (arg in aggCall.getArgList()) {
                        newArgList.add(bottomGroups.headSet(arg, false).size())
                    }
                    newCall = AggregateCall.create(
                        aggCall.getAggregation(),
                        false,
                        aggCall.isApproximate(),
                        aggCall.ignoreNulls(),
                        newArgList,
                        -1,
                        aggCall.distinctKeys,
                        aggCall.collation,
                        originalGroupSet.cardinality(),
                        relBuilder.peek(),
                        aggCall.getType(),
                        aggCall.name
                    )
                } else {
                    // If aggregate B had a COUNT aggregate call the corresponding aggregate at
                    // aggregate A must be SUM. For other aggregates, it remains the same.
                    val arg: Int = bottomGroups.size() + nonDistinctAggCallProcessedSoFar
                    val newArgs: List<Integer> = ImmutableList.of(arg)
                    newCall = if (aggCall.getAggregation().getKind() === SqlKind.COUNT) {
                        AggregateCall.create(
                            SqlSumEmptyIsZeroAggFunction(), false,
                            aggCall.isApproximate(), aggCall.ignoreNulls(),
                            newArgs, -1, aggCall.distinctKeys, aggCall.collation,
                            originalGroupSet.cardinality(), relBuilder.peek(),
                            null,
                            aggCall.getName()
                        )
                    } else {
                        AggregateCall.create(
                            aggCall.getAggregation(), false,
                            aggCall.isApproximate(), aggCall.ignoreNulls(),
                            newArgs, -1, aggCall.distinctKeys, aggCall.collation,
                            originalGroupSet.cardinality(),
                            relBuilder.peek(), null, aggCall.name
                        )
                    }
                    nonDistinctAggCallProcessedSoFar++
                }
                topAggregateCalls.add(newCall)
            }

            // Populate the group-by keys with the remapped arguments for aggregate A
            // The top groupset is basically an identity (first X fields of aggregate B's
            // output), minus the distinct aggCall's input.
            val topGroupSet: Set<Integer> = HashSet()
            var groupSetToAdd = 0
            for (bottomGroup in bottomGroups) {
                if (originalGroupSet.get(bottomGroup)) {
                    topGroupSet.add(groupSetToAdd)
                }
                groupSetToAdd++
            }
            relBuilder.push(
                aggregate.copy(
                    aggregate.getTraitSet(), relBuilder.build(),
                    ImmutableBitSet.of(topGroupSet), null, topAggregateCalls
                )
            )

            // Add projection node for case: SUM of COUNT(*):
            // Type of the SUM may be larger than type of COUNT.
            // CAST to original type must be added.
            relBuilder.convert(aggregate.getRowType(), true)
            return relBuilder
        }

        private fun rewriteUsingGroupingSets(
            call: RelOptRuleCall,
            aggregate: Aggregate
        ) {
            val groupSetTreeSet: Set<ImmutableBitSet> = TreeSet(ImmutableBitSet.ORDERING)
            // GroupSet to distinct filter arg map,
            // filterArg will be -1 for non-distinct agg call.

            // Using `Set` here because it's possible that two agg calls
            // have different filterArgs but same groupSet.
            val distinctFilterArgMap: Map<ImmutableBitSet, Set<Integer>> = HashMap()
            for (aggCall in aggregate.getAggCallList()) {
                var groupSet: ImmutableBitSet
                var filterArg: Int
                if (!aggCall.isDistinct()) {
                    filterArg = -1
                    groupSet = aggregate.getGroupSet()
                    groupSetTreeSet.add(aggregate.getGroupSet())
                } else {
                    filterArg = aggCall.filterArg
                    groupSet = ImmutableBitSet.of(aggCall.getArgList())
                        .setIf(filterArg, filterArg >= 0)
                        .union(aggregate.getGroupSet())
                    groupSetTreeSet.add(groupSet)
                }
                val filterList: Set<Integer> = distinctFilterArgMap
                    .computeIfAbsent(groupSet) { g -> HashSet() }
                filterList.add(filterArg)
            }
            val groupSets: ImmutableList<ImmutableBitSet> = ImmutableList.copyOf(groupSetTreeSet)
            val fullGroupSet: ImmutableBitSet = ImmutableBitSet.union(groupSets)
            val distinctAggCalls: List<AggregateCall> = ArrayList()
            for (aggCall in aggregate.getNamedAggCalls()) {
                if (!aggCall.left.isDistinct()) {
                    val newAggCall: AggregateCall = aggCall.left.adaptTo(
                        aggregate.getInput(),
                        aggCall.left.getArgList(), aggCall.left.filterArg,
                        aggregate.getGroupCount(), fullGroupSet.cardinality()
                    )
                    distinctAggCalls.add(newAggCall.withName(aggCall.right))
                }
            }
            val relBuilder: RelBuilder = call.builder()
            relBuilder.push(aggregate.getInput())
            val groupCount: Int = fullGroupSet.cardinality()

            // Get the base ordinal of filter args for different groupSets.
            val filters: Map<Pair<ImmutableBitSet, Integer>, Integer> = LinkedHashMap()
            var z: Int = groupCount + distinctAggCalls.size()
            for (groupSet in groupSets) {
                val filterArgList: Set<Integer> = distinctFilterArgMap[groupSet]!!
                for (filterArg in requireNonNull(filterArgList, "filterArgList")) {
                    filters.put(Pair.of(groupSet, filterArg), z)
                    z += 1
                }
            }
            distinctAggCalls.add(
                AggregateCall.create(
                    SqlStdOperatorTable.GROUPING, false, false, false,
                    ImmutableIntList.copyOf(fullGroupSet), -1,
                    null, RelCollations.EMPTY,
                    groupSets.size(), relBuilder.peek(), null, "\$g"
                )
            )
            relBuilder.aggregate(
                relBuilder.groupKey(fullGroupSet, groupSets),
                distinctAggCalls
            )

            // GROUPING returns an integer (0 or 1). Add a project to convert those
            // values to BOOLEAN.
            if (!filters.isEmpty()) {
                val nodes: List<RexNode> = ArrayList(relBuilder.fields())
                val nodeZ: RexNode = nodes.remove(nodes.size() - 1)
                for (entry in filters.entrySet()) {
                    val v = groupValue(fullGroupSet.asList(), entry.getKey().left)
                    val distinctFilterArg: Int = remap(fullGroupSet, entry.getKey().right)
                    var expr: RexNode = relBuilder.equals(nodeZ, relBuilder.literal(v))
                    if (distinctFilterArg > -1) {
                        // 'AND' the filter of the distinct aggregate call and the group value.
                        expr = relBuilder.and(
                            expr,
                            relBuilder.call(
                                SqlStdOperatorTable.IS_TRUE,
                                relBuilder.field(distinctFilterArg)
                            )
                        )
                    }
                    // "f" means filter.
                    nodes.add(
                        relBuilder.alias(
                            expr,
                            "\$g_" + v + if (distinctFilterArg < 0) "" else "_f_$distinctFilterArg"
                        )
                    )
                }
                relBuilder.project(nodes)
            }
            var x = groupCount
            val groupSet: ImmutableBitSet = aggregate.getGroupSet()
            val newCalls: List<AggregateCall> = ArrayList()
            for (aggCall in aggregate.getAggCallList()) {
                val newFilterArg: Int
                val newArgList: List<Integer>
                val aggregation: SqlAggFunction
                if (!aggCall.isDistinct()) {
                    aggregation = SqlStdOperatorTable.MIN
                    newArgList = ImmutableIntList.of(x++)
                    newFilterArg = requireNonNull(
                        filters[Pair.of(groupSet, -1)],
                        "filters.get(Pair.of(groupSet, -1))"
                    )
                } else {
                    aggregation = aggCall.getAggregation()
                    newArgList = remap(fullGroupSet, aggCall.getArgList())
                    val newGroupSet: ImmutableBitSet = ImmutableBitSet.of(aggCall.getArgList())
                        .setIf(aggCall.filterArg, aggCall.filterArg >= 0)
                        .union(groupSet)
                    newFilterArg = requireNonNull(
                        filters[Pair.of(newGroupSet, aggCall.filterArg)],
                        "filters.get(of(newGroupSet, aggCall.filterArg))"
                    )
                }
                val newCall: AggregateCall = AggregateCall.create(
                    aggregation, false,
                    aggCall.isApproximate(), aggCall.ignoreNulls(),
                    newArgList, newFilterArg, aggCall.distinctKeys, aggCall.collation,
                    aggregate.getGroupCount(), relBuilder.peek(), null, aggCall.name
                )
                newCalls.add(newCall)
            }
            relBuilder.aggregate(
                relBuilder.groupKey(
                    remap(fullGroupSet, groupSet),
                    remap(fullGroupSet, aggregate.getGroupSets())
                ),
                newCalls
            )
            relBuilder.convert(aggregate.getRowType(), true)
            call.transformTo(relBuilder.build())
        }

        /** Returns the value that "GROUPING(fullGroupSet)" will return for
         * "groupSet".
         *
         *
         * It is important that `fullGroupSet` is not an
         * [ImmutableBitSet]; the order of the bits matters.  */
        fun groupValue(
            fullGroupSet: Collection<Integer?>,
            groupSet: ImmutableBitSet
        ): Long {
            var v: Long = 0
            var x = 1L shl fullGroupSet.size() - 1
            assert(ImmutableBitSet.of(fullGroupSet).contains(groupSet))
            for (i in fullGroupSet) {
                if (!groupSet.get(i)) {
                    v = v or x
                }
                x = x shr 1
            }
            return v
        }

        fun remap(
            groupSet: ImmutableBitSet?,
            bitSet: ImmutableBitSet
        ): ImmutableBitSet {
            val builder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (bit in bitSet) {
                builder.set(remap(groupSet, bit))
            }
            return builder.build()
        }

        fun remap(
            groupSet: ImmutableBitSet?,
            bitSets: Iterable<ImmutableBitSet?>
        ): ImmutableList<ImmutableBitSet> {
            val builder: ImmutableList.Builder<ImmutableBitSet> = ImmutableList.builder()
            for (bitSet in bitSets) {
                builder.add(remap(groupSet, bitSet))
            }
            return builder.build()
        }

        private fun remap(
            groupSet: ImmutableBitSet,
            argList: List<Integer>
        ): List<Integer> {
            var list: ImmutableIntList = ImmutableIntList.of()
            for (arg in argList) {
                list = list.append(remap(groupSet, arg))
            }
            return list
        }

        private fun remap(groupSet: ImmutableBitSet, arg: Int): Int {
            return if (arg < 0) -1 else groupSet.indexOf(arg)
        }

        /**
         * Converts an aggregate relational expression that contains just one
         * distinct aggregate function (or perhaps several over the same arguments)
         * and no non-distinct aggregate functions.
         */
        private fun convertMonopole(
            relBuilder: RelBuilder, aggregate: Aggregate,
            argList: List<Integer>, filterArg: Int
        ): RelBuilder {
            // For example,
            //    SELECT deptno, COUNT(DISTINCT sal), SUM(DISTINCT sal)
            //    FROM emp
            //    GROUP BY deptno
            //
            // becomes
            //
            //    SELECT deptno, COUNT(distinct_sal), SUM(distinct_sal)
            //    FROM (
            //      SELECT DISTINCT deptno, sal AS distinct_sal
            //      FROM EMP GROUP BY deptno)
            //    GROUP BY deptno

            // Project the columns of the GROUP BY plus the arguments
            // to the agg function.
            val sourceOf: Map<Integer, Integer> = HashMap()
            createSelectDistinct(relBuilder, aggregate, argList, filterArg, sourceOf)

            // Create an aggregate on top, with the new aggregate list.
            val newAggCalls: List<AggregateCall> = Lists.newArrayList(aggregate.getAggCallList())
            rewriteAggCalls(newAggCalls, argList, sourceOf)
            val cardinality: Int = aggregate.getGroupSet().cardinality()
            relBuilder.push(
                aggregate.copy(
                    aggregate.getTraitSet(), relBuilder.build(),
                    ImmutableBitSet.range(cardinality), null, newAggCalls
                )
            )
            return relBuilder
        }

        /**
         * Converts all distinct aggregate calls to a given set of arguments.
         *
         *
         * This method is called several times, one for each set of arguments.
         * Each time it is called, it generates a JOIN to a new SELECT DISTINCT
         * relational expression, and modifies the set of top-level calls.
         *
         * @param aggregate Original aggregate
         * @param n         Ordinal of this in a join. `relBuilder` contains the
         * input relational expression (either the original
         * aggregate, the output from the previous call to this
         * method. `n` is 0 if we're converting the
         * first distinct aggregate in a query with no non-distinct
         * aggregates)
         * @param argList   Arguments to the distinct aggregate function
         * @param filterArg Argument that filters input to aggregate function, or -1
         * @param refs      Array of expressions which will be the projected by the
         * result of this rule. Those relating to this arg list will
         * be modified
         */
        private fun doRewrite(
            relBuilder: RelBuilder, aggregate: Aggregate, n: Int,
            argList: List<Integer>, filterArg: Int, refs: List<RexInputRef?>
        ) {
            val rexBuilder: RexBuilder = aggregate.getCluster().getRexBuilder()
            val leftFields: List<RelDataTypeField>?
            leftFields = if (n == 0) {
                null
            } else {
                relBuilder.peek().getRowType().getFieldList()
            }

            // Aggregate(
            //     child,
            //     {COUNT(DISTINCT 1), SUM(DISTINCT 1), SUM(2)})
            //
            // becomes
            //
            // Aggregate(
            //     Join(
            //         child,
            //         Aggregate(child, < all columns > {}),
            //         INNER,
            //         <f2 = f5>))
            //
            // E.g.
            //   SELECT deptno, SUM(DISTINCT sal), COUNT(DISTINCT gender), MAX(age)
            //   FROM Emps
            //   GROUP BY deptno
            //
            // becomes
            //
            //   SELECT e.deptno, adsal.sum_sal, adgender.count_gender, e.max_age
            //   FROM (
            //     SELECT deptno, MAX(age) as max_age
            //     FROM Emps GROUP BY deptno) AS e
            //   JOIN (
            //     SELECT deptno, COUNT(gender) AS count_gender FROM (
            //       SELECT DISTINCT deptno, gender FROM Emps) AS dgender
            //     GROUP BY deptno) AS adgender
            //     ON e.deptno = adgender.deptno
            //   JOIN (
            //     SELECT deptno, SUM(sal) AS sum_sal FROM (
            //       SELECT DISTINCT deptno, sal FROM Emps) AS dsal
            //     GROUP BY deptno) AS adsal
            //   ON e.deptno = adsal.deptno
            //   GROUP BY e.deptno
            //
            // Note that if a query contains no non-distinct aggregates, then the
            // very first join/group by is omitted.  In the example above, if
            // MAX(age) is removed, then the sub-select of "e" is not needed, and
            // instead the two other group by's are joined to one another.

            // Project the columns of the GROUP BY plus the arguments
            // to the agg function.
            val sourceOf: Map<Integer, Integer> = HashMap()
            createSelectDistinct(relBuilder, aggregate, argList, filterArg, sourceOf)

            // Now compute the aggregate functions on top of the distinct dataset.
            // Each distinct agg becomes a non-distinct call to the corresponding
            // field from the right; for example,
            //   "COUNT(DISTINCT e.sal)"
            // becomes
            //   "COUNT(distinct_e.sal)".
            val aggCallList: List<AggregateCall> = ArrayList()
            val aggCalls: List<AggregateCall> = aggregate.getAggCallList()
            val groupCount: Int = aggregate.getGroupCount()
            var i = groupCount - 1
            for (aggCall in aggCalls) {
                ++i

                // Ignore agg calls which are not distinct or have the wrong set
                // arguments. If we're rewriting aggs whose args are {sal}, we will
                // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
                // COUNT(DISTINCT gender) or SUM(sal).
                if (!aggCall.isDistinct()) {
                    continue
                }
                if (!aggCall.getArgList().equals(argList)) {
                    continue
                }

                // Re-map arguments.
                val argCount: Int = aggCall.getArgList().size()
                val newArgs: List<Integer> = ArrayList(argCount)
                for (arg in aggCall.getArgList()) {
                    newArgs.add(requireNonNull(sourceOf[arg]) { "sourceOf.get($arg)" })
                }
                val newFilterArg = if (aggCall.filterArg < 0) -1 else requireNonNull(
                    sourceOf[aggCall.filterArg]
                ) { "sourceOf.get(" + aggCall.filterArg.toString() + ")" }
                val newAggCall: AggregateCall = AggregateCall.create(
                    aggCall.getAggregation(), false,
                    aggCall.isApproximate(), aggCall.ignoreNulls(),
                    newArgs, newFilterArg, aggCall.distinctKeys, aggCall.collation,
                    aggCall.getType(), aggCall.getName()
                )
                assert(refs[i] == null)
                if (leftFields == null) {
                    refs.set(
                        i,
                        RexInputRef(
                            groupCount + aggCallList.size(),
                            newAggCall.getType()
                        )
                    )
                } else {
                    refs.set(
                        i,
                        RexInputRef(
                            leftFields.size() + groupCount
                                    + aggCallList.size(), newAggCall.getType()
                        )
                    )
                }
                aggCallList.add(newAggCall)
            }
            val map: Map<Integer, Integer> = HashMap()
            for (key in aggregate.getGroupSet()) {
                map.put(key, map.size())
            }
            val newGroupSet: ImmutableBitSet = aggregate.getGroupSet().permute(map)
            assert(
                newGroupSet
                    .equals(ImmutableBitSet.range(aggregate.getGroupSet().cardinality()))
            )
            val newGroupingSets: ImmutableList<ImmutableBitSet>? = null
            relBuilder.push(
                aggregate.copy(
                    aggregate.getTraitSet(), relBuilder.build(),
                    newGroupSet, newGroupingSets, aggCallList
                )
            )

            // If there's no left child yet, no need to create the join
            if (leftFields == null) {
                return
            }

            // Create the join condition. It is of the form
            //  'left.f0 = right.f0 and left.f1 = right.f1 and ...'
            // where {f0, f1, ...} are the GROUP BY fields.
            val distinctFields: List<RelDataTypeField> = relBuilder.peek().getRowType().getFieldList()
            val conditions: List<RexNode> = ArrayList()
            i = 0
            while (i < groupCount) {

                // null values form its own group
                // use "is not distinct from" so that the join condition
                // allows null values to match.
                conditions.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                        RexInputRef.of(i, leftFields),
                        RexInputRef(
                            leftFields.size() + i,
                            distinctFields[i].getType()
                        )
                    )
                )
                ++i
            }

            // Join in the new 'select distinct' relation.
            relBuilder.join(JoinRelType.INNER, conditions)
        }

        private fun rewriteAggCalls(
            newAggCalls: List<AggregateCall>,
            argList: List<Integer>,
            sourceOf: Map<Integer, Integer>
        ) {
            // Rewrite the agg calls. Each distinct agg becomes a non-distinct call
            // to the corresponding field from the right; for example,
            // "COUNT(DISTINCT e.sal)" becomes   "COUNT(distinct_e.sal)".
            for (i in 0 until newAggCalls.size()) {
                val aggCall: AggregateCall = newAggCalls[i]

                // Ignore agg calls which are not distinct or have the wrong set
                // arguments. If we're rewriting aggregates whose args are {sal}, we will
                // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
                // COUNT(DISTINCT gender) or SUM(sal).
                if (!aggCall.isDistinct()
                    && aggCall.getAggregation().getDistinctOptionality() !== Optionality.IGNORED
                ) {
                    continue
                }
                if (!aggCall.getArgList().equals(argList)) {
                    continue
                }

                // Re-map arguments.
                val argCount: Int = aggCall.getArgList().size()
                val newArgs: List<Integer> = ArrayList(argCount)
                for (j in 0 until argCount) {
                    val arg: Integer = aggCall.getArgList().get(j)
                    newArgs.add(
                        requireNonNull(
                            sourceOf[arg]
                        ) { "sourceOf.get($arg)" })
                }
                val newAggCall: AggregateCall = AggregateCall.create(
                    aggCall.getAggregation(), false,
                    aggCall.isApproximate(), aggCall.ignoreNulls(), newArgs, -1,
                    aggCall.distinctKeys, aggCall.collation,
                    aggCall.getType(), aggCall.getName()
                )
                newAggCalls.set(i, newAggCall)
            }
        }

        /**
         * Given an [org.apache.calcite.rel.core.Aggregate]
         * and the ordinals of the arguments to a
         * particular call to an aggregate function, creates a 'select distinct'
         * relational expression which projects the group columns and those
         * arguments but nothing else.
         *
         *
         * For example, given
         *
         * <blockquote>
         * <pre>select f0, count(distinct f1), count(distinct f2)
         * from t group by f0</pre>
        </blockquote> *
         *
         *
         * and the argument list
         *
         * <blockquote>{2}</blockquote>
         *
         *
         * returns
         *
         * <blockquote>
         * <pre>select distinct f0, f2 from t</pre>
        </blockquote> *
         *
         *
         * The `sourceOf` map is populated with the source of each
         * column; in this case sourceOf.get(0) = 0, and sourceOf.get(1) = 2.
         *
         * @param relBuilder Relational expression builder
         * @param aggregate Aggregate relational expression
         * @param argList   Ordinals of columns to make distinct
         * @param filterArg Ordinal of column to filter on, or -1
         * @param sourceOf  Out parameter, is populated with a map of where each
         * output field came from
         * @return Aggregate relational expression which projects the required
         * columns
         */
        private fun createSelectDistinct(
            relBuilder: RelBuilder,
            aggregate: Aggregate, argList: List<Integer>, filterArg: Int,
            sourceOf: Map<Integer, Integer?>
        ): RelBuilder {
            relBuilder.push(aggregate.getInput())
            val projects: List<Pair<RexNode, String>> = ArrayList()
            val childFields: List<RelDataTypeField> = relBuilder.peek().getRowType().getFieldList()
            for (i in aggregate.getGroupSet()) {
                sourceOf.put(i, projects.size())
                projects.add(RexInputRef.of2(i, childFields))
            }
            for (arg in argList) {
                if (filterArg >= 0) {
                    // Implement
                    //   agg(DISTINCT arg) FILTER $f
                    // by generating
                    //   SELECT DISTINCT ... CASE WHEN $f THEN arg ELSE NULL END AS arg
                    // and then applying
                    //   agg(arg)
                    // as usual.
                    //
                    // It works except for (rare) agg functions that need to see null
                    // values.
                    val rexBuilder: RexBuilder = aggregate.getCluster().getRexBuilder()
                    val filterRef: RexInputRef = RexInputRef.of(filterArg, childFields)
                    val argRef: Pair<RexNode, String> = RexInputRef.of2(arg, childFields)
                    val condition: RexNode = rexBuilder.makeCall(
                        SqlStdOperatorTable.CASE, filterRef,
                        argRef.left,
                        rexBuilder.makeNullLiteral(argRef.left.getType())
                    )
                    sourceOf.put(arg, projects.size())
                    projects.add(Pair.of(condition, "i$" + argRef.right))
                    continue
                }
                if (sourceOf[arg] != null) {
                    continue
                }
                sourceOf.put(arg, projects.size())
                projects.add(RexInputRef.of2(arg, childFields))
            }
            relBuilder.project(Pair.left(projects), Pair.right(projects))

            // Get the distinct values of the GROUP BY fields and the arguments
            // to the agg functions.
            relBuilder.push(
                aggregate.copy(
                    aggregate.getTraitSet(), relBuilder.build(),
                    ImmutableBitSet.range(projects.size()), null, ImmutableList.of()
                )
            )
            return relBuilder
        }
    }
}
