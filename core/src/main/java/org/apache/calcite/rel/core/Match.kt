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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.RelOptCluster

/**
 * Relational expression that represent a MATCH_RECOGNIZE node.
 *
 *
 * Each output row has the columns defined in the measure statements.
 */
abstract class Match protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
    rowType: RelDataType?, pattern: RexNode?,
    strictStart: Boolean, strictEnd: Boolean,
    patternDefinitions: Map<String?, RexNode?>, measures: Map<String?, RexNode?>?,
    after: RexNode?, subsets: Map<String, SortedSet<String?>?>,
    allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
    @Nullable interval: RexNode
) : SingleRel(cluster, traitSet, input) {
    protected val measures: ImmutableMap<String, RexNode>
    protected val pattern: RexNode
    val isStrictStart: Boolean
    val isStrictEnd: Boolean
    val isAllRows: Boolean
    protected val after: RexNode
    protected val patternDefinitions: ImmutableMap<String, RexNode>
    protected val aggregateCalls: Set<RexMRAggCall>
    protected val aggregateCallsPreVar: Map<String, SortedSet<RexMRAggCall>>
    protected val subsets: ImmutableMap<String, SortedSet<String>>
    protected val partitionKeys: ImmutableBitSet
    protected val orderKeys: RelCollation

    @Nullable
    protected val interval: RexNode
    //~ Constructors -----------------------------------------------
    /**
     * Creates a Match.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param rowType Row type
     * @param pattern Regular expression that defines pattern variables
     * @param strictStart Whether it is a strict start pattern
     * @param strictEnd Whether it is a strict end pattern
     * @param patternDefinitions Pattern definitions
     * @param measures Measure definitions
     * @param after After match definitions
     * @param subsets Subsets of pattern variables
     * @param allRows Whether all rows per match (false means one row per match)
     * @param partitionKeys Partition by columns
     * @param orderKeys Order by columns
     * @param interval Interval definition, null if WITHIN clause is not defined
     */
    init {
        rowType = Objects.requireNonNull(rowType, "rowType")
        this.pattern = Objects.requireNonNull(pattern, "pattern")
        Preconditions.checkArgument(patternDefinitions.size() > 0)
        isStrictStart = strictStart
        isStrictEnd = strictEnd
        this.patternDefinitions = ImmutableMap.copyOf(patternDefinitions)
        this.measures = ImmutableMap.copyOf(measures)
        this.after = Objects.requireNonNull(after, "after")
        this.subsets = copyMap<Comparable<K>, Any?>(subsets)
        isAllRows = allRows
        this.partitionKeys = Objects.requireNonNull(partitionKeys, "partitionKeys")
        this.orderKeys = Objects.requireNonNull(orderKeys, "orderKeys")
        this.interval = interval
        val aggregateFinder = AggregateFinder()
        for (rex in this.patternDefinitions.values()) {
            if (rex is RexCall) {
                aggregateFinder.go(rex as RexCall)
            }
        }
        for (rex in this.measures.values()) {
            if (rex is RexCall) {
                aggregateFinder.go(rex as RexCall)
            }
        }
        aggregateCalls = ImmutableSortedSet.copyOf(aggregateFinder.aggregateCalls)
        aggregateCallsPreVar = copyMap<Comparable<K>, Any>(aggregateFinder.aggregateCallsPerVar)
    }

    //~ Methods --------------------------------------------------
    fun getMeasures(): ImmutableMap<String, RexNode> {
        return measures
    }

    fun getAfter(): RexNode {
        return after
    }

    fun getPattern(): RexNode {
        return pattern
    }

    fun getPatternDefinitions(): ImmutableMap<String, RexNode> {
        return patternDefinitions
    }

    fun getSubsets(): ImmutableMap<String, SortedSet<String>> {
        return subsets
    }

    fun getPartitionKeys(): ImmutableBitSet {
        return partitionKeys
    }

    fun getOrderKeys(): RelCollation {
        return orderKeys
    }

    @Nullable
    fun getInterval(): RexNode? {
        return interval
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("partition", getPartitionKeys().asList())
            .item("order", getOrderKeys())
            .item("outputFields", getRowType().getFieldNames())
            .item("allRows", isAllRows)
            .item("after", getAfter())
            .item("pattern", getPattern())
            .item("isStrictStarts", isStrictStart)
            .item("isStrictEnds", isStrictEnd)
            .itemIf("interval", getInterval(), getInterval() != null)
            .item("subsets", getSubsets().values().asList())
            .item("patternDefinitions", getPatternDefinitions().values().asList())
            .item("inputFields", getInput().getRowType().getFieldNames())
    }

    /**
     * Find aggregate functions in operands.
     */
    private class AggregateFinder internal constructor() : RexVisitorImpl<Void?>(true) {
        val aggregateCalls: NavigableSet<RexMRAggCall> = TreeSet()
        val aggregateCallsPerVar: Map<String, NavigableSet<RexMRAggCall>> = TreeMap()
        @Override
        fun visitCall(call: RexCall): Void? {
            var aggFunction: SqlAggFunction? = null
            when (call.getKind()) {
                SUM -> aggFunction = SqlSumAggFunction(call.getType())
                SUM0 -> aggFunction = SqlSumEmptyIsZeroAggFunction()
                MAX, MIN -> aggFunction = SqlMinMaxAggFunction(call.getKind())
                COUNT -> aggFunction = SqlStdOperatorTable.COUNT
                ANY_VALUE -> aggFunction = SqlStdOperatorTable.ANY_VALUE
                BIT_AND, BIT_OR, BIT_XOR -> aggFunction = SqlBitOpAggFunction(call.getKind())
                else -> visitEach(call.operands)
            }
            if (aggFunction != null) {
                val aggCall = RexMRAggCall(
                    aggFunction,
                    call.getType(), call.getOperands(), aggregateCalls.size()
                )
                aggregateCalls.add(aggCall)
                val pv: Set<String> = PatternVarFinder().go(call.getOperands())
                if (pv.size() === 0) {
                    pv.add(STAR)
                }
                for (alpha in pv) {
                    val set: NavigableSet<RexMRAggCall>
                    if (aggregateCallsPerVar.containsKey(alpha)) {
                        set = aggregateCallsPerVar[alpha]
                    } else {
                        set = TreeSet()
                        aggregateCallsPerVar.put(alpha, set)
                    }
                    var update = true
                    for (rex in set) {
                        if (rex.equals(aggCall)) {
                            update = false
                            break
                        }
                    }
                    if (update) {
                        set.add(aggCall)
                    }
                }
            }
            return null
        }

        fun go(call: RexCall) {
            call.accept(this)
        }
    }

    /**
     * Visits the operands of an aggregate call to retrieve relevant pattern
     * variables.
     */
    private class PatternVarFinder internal constructor() : RexVisitorImpl<Void?>(true) {
        val patternVars: Set<String> = HashSet()
        @Override
        fun visitPatternFieldRef(fieldRef: RexPatternFieldRef): Void? {
            patternVars.add(fieldRef.getAlpha())
            return null
        }

        @Override
        fun visitCall(call: RexCall): Void? {
            visitEach(call.operands)
            return null
        }

        fun go(rex: RexNode): Set<String> {
            rex.accept(this)
            return patternVars
        }

        fun go(rexNodeList: List<RexNode?>?): Set<String> {
            visitEach(rexNodeList)
            return patternVars
        }
    }

    /**
     * Aggregate calls in match recognize.
     */
    class RexMRAggCall internal constructor(
        aggFun: SqlAggFunction?,
        type: RelDataType?,
        operands: List<RexNode?>?,
        val ordinal: Int
    ) : RexCall(type, aggFun, operands), Comparable<RexMRAggCall?> {
        init {
            digest = toString() // can compute here because class is final
        }

        @Override
        operator fun compareTo(o: RexMRAggCall): Int {
            return toString().compareTo(o.toString())
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || obj is RexMRAggCall
                    && toString().equals(obj.toString()))
        }

        @Override
        override fun hashCode(): Int {
            return toString().hashCode()
        }
    }

    companion object {
        //~ Instance fields ---------------------------------------------
        private const val STAR = "*"

        /** Creates an immutable map of a map of sorted sets.  */
        private fun <K : Comparable<K>?, V> copyMap(map: Map<K, SortedSet<V>?>): ImmutableSortedMap<K, SortedSet<V>> {
            val b: ImmutableSortedMap.Builder<K, SortedSet<V>> = ImmutableSortedMap.naturalOrder()
            for (e in map.entrySet()) {
                b.put(e.getKey(), ImmutableSortedSet.copyOf(e.getValue()))
            }
            return b.build()
        }
    }
}
