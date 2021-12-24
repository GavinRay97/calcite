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
package org.apache.calcite.rel.logical

import org.apache.calcite.linq4j.Ord

/**
 * Sub-class of [org.apache.calcite.rel.core.Window]
 * not targeted at any particular engine or calling convention.
 */
class LogicalWindow
/**
 * Creates a LogicalWindow.
 *
 *
 * Use [.create] unless you know what you're doing.
 *
 * @param cluster Cluster
 * @param traitSet Trait set
 * @param input   Input relational expression
 * @param constants List of constants that are additional inputs
 * @param rowType Output row type
 * @param groups Window groups
 */
    (
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, constants: List<RexLiteral?>?, rowType: RelDataType?,
    groups: List<Group?>?
) : Window(cluster, traitSet, input, constants, rowType, groups) {
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): LogicalWindow {
        return LogicalWindow(
            getCluster(), traitSet, sole(inputs), constants,
            getRowType(), groups
        )
    }

    /** Group specification. All windowed aggregates over the same window
     * (regardless of how it is specified, in terms of a named window or specified
     * attribute by attribute) will end up with the same window key.  */
    private class WindowKey internal constructor(
        groupSet: ImmutableBitSet,
        orderKeys: RelCollation,
        isRows: Boolean,
        lowerBound: RexWindowBound,
        upperBound: RexWindowBound
    ) {
        val groupSet: ImmutableBitSet
        val orderKeys: RelCollation
        val isRows: Boolean
        val lowerBound: RexWindowBound
        val upperBound: RexWindowBound

        init {
            this.groupSet = groupSet
            this.orderKeys = orderKeys
            this.isRows = isRows
            this.lowerBound = lowerBound
            this.upperBound = upperBound
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(groupSet, orderKeys, isRows, lowerBound, upperBound)
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || (obj is WindowKey
                    && groupSet.equals((obj as WindowKey).groupSet)
                    && orderKeys.equals((obj as WindowKey).orderKeys)
                    && Objects.equals(lowerBound, (obj as WindowKey).lowerBound)
                    && Objects.equals(upperBound, (obj as WindowKey).upperBound)
                    && isRows == (obj as WindowKey).isRows))
        }
    }

    companion object {
        /**
         * Creates a LogicalWindow.
         *
         * @param input   Input relational expression
         * @param traitSet Trait set
         * @param constants List of constants that are additional inputs
         * @param rowType Output row type
         * @param groups Window groups
         */
        fun create(
            traitSet: RelTraitSet?, input: RelNode,
            constants: List<RexLiteral?>?, rowType: RelDataType?, groups: List<Group?>?
        ): LogicalWindow {
            return LogicalWindow(
                input.getCluster(), traitSet, input, constants,
                rowType, groups
            )
        }

        /**
         * Creates a LogicalWindow by parsing a [RexProgram].
         */
        fun create(
            cluster: RelOptCluster,
            traitSet: RelTraitSet?, relBuilder: RelBuilder, child: RelNode,
            program: RexProgram
        ): RelNode {
            val outRowType: RelDataType = program.getOutputRowType()
            // Build a list of distinct groups, partitions and aggregate
            // functions.
            val windowMap: Multimap<WindowKey, RexOver> = LinkedListMultimap.create()
            val inputFieldCount: Int = child.getRowType().getFieldCount()
            val constantPool: Map<RexLiteral, RexInputRef> = HashMap()
            val constants: List<RexLiteral> = ArrayList()

            // Identify constants in the expression tree and replace them with
            // references to newly generated constant pool.
            val replaceConstants: RexShuttle = object : RexShuttle() {
                @Override
                fun visitLiteral(literal: RexLiteral): RexNode? {
                    var ref: RexInputRef? = constantPool[literal]
                    if (ref != null) {
                        return ref
                    }
                    constants.add(literal)
                    ref = RexInputRef(
                        constantPool.size() + inputFieldCount,
                        literal.getType()
                    )
                    constantPool.put(literal, ref)
                    return ref
                }
            }

            // Build a list of groups, partitions, and aggregate functions. Each
            // aggregate function will add its arguments as outputs of the input
            // program.
            val origToNewOver: IdentityHashMap<RexOver, RexOver> = IdentityHashMap()
            for (agg in program.getExprList()) {
                if (agg is RexOver) {
                    val origOver: RexOver = agg as RexOver
                    val newOver: RexOver = origOver.accept(replaceConstants) as RexOver
                    origToNewOver.put(origOver, newOver)
                    addWindows(windowMap, newOver, inputFieldCount)
                }
            }
            val aggMap: Map<RexOver, Window.RexWinAggCall> = HashMap()
            val groups: List<Group> = ArrayList()
            for (entry in windowMap.asMap().entrySet()) {
                val windowKey: WindowKey = entry.getKey()
                val aggCalls: List<RexWinAggCall> = ArrayList()
                for (over in entry.getValue()) {
                    val aggCall = RexWinAggCall(
                        over.getAggOperator(),
                        over.getType(),
                        toInputRefs(over.operands),
                        aggMap.size(),
                        over.isDistinct(),
                        over.ignoreNulls()
                    )
                    aggCalls.add(aggCall)
                    aggMap.put(over, aggCall)
                }
                val toInputRefs: RexShuttle = object : RexShuttle() {
                    @Override
                    fun visitLocalRef(localRef: RexLocalRef): RexNode {
                        return RexInputRef(localRef.getIndex(), localRef.getType())
                    }
                }
                groups.add(
                    Group(
                        windowKey.groupSet,
                        windowKey.isRows,
                        windowKey.lowerBound.accept(toInputRefs),
                        windowKey.upperBound.accept(toInputRefs),
                        windowKey.orderKeys,
                        aggCalls
                    )
                )
            }

            // Figure out the type of the inputs to the output program.
            // They are: the inputs to this rel, followed by the outputs of
            // each window.
            val flattenedAggCallList: List<Window.RexWinAggCall?> = ArrayList()
            val fieldList: List<Map.Entry<String, RelDataType>> = ArrayList(child.getRowType().getFieldList())
            val offset: Int = fieldList.size()

            // Use better field names for agg calls that are projected.
            val fieldNames: Map<Integer, String> = HashMap()
            for (ref in Ord.zip(program.getProjectList())) {
                val index: Int = ref.e.getIndex()
                if (index >= offset) {
                    fieldNames.put(
                        index - offset, outRowType.getFieldNames().get(ref.i)
                    )
                }
            }
            for (window in Ord.zip(groups)) {
                for (over in Ord.zip(window.e.aggCalls)) {
                    // Add the k-th over expression of
                    // the i-th window to the output of the program.
                    var name = fieldNames[over.i]
                    if (name == null || name.startsWith("$")) {
                        name = "w" + window.i.toString() + "\$o" + over.i
                    }
                    fieldList.add(Pair.of(name, over.e.getType()))
                    flattenedAggCallList.add(over.e)
                }
            }
            val intermediateRowType: RelDataType = cluster.getTypeFactory().createStructType(fieldList)

            // The output program is the windowed agg's program, combined with
            // the output calc (if it exists).
            val shuttle: RexShuttle = object : RexShuttle() {
                @Override
                fun visitOver(over: RexOver): RexNode {
                    // Look up the aggCall which this expr was translated to.
                    val aggCall: Window.RexWinAggCall? = aggMap[origToNewOver.get(over)]
                    assert(aggCall != null)
                    assert(
                        RelOptUtil.eq(
                            "over",
                            over.getType(),
                            "aggCall",
                            aggCall.getType(),
                            Litmus.THROW
                        )
                    )

                    // Find the index of the aggCall among all partitions of all
                    // groups.
                    val aggCallIndex = flattenedAggCallList.indexOf(aggCall)
                    assert(aggCallIndex >= 0)

                    // Replace expression with a reference to the window slot.
                    val index = inputFieldCount + aggCallIndex
                    assert(
                        RelOptUtil.eq(
                            "over",
                            over.getType(),
                            "intermed",
                            intermediateRowType.getFieldList().get(index).getType(),
                            Litmus.THROW
                        )
                    )
                    return RexInputRef(
                        index,
                        over.getType()
                    )
                }

                @Override
                fun visitLocalRef(localRef: RexLocalRef): RexNode {
                    val index: Int = localRef.getIndex()
                    return if (index < inputFieldCount) {
                        // Reference to input field.
                        localRef
                    } else RexLocalRef(
                        flattenedAggCallList.size() + index,
                        localRef.getType()
                    )
                }
            }
            val window: LogicalWindow = create(
                traitSet, child, constants, intermediateRowType,
                groups
            )

            // The order that the "over" calls occur in the groups and
            // partitions may not match the order in which they occurred in the
            // original expression.
            // Add a project to permute them.
            val refToWindow: List<RexNode> = toInputRefs(shuttle.visitList(program.getExprList()))
            val projectList: List<RexNode> = ArrayList()
            for (inputRef in program.getProjectList()) {
                val index: Int = inputRef.getIndex()
                val ref: RexInputRef = refToWindow[index] as RexInputRef
                projectList.add(ref)
            }
            return relBuilder.push(window)
                .project(projectList, outRowType.getFieldNames())
                .build()
        }

        private fun toInputRefs(
            operands: List<RexNode?>
        ): List<RexNode> {
            return object : AbstractList<RexNode?>() {
                @Override
                fun size(): Int {
                    return operands.size()
                }

                @Override
                operator fun get(index: Int): RexNode? {
                    val operand: RexNode? = operands[index]
                    if (operand is RexInputRef) {
                        return operand
                    }
                    assert(operand is RexLocalRef)
                    val ref: RexLocalRef? = operand as RexLocalRef?
                    return RexInputRef(ref.getIndex(), ref.getType())
                }
            }
        }

        private fun addWindows(
            windowMap: Multimap<WindowKey, RexOver>,
            over: RexOver, inputFieldCount: Int
        ) {
            val aggWindow: RexWindow = over.getWindow()

            // Look up or create a window.
            val orderKeys: RelCollation = getCollation(
                Lists.newArrayList(
                    Util.filter(
                        aggWindow.orderKeys
                    ) { rexFieldCollation ->  // If ORDER BY references constant (i.e. RexInputRef),
                        // then we can ignore such ORDER BY key.
                        rexFieldCollation.left is RexLocalRef
                    })
            )
            var groupSet: ImmutableBitSet = ImmutableBitSet.of(getProjectOrdinals(aggWindow.partitionKeys))
            val groupLength: Int = groupSet.length()
            if (inputFieldCount < groupLength) {
                // If PARTITION BY references constant, we can ignore such partition key.
                // All the inputs after inputFieldCount are literals, thus we can clear.
                groupSet = groupSet.except(ImmutableBitSet.range(inputFieldCount, groupLength))
            }
            val windowKey = WindowKey(
                groupSet, orderKeys, aggWindow.isRows(),
                aggWindow.getLowerBound(), aggWindow.getUpperBound()
            )
            windowMap.put(windowKey, over)
        }
    }
}
