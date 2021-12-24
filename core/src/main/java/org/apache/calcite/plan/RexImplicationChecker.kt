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
package org.apache.calcite.plan

import org.apache.calcite.DataContext

/**
 * Checks whether one condition logically implies another.
 *
 *
 * If A  B, whenever A is true, B will be true also.
 *
 *
 * For example:
 *
 *  * (x &gt; 10)  (x &gt; 5)
 *  * (x = 10)  (x &lt; 30 OR y &gt; 30)
 *  * (x = 10)  (x IS NOT NULL)
 *  * (x &gt; 10 AND y = 20)  (x &gt; 5)
 *
 */
class RexImplicationChecker(
    builder: RexBuilder?,
    executor: RexExecutor?,
    rowType: RelDataType?
) {
    val builder: RexBuilder
    val executor: RexExecutor
    val rowType: RelDataType

    init {
        this.builder = requireNonNull(builder, "builder")
        this.executor = requireNonNull(executor, "executor")
        this.rowType = requireNonNull(rowType, "rowType")
    }

    /**
     * Checks if condition first implies () condition second.
     *
     *
     * This reduces to SAT problem which is NP-Complete.
     * When this method says first implies second then it is definitely true.
     * But it cannot prove that first does not imply second.
     *
     * @param first first condition
     * @param second second condition
     * @return true if it can prove first  second; otherwise false i.e.,
     * it doesn't know if implication holds
     */
    fun implies(first: RexNode, second: RexNode): Boolean {
        // Validation
        if (!validate(first, second)) {
            return false
        }
        LOGGER.debug("Checking if {} => {}", first.toString(), second.toString())

        // Get DNF
        val firstDnf: RexNode = RexUtil.toDnf(builder, first)
        val secondDnf: RexNode = RexUtil.toDnf(builder, second)

        // Check Trivial Cases
        if (firstDnf.isAlwaysFalse()
            || secondDnf.isAlwaysTrue()
        ) {
            return true
        }

        // Decompose DNF into a list of conditions, each of which is a conjunction.
        // For example,
        //   (x > 10 AND y > 30) OR (z > 90)
        // is converted to list of 2 conditions:
        //   (x > 10 AND y > 30)
        //   z > 90
        //
        // Similarly, decompose CNF into a list of conditions, each of which is a
        // disjunction.
        val firsts: List<RexNode> = RelOptUtil.disjunctions(firstDnf)
        val seconds: List<RexNode> = RelOptUtil.disjunctions(secondDnf)
        for (f in firsts) {
            // Check if f implies at least
            // one of the conjunctions in list secondDnfs.
            // If f could not imply even one conjunction in
            // secondDnfs, then final implication may be false.
            if (!impliesAny(f, seconds)) {
                LOGGER.debug("{} does not imply {}", first, second)
                return false
            }
        }
        LOGGER.debug("{} implies {}", first, second)
        return true
    }

    /** Returns whether the predicate `first` implies ()
     * at least one predicate in `seconds`.  */
    private fun impliesAny(first: RexNode, seconds: List<RexNode>): Boolean {
        for (second in seconds) {
            if (impliesConjunction(first, second)) {
                return true
            }
        }
        return false
    }

    /** Returns whether the predicate `first` implies `second` (both
     * may be conjunctions).  */
    private fun impliesConjunction(first: RexNode, second: RexNode): Boolean {
        if (implies2(first, second)) {
            return true
        }
        when (first.getKind()) {
            AND -> for (f in RelOptUtil.conjunctions(first)) {
                if (implies2(f, second)) {
                    return true
                }
            }
            else -> {}
        }
        return false
    }

    /** Returns whether the predicate `first` (not a conjunction)
     * implies `second`.  */
    private fun implies2(first: RexNode, second: RexNode): Boolean {
        if (second.isAlwaysFalse()) { // f cannot imply s
            return false
        }

        // E.g. "x is null" implies "x is null".
        if (first.equals(second)) {
            return true
        }
        when (second.getKind()) {
            IS_NOT_NULL -> {
                // Suppose we know that first is strong in second; that is,
                // the if second is null, then first will be null.
                // Then, first being not null implies that second is not null.
                //
                // For example, first is "x > y", second is "x".
                // If we know that "x > y" is not null, we know that "x" is not null.
                val operand: RexNode = (second as RexCall).getOperands().get(0)
                val strong: Strong = object : Strong() {
                    @Override
                    override fun isNull(node: RexNode): Boolean {
                        return (node.equals(operand)
                                || super.isNull(node))
                    }
                }
                if (strong.isNull(first)) {
                    return true
                }
            }
            else -> {}
        }
        val firstUsageFinder = InputUsageFinder()
        val secondUsageFinder = InputUsageFinder()
        RexUtil.apply(firstUsageFinder, ImmutableList.of(), first)
        RexUtil.apply(secondUsageFinder, ImmutableList.of(), second)

        // Check Support
        if (!checkSupport(firstUsageFinder, secondUsageFinder)) {
            LOGGER.warn("Support for checking {} => {} is not there", first, second)
            return false
        }
        val usagesBuilder: ImmutableList.Builder<Set<Pair<RexInputRef, RexNode>>> = ImmutableList.builder()
        for (entry in firstUsageFinder.usageMap.entrySet()) {
            val usageBuilder: ImmutableSet.Builder<Pair<RexInputRef, RexNode>> = ImmutableSet.builder()
            if (entry.getValue().usageList.size() > 0) {
                for (pair in entry.getValue().usageList) {
                    usageBuilder.add(Pair.of(entry.getKey(), pair.getValue()))
                }
                usagesBuilder.add(usageBuilder.build())
            }
        }
        val usages: Set<List<Pair<RexInputRef, RexNode>>> = Sets.cartesianProduct(usagesBuilder.build())
        for (usageList in usages) {
            // Get the literals from first conjunction and executes second conjunction
            // using them.
            //
            // E.g., for
            //   x > 30 &rArr; x > 10,
            // we will replace x by 30 in second expression and execute it i.e.,
            //   30 > 10
            //
            // If it's true then we infer implication.
            val dataValues: DataContext = VisitorDataContext.of(rowType, usageList)
            if (!isSatisfiable(second, dataValues)) {
                return false
            }
        }
        return true
    }

    private fun isSatisfiable(second: RexNode, @Nullable dataValues: DataContext?): Boolean {
        if (dataValues == null) {
            return false
        }
        val constExps: ImmutableList<RexNode> = ImmutableList.of(second)
        val exec: RexExecutable = RexExecutorImpl.getExecutable(builder, constExps, rowType)
        @Nullable val result: Array<Object>
        exec.setDataContext(dataValues)
        result = try {
            exec.execute()
        } catch (e: Exception) {
            // TODO: CheckSupport should not allow this exception to be thrown
            // Need to monitor it and handle all the cases raising them.
            LOGGER.warn("Exception thrown while checking if => {}: {}", second, e.getMessage())
            return false
        }
        return (result != null && result.size == 1 && result[0] is Boolean
                && result[0])
    }

    /**
     * Visitor that builds a usage map of inputs used by an expression.
     *
     *
     * E.g: for x &gt; 10 AND y &lt; 20 AND x = 40, usage map is as follows:
     *
     *  * key: x value: {(&gt;, 10),(=, 40), usageCount = 2}
     *  * key: y value: {(&gt;, 20), usageCount = 1}
     *
     */
    private class InputUsageFinder internal constructor() : RexVisitorImpl<Void?>(true) {
        val usageMap: Map<RexInputRef, InputRefUsage<SqlOperator, RexNode>> = HashMap()
        @Override
        fun visitInputRef(inputRef: RexInputRef): Void? {
            val inputRefUse: InputRefUsage<SqlOperator, RexNode>? = getUsageMap(inputRef)
            inputRefUse!!.usageCount++
            return null
        }

        @Override
        fun visitCall(call: RexCall): Void {
            when (call.getOperator().getKind()) {
                GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, EQUALS, NOT_EQUALS -> updateBinaryOpUsage(
                    call
                )
                IS_NULL, IS_NOT_NULL -> updateUnaryOpUsage(call)
                else -> {}
            }
            return super.visitCall(call)
        }

        private fun updateUnaryOpUsage(call: RexCall) {
            val operands: List<RexNode> = call.getOperands()
            val first: RexNode = RexUtil.removeCast(operands[0])
            if (first.isA(SqlKind.INPUT_REF)) {
                updateUsage(call.getOperator(), first as RexInputRef, null)
            }
        }

        private fun updateBinaryOpUsage(call: RexCall) {
            val operands: List<RexNode> = call.getOperands()
            val first: RexNode = RexUtil.removeCast(operands[0])
            val second: RexNode = RexUtil.removeCast(operands[1])
            if (first.isA(SqlKind.INPUT_REF)
                && second.isA(SqlKind.LITERAL)
            ) {
                updateUsage(call.getOperator(), first as RexInputRef, second)
            }
            if (first.isA(SqlKind.LITERAL)
                && second.isA(SqlKind.INPUT_REF)
            ) {
                updateUsage(
                    requireNonNull(call.getOperator().reverse()),
                    second as RexInputRef, first
                )
            }
        }

        private fun updateUsage(
            op: SqlOperator, inputRef: RexInputRef,
            @Nullable literal: RexNode?
        ) {
            val inputRefUse: InputRefUsage<SqlOperator, RexNode>? = getUsageMap(inputRef)
            val use: Pair<SqlOperator, RexNode> = Pair.of(op, literal)
            inputRefUse!!.usageList.add(use)
        }

        private fun getUsageMap(rex: RexInputRef): InputRefUsage<SqlOperator, RexNode>? {
            var inputRefUse: InputRefUsage<SqlOperator, RexNode>? = usageMap[rex]
            if (inputRefUse == null) {
                inputRefUse = InputRefUsage<SqlOperator, RexNode>()
                usageMap.put(rex, inputRefUse)
            }
            return inputRefUse
        }
    }

    /**
     * Usage of a [RexInputRef] in an expression.
     *
     * @param <T1> left type
     * @param <T2> right type
    </T2></T1> */
    private class InputRefUsage<T1, T2> {
        val usageList: List<Pair<T1, T2>> = ArrayList()
        val usageCount = 0
    }

    companion object {
        private val LOGGER: CalciteLogger = CalciteLogger(LoggerFactory.getLogger(RexImplicationChecker::class.java))

        /**
         * Looks at the usage of variables in first and second conjunction to decide
         * whether this kind of expression is currently supported for proving first
         * implies second.
         *
         *
         *  1. Variables should be used only once in both the conjunction against
         * given set of operations only: &gt;, &lt;, , , =; .
         *
         *  1. All the variables used in second condition should be used even in the
         * first.
         *
         *  1. If operator used for variable in first is op1 and op2 for second, then
         * we support these combination for conjunction (op1, op2) then op1, op2
         * belongs to one of the following sets:
         *
         *
         *  * (&lt;, ) X (&lt;, ) *note: X represents cartesian product*
         *  * (&gt; / ) X (&gt;, )
         *  * (=) X (&gt;, , &lt;, , =, )
         *  * (, =)
         *
         *
         *  1. We support at most 2 operators to be be used for a variable in first
         * and second usages.
         *
         *
         *
         * @return whether input usage pattern is supported
         */
        private fun checkSupport(
            firstUsageFinder: InputUsageFinder,
            secondUsageFinder: InputUsageFinder
        ): Boolean {
            val firstUsageMap: Map<RexInputRef, InputRefUsage<SqlOperator, RexNode>> = firstUsageFinder.usageMap
            val secondUsageMap: Map<RexInputRef, InputRefUsage<SqlOperator, RexNode>> = secondUsageFinder.usageMap
            for (entry in secondUsageMap.entrySet()) {
                val secondUsage: InputRefUsage<SqlOperator, RexNode> = entry.getValue()
                val secondUsageList: List<Pair<SqlOperator, RexNode>> = secondUsage.usageList
                val secondLen: Int = secondUsageList.size()
                if (secondUsage.usageCount != secondLen || secondLen > 2) {
                    return false
                }
                val firstUsage: InputRefUsage<SqlOperator, RexNode>? = firstUsageMap[entry.getKey()]
                if (firstUsage == null || firstUsage.usageList.size() !== firstUsage.usageCount || firstUsage.usageCount > 2) {
                    return false
                }
                val firstUsageList: List<Pair<SqlOperator, RexNode>> = firstUsage.usageList
                val firstLen: Int = firstUsageList.size()
                val fKind: SqlKind = firstUsageList[0].getKey().getKind()
                val sKind: SqlKind = secondUsageList[0].getKey().getKind()
                val fKind2: SqlKind? = if (firstLen == 2) firstUsageList[1].getKey().getKind() else null
                val sKind2: SqlKind? = if (secondLen == 2) secondUsageList[1].getKey().getKind() else null

                // Note: arguments to isEquivalentOp are never null, however checker-framework's
                // dataflow is not strong enough, so the first parameter is marked as nullable
                if (firstLen == 2 && secondLen == 2 && fKind2 != null && sKind2 != null && !(isEquivalentOp(
                        fKind,
                        sKind
                    ) && isEquivalentOp(fKind2, sKind2))
                    && !(isEquivalentOp(fKind, sKind2) && isEquivalentOp(fKind2, sKind))
                ) {
                    return false
                } else if (firstLen == 1 && secondLen == 1 && fKind !== SqlKind.EQUALS && !isSupportedUnaryOperators(
                        sKind
                    )
                    && !isEquivalentOp(fKind, sKind)
                ) {
                    return false
                } else if (firstLen == 1 && secondLen == 2 && fKind !== SqlKind.EQUALS) {
                    return false
                } else if (firstLen == 2 && secondLen == 1) {
                    // Allow only cases like
                    // x < 30 and x < 40 implies x < 70
                    // x > 30 and x < 40 implies x < 70
                    // But disallow cases like
                    // x > 30 and x > 40 implies x < 70
                    if (fKind2 != null && !isOppositeOp(fKind, fKind2) && !isSupportedUnaryOperators(sKind)
                        && !(isEquivalentOp(fKind, fKind2) && isEquivalentOp(fKind, sKind))
                    ) {
                        return false
                    }
                }
            }
            return true
        }

        private fun isSupportedUnaryOperators(kind: SqlKind): Boolean {
            return when (kind) {
                IS_NOT_NULL, IS_NULL -> true
                else -> false
            }
        }

        private fun isEquivalentOp(@Nullable fKind: SqlKind?, sKind: SqlKind?): Boolean {
            when (sKind) {
                GREATER_THAN, GREATER_THAN_OR_EQUAL -> if (!(fKind === SqlKind.GREATER_THAN)
                    && !(fKind === SqlKind.GREATER_THAN_OR_EQUAL)
                ) {
                    return false
                }
                LESS_THAN, LESS_THAN_OR_EQUAL -> if (!(fKind === SqlKind.LESS_THAN)
                    && !(fKind === SqlKind.LESS_THAN_OR_EQUAL)
                ) {
                    return false
                }
                else -> return false
            }
            return true
        }

        private fun isOppositeOp(fKind: SqlKind, sKind: SqlKind?): Boolean {
            when (sKind) {
                GREATER_THAN, GREATER_THAN_OR_EQUAL -> if (!(fKind === SqlKind.LESS_THAN)
                    && !(fKind === SqlKind.LESS_THAN_OR_EQUAL)
                ) {
                    return false
                }
                LESS_THAN, LESS_THAN_OR_EQUAL -> if (!(fKind === SqlKind.GREATER_THAN)
                    && !(fKind === SqlKind.GREATER_THAN_OR_EQUAL)
                ) {
                    return false
                }
                else -> return false
            }
            return true
        }

        private fun validate(first: RexNode, second: RexNode): Boolean {
            return first is RexCall && second is RexCall
        }
    }
}
