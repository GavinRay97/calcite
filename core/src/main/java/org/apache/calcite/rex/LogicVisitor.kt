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
package org.apache.calcite.rex

import org.apache.calcite.plan.RelOptUtil.Logic
import com.google.common.collect.Iterables
import java.util.Collection
import java.util.Collections
import java.util.EnumSet
import java.util.List
import java.util.Set
import java.util.Objects.requireNonNull

/**
 * Visitor pattern for traversing a tree of [RexNode] objects.
 */
class LogicVisitor private constructor(seek: RexNode, logicCollection: Collection<Logic>) :
    RexUnaryBiVisitor<Logic?>(true) {
    private val seek: RexNode
    private val logicCollection: Collection<Logic>

    /** Creates a LogicVisitor.  */
    init {
        this.seek = seek
        this.logicCollection = logicCollection
    }

    @Override
    @Nullable
    fun visitCall(call: RexCall, @Nullable logic: Logic?): Logic? {
        var logic: Logic? = logic
        val arg0: Logic? = logic
        when (call.getKind()) {
            IS_NOT_NULL, IS_NULL -> logic = Logic.TRUE_FALSE_UNKNOWN
            IS_TRUE, IS_NOT_TRUE -> logic = Logic.UNKNOWN_AS_FALSE
            IS_FALSE, IS_NOT_FALSE -> logic = Logic.UNKNOWN_AS_TRUE
            NOT -> logic = requireNonNull(logic, "logic").negate2()
            CASE -> logic = Logic.TRUE_FALSE_UNKNOWN
            else -> {}
        }
        when (requireNonNull(logic, "logic")) {
            TRUE -> when (call.getKind()) {
                AND -> {}
                else -> logic = Logic.TRUE_FALSE_UNKNOWN
            }
            else -> {}
        }
        for (operand in call.operands) {
            operand.accept(this, logic)
        }
        return end(call, arg0)
    }

    @Override
    @Nullable
    protected fun end(node: RexNode, @Nullable arg: Logic?): Logic? {
        if (node.equals(seek)) {
            logicCollection.add(requireNonNull(arg, "arg"))
        }
        return arg
    }

    @Override
    @Nullable
    fun visitOver(over: RexOver, @Nullable arg: Logic?): Logic? {
        return end(over, arg)
    }

    @Override
    @Nullable
    fun visitFieldAccess(
        fieldAccess: RexFieldAccess,
        @Nullable arg: Logic?
    ): Logic? {
        return end(fieldAccess, arg)
    }

    @Override
    @Nullable
    fun visitSubQuery(subQuery: RexSubQuery, @Nullable arg: Logic): Logic? {
        var arg: Logic = arg
        if (!subQuery.getType().isNullable()) {
            if (arg === Logic.TRUE_FALSE_UNKNOWN) {
                arg = Logic.TRUE_FALSE
            }
        }
        return end(subQuery, arg)
    }

    companion object {
        /** Finds a suitable logic for evaluating `seek` within a list of
         * expressions.
         *
         *
         * Chooses a logic that is safe (that is, gives the right
         * answer) with the fewest possibilities (that is, we prefer one that
         * returns [true as true, false as false, unknown as false] over one that
         * distinguishes false from unknown).
         */
        fun find(
            logic: Logic?, nodes: List<RexNode?>,
            seek: RexNode
        ): Logic {
            val set: Set<Logic> = EnumSet.noneOf(Logic::class.java)
            val visitor = LogicVisitor(seek, set)
            for (node in nodes) {
                node.accept(visitor, logic)
            }
            // Convert FALSE (which can only exist within LogicVisitor) to
            // UNKNOWN_AS_TRUE.
            if (set.remove(Logic.FALSE)) {
                set.add(Logic.UNKNOWN_AS_TRUE)
            }
            return when (set.size()) {
                0 -> throw IllegalArgumentException("not found: $seek")
                1 -> Iterables.getOnlyElement(set)
                else -> Logic.TRUE_FALSE_UNKNOWN
            }
        }

        fun collect(
            node: RexNode, seek: RexNode, logic: Logic?,
            logicList: List<Logic>
        ) {
            node.accept(LogicVisitor(seek, logicList), logic)
            // Convert FALSE (which can only exist within LogicVisitor) to
            // UNKNOWN_AS_TRUE.
            Collections.replaceAll(logicList, Logic.FALSE, Logic.UNKNOWN_AS_TRUE)
        }
    }
}
