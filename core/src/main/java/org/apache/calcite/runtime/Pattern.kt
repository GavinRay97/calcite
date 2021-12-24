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
package org.apache.calcite.runtime

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import java.util.Objects
import java.util.Stack
import java.util.stream.Collectors

/** Regular expression, to be compiled into an [Automaton].  */
interface Pattern {
    fun toAutomaton(): Automaton? {
        return AutomatonBuilder().add(this).build()
    }

    /** Operator that constructs composite [Pattern] instances.  */
    enum class Op(val minArity: Int, val maxArity: Int) {
        /** A leaf pattern, consisting of a single symbol.  */
        SYMBOL(0, 0),

        /** Anchor for start "^".  */
        ANCHOR_START(0, 0),

        /** Anchor for end "$".  */
        ANCHOR_END(0, 0),

        /** Pattern that matches one pattern followed by another.  */
        SEQ(2, -1),

        /** Pattern that matches one pattern or another.  */
        OR(2, -1),

        /** Pattern that matches a pattern repeated zero or more times.  */
        STAR(1, 1),

        /** Pattern that matches a pattern repeated one or more times.  */
        PLUS(1, 1),

        /** Pattern that matches a pattern repeated between `minRepeat`
         * and `maxRepeat` times.  */
        REPEAT(1, 1),

        /** Pattern that matches a pattern one time or zero times.  */
        OPTIONAL(1, 1);
    }

    /** Builds a pattern expression.  */
    @SuppressWarnings("JdkObsolete")
    class PatternBuilder {
        val stack: Stack<Pattern> = Stack() // TODO: replace with Deque
        private fun push(item: Pattern): PatternBuilder {
            stack.push(item)
            return this
        }

        /** Returns the resulting pattern.  */
        fun build(): Pattern {
            if (stack.size() !== 1) {
                throw AssertionError(
                    "expected stack to have one item, but was "
                            + stack
                )
            }
            return stack.pop()
        }

        /** Returns the resulting automaton.  */
        fun automaton(): Automaton {
            return AutomatonBuilder().add(build()).build()
        }

        /** Creates a pattern that matches symbol,
         * and pushes it onto the stack.
         *
         * @see SymbolPattern
         */
        fun symbol(symbolName: String?): PatternBuilder {
            return push(SymbolPattern(symbolName))
        }

        /** Creates a pattern that matches the two patterns at the top of the
         * stack in sequence,
         * and pushes it onto the stack.  */
        fun seq(): PatternBuilder {
            val pattern1: Pattern = stack.pop()
            val pattern0: Pattern = stack.pop()
            return push(OpPattern(Op.SEQ, pattern0, pattern1))
        }

        /** Creates a pattern that matches the patterns at the top
         * of the stack zero or more times,
         * and pushes it onto the stack.  */
        fun star(): PatternBuilder {
            return push(OpPattern(Op.STAR, stack.pop()))
        }

        /** Creates a pattern that matches the patterns at the top
         * of the stack one or more times,
         * and pushes it onto the stack.  */
        fun plus(): PatternBuilder {
            return push(OpPattern(Op.PLUS, stack.pop()))
        }

        /** Creates a pattern that matches either of the two patterns at the top
         * of the stack,
         * and pushes it onto the stack.  */
        fun or(): PatternBuilder {
            if (stack.size() < 2) {
                throw AssertionError(
                    "Expecting stack to have at least 2 items, but has "
                            + stack.size()
                )
            }
            val pattern1: Pattern = stack.pop()
            val pattern0: Pattern = stack.pop()
            return push(OpPattern(Op.OR, pattern0, pattern1))
        }

        fun repeat(minRepeat: Int, maxRepeat: Int): PatternBuilder {
            val pattern: Pattern = stack.pop()
            return push(RepeatPattern(minRepeat, maxRepeat, pattern))
        }

        fun optional(): PatternBuilder {
            val pattern: Pattern = stack.pop()
            return push(OpPattern(Op.OPTIONAL, pattern))
        }
    }

    /** Base class for implementations of [Pattern].  */
    abstract class AbstractPattern internal constructor(op: Op?) : Pattern {
        val op: Op

        init {
            this.op = Objects.requireNonNull(op, "op")
        }

        @Override
        override fun toAutomaton(): Automaton {
            return AutomatonBuilder().add(this).build()
        }
    }

    /** Pattern that matches a symbol.  */
    class SymbolPattern internal constructor(name: String?) : AbstractPattern(Op.SYMBOL) {
        val name: String

        init {
            this.name = Objects.requireNonNull(name, "name")
        }

        @Override
        override fun toString(): String {
            return name
        }
    }

    /** Pattern with one or more arguments.  */
    class OpPattern internal constructor(op: Op, vararg patterns: Pattern?) : AbstractPattern(op) {
        val patterns: ImmutableList<Pattern>

        init {
            Preconditions.checkArgument(patterns.size >= op.minArity)
            Preconditions.checkArgument(
                op.maxArity == -1
                        || patterns.size <= op.maxArity
            )
            this.patterns = ImmutableList.copyOf(patterns)
        }

        @Override
        override fun toString(): String {
            return when (op) {
                Op.SEQ -> patterns.stream().map(Object::toString)
                    .collect(Collectors.joining(" "))
                Op.STAR -> "(" + patterns.get(0).toString() + ")*"
                Op.PLUS -> "(" + patterns.get(0).toString() + ")+"
                Op.OR -> patterns.get(0) + "|" + patterns.get(1)
                Op.OPTIONAL -> patterns.get(0) + "?"
                else -> throw AssertionError("unknown op $op")
            }
        }
    }

    /** Pattern that matches a pattern repeated between `minRepeat`
     * and `maxRepeat` times.  */
    class RepeatPattern internal constructor(val minRepeat: Int, val maxRepeat: Int, pattern: Pattern?) : OpPattern(
        Op.REPEAT, pattern
    ) {
        @Override
        override fun toString(): String {
            return "(" + patterns.get(0).toString() + "){" + minRepeat
                .toString() + (if (maxRepeat == minRepeat) "" else ", $maxRepeat")
                .toString() + "}"
        }
    }

    companion object {
        /** Creates a builder.  */
        fun builder(): PatternBuilder? {
            return PatternBuilder()
        }
    }
}
