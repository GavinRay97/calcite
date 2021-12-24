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
package org.apache.calcite.util

import org.apache.calcite.linq4j.Ord

/**
 * Parser that takes a collection of tokens (atoms and operators)
 * and groups them together according to the operators' precedence
 * and associativity.
 */
class PrecedenceClimbingParser private constructor(tokens: List<Token>) {
    @Nullable
    private var first: Token? = null

    @Nullable
    private var last: Token?

    init {
        var p: Token? = null
        for (token in tokens) {
            if (p != null) {
                p.next = token
            } else {
                first = token
            }
            token.previous = p
            token.next = null
            p = token
        }
        last = p
    }

    fun atom(o: Object?): Token {
        return Token(Type.ATOM, o, -1, -1)
    }

    fun call(op: Op, args: ImmutableList<Token?>): Call {
        return Call(op, args)
    }

    fun infix(o: Object?, precedence: Int, left: Boolean): Op {
        return Op(
            Type.INFIX, o, precedence * 2 + if (left) 0 else 1,
            precedence * 2 + if (left) 1 else 0
        )
    }

    fun prefix(o: Object?, precedence: Int): Op {
        return Op(Type.PREFIX, o, -1, precedence * 2)
    }

    fun postfix(o: Object?, precedence: Int): Op {
        return Op(Type.POSTFIX, o, precedence * 2, -1)
    }

    fun special(
        o: Object?, leftPrec: Int, rightPrec: Int,
        special: Special
    ): SpecialOp {
        return SpecialOp(o, leftPrec * 2, rightPrec * 2, special)
    }

    @Nullable
    fun parse(): Token? {
        partialParse()
        if (first !== last) {
            throw AssertionError(
                "could not find next operator to reduce: "
                        + this
            )
        }
        return first
    }

    fun partialParse() {
        while (true) {
            val op = highest() ?: return
            val t: Token
            when (op.type) {
                Type.POSTFIX -> {
                    val previous: Token = requireNonNull(op.previous) { "previous of $op" }
                    t = call(op, ImmutableList.of(previous))
                    replace(t, previous.previous, op.next)
                }
                Type.PREFIX -> {
                    val next: Token = requireNonNull(op.next) { "next of $op" }
                    t = call(op, ImmutableList.of(next))
                    replace(t, op.previous, next.next)
                }
                Type.INFIX -> {
                    val previous: Token = requireNonNull(op.previous) { "previous of $op" }
                    val next: Token = requireNonNull(op.next) { "next of $op" }
                    t = call(op, ImmutableList.of(previous, next))
                    replace(t, previous.previous, next.next)
                }
                Type.SPECIAL -> {
                    val r = (op as SpecialOp).special.apply(this, op as SpecialOp)
                    requireNonNull(r, "r")
                    replace(r.replacement, r.first.previous, r.last.next)
                }
                else -> throw AssertionError()
            }
        }
    }

    @Override
    override fun toString(): String {
        return Util.commaList(all())
    }

    /** Returns a list of all tokens.  */
    fun all(): List<Token> {
        return TokenList()
    }

    private fun replace(t: Token, @Nullable previous: Token?, @Nullable next: Token?) {
        t.previous = previous
        t.next = next
        if (previous == null) {
            first = t
        } else {
            previous.next = t
        }
        if (next == null) {
            last = t
        } else {
            next.previous = t
        }
    }

    @Nullable
    private fun highest(): Op? {
        var p = -1
        var highest: Op? = null
        var t = first
        while (t != null) {
            if ((t.left > p || t.right > p)
                && (t.left < 0 || t.left >= prevRight(t.previous))
                && (t.right < 0 || t.right >= nextLeft(t.next))
            ) {
                p = Math.max(t.left, t.right)
                highest = t as Op?
            }
            t = t.next
        }
        return highest
    }

    fun print(token: Token): String {
        return token.toString()
    }

    fun copy(start: Int, predicate: Predicate<Token?>): PrecedenceClimbingParser {
        val tokens: List<Token> = ArrayList()
        for (token in Util.skip(all(), start)) {
            if (predicate.test(token)) {
                break
            }
            tokens.add(token.copy())
        }
        return PrecedenceClimbingParser(tokens)
    }

    /** Token type.  */
    enum class Type {
        ATOM, CALL, PREFIX, INFIX, POSTFIX, SPECIAL
    }

    /** A token: either an atom, a call to an operator with arguments,
     * or an unmatched operator.  */
    class Token internal constructor(val type: Type, @Nullable o: Object?, left: Int, right: Int) {
        @Nullable
        var previous: Token? = null

        @Nullable
        var next: Token? = null

        @Nullable
        val o: Object?
        val left: Int
        val right: Int

        init {
            this.o = o
            this.left = left
            this.right = right
        }

        /**
         * Returns `o`.
         * @return o
         */
        @Nullable
        fun o(): Object? {
            return o
        }

        @Override
        override fun toString(): String {
            return String.valueOf(o)
        }

        protected fun print(b: StringBuilder): StringBuilder {
            return b.append(o)
        }

        fun copy(): Token {
            return Token(type, o, left, right)
        }
    }

    /** An operator token.  */
    class Op internal constructor(type: Type, o: Object?, left: Int, right: Int) : Token(type, o, left, right) {
        @Override
        override fun o(): Object {
            return castNonNull(super.o())
        }

        @Override
        override fun copy(): Token {
            return Op(type, o(), left, right)
        }
    }

    /** An token corresponding to a special operator.  */
    class SpecialOp internal constructor(o: Object?, left: Int, right: Int, val special: Special) :
        Op(Type.SPECIAL, o, left, right) {
        @Override
        override fun copy(): Token {
            return SpecialOp(o(), left, right, special)
        }
    }

    /** A token that is a call to an operator with arguments.  */
    class Call internal constructor(val op: Op, args: ImmutableList<Token?>) : Token(Type.CALL, null, -1, -1) {
        val args: ImmutableList<Token?>

        init {
            this.args = args
        }

        @Override
        override fun copy(): Token {
            return Call(op, args)
        }

        @Override
        override fun toString(): String {
            return print(StringBuilder()).toString()
        }

        @Override
        override fun print(b: StringBuilder): StringBuilder {
            return when (op.type) {
                Type.PREFIX -> {
                    b.append('(')
                    printOp(b, false, true)
                    args.get(0).print(b)
                    b.append(')')
                }
                Type.POSTFIX -> {
                    b.append('(')
                    args.get(0).print(b)
                    printOp(b, true, false).append(')')
                }
                Type.INFIX -> {
                    b.append('(')
                    args.get(0).print(b)
                    printOp(b, true, true)
                    args.get(1).print(b)
                    b.append(')')
                }
                Type.SPECIAL -> {
                    printOp(b, false, false)
                        .append('(')
                    for (arg in Ord.zip(args)) {
                        if (arg.i > 0) {
                            b.append(", ")
                        }
                        arg.e.print(b)
                    }
                    b.append(')')
                }
                else -> throw AssertionError()
            }
        }

        private fun printOp(
            b: StringBuilder, leftSpace: Boolean,
            rightSpace: Boolean
        ): StringBuilder {
            val s: String = String.valueOf(op.o)
            if (leftSpace) {
                b.append(' ')
            }
            b.append(s)
            if (rightSpace) {
                b.append(' ')
            }
            return b
        }
    }

    /** Callback defining the behavior of a special function.  */
    interface Special {
        /** Given an occurrence of this operator, identifies the range of tokens to
         * be collapsed into a call of this operator, and the arguments to that
         * call.  */
        fun apply(parser: PrecedenceClimbingParser?, op: SpecialOp?): Result
    }

    /** Result of a call to [Special.apply].  */
    class Result(val first: Token, val last: Token, val replacement: Token)

    /** Fluent helper to build a parser containing a list of tokens.  */
    class Builder {
        val tokens: List<Token> = ArrayList()
        private val dummy = PrecedenceClimbingParser(ImmutableList.of())
        private fun add(t: Token): Builder {
            tokens.add(t)
            return this
        }

        fun atom(o: Object?): Builder {
            return add(dummy.atom(o))
        }

        fun call(op: Op, arg0: Token?, arg1: Token?): Builder {
            return add(dummy.call(op, ImmutableList.of(arg0, arg1)))
        }

        fun infix(o: Object?, precedence: Int, left: Boolean): Builder {
            return add(dummy.infix(o, precedence, left))
        }

        fun prefix(o: Object?, precedence: Int): Builder {
            return add(dummy.prefix(o, precedence))
        }

        fun postfix(o: Object?, precedence: Int): Builder {
            return add(dummy.postfix(o, precedence))
        }

        fun special(
            o: Object?, leftPrec: Int, rightPrec: Int,
            special: Special
        ): Builder {
            return add(dummy.special(o, leftPrec, rightPrec, special))
        }

        fun build(): PrecedenceClimbingParser {
            return PrecedenceClimbingParser(tokens)
        }
    }

    /** List view onto the tokens in a parser. The view is semi-mutable; it
     * supports [List.remove] but not [List.set] or
     * [List.add].  */
    private inner class TokenList : AbstractList<Token?>() {
        @Override
        operator fun get(index: Int): Token {
            var index = index
            var t = first
            while (t != null) {
                if (index-- == 0) {
                    return t
                }
                t = t.next
            }
            throw IndexOutOfBoundsException()
        }

        @Override
        fun size(): Int {
            var n = 0
            var t = first
            while (t != null) {
                ++n
                t = t.next
            }
            return n
        }

        @Override
        fun remove(index: Int): Token {
            val t = get(index)
            if (t.previous == null) {
                first = t.next
            } else {
                t.previous!!.next = t.next
            }
            if (t.next == null) {
                last = t.previous
            } else {
                t.next!!.previous = t.previous
            }
            return t
        }

        @Override
        operator fun set(index: Int, element: Token): Token {
            val t = get(index)
            element.previous = t.previous
            if (t.previous == null) {
                first = element
            } else {
                t.previous!!.next = element
            }
            element.next = t.next
            if (t.next == null) {
                last = element
            } else {
                t.next!!.previous = element
            }
            return t
        }
    }

    companion object {
        /** Returns the right precedence of the preceding operator token.  */
        private fun prevRight(@Nullable token: Token?): Int {
            var token = token
            while (token != null) {
                if (token.type == Type.POSTFIX) {
                    return Integer.MAX_VALUE
                }
                if (token.right >= 0) {
                    return token.right
                }
                token = token.previous
            }
            return -1
        }

        /** Returns the left precedence of the following operator token.  */
        private fun nextLeft(@Nullable token: Token?): Int {
            var token = token
            while (token != null) {
                if (token.type == Type.PREFIX) {
                    return Integer.MAX_VALUE
                }
                if (token.left >= 0) {
                    return token.left
                }
                token = token.next
            }
            return -1
        }
    }
}
