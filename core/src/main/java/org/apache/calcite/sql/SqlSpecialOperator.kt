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
package org.apache.calcite.sql

import org.apache.calcite.sql.parser.SqlParserPos

/**
 * Generic operator for nodes with special syntax.
 */
class SqlSpecialOperator(
    name: String,
    kind: SqlKind?,
    prec: Int,
    leftAssoc: Boolean,
    @Nullable returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?
) : SqlOperator(
    name,
    kind,
    prec,
    leftAssoc,
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker
) {
    //~ Constructors -----------------------------------------------------------
    constructor(
        name: String,
        kind: SqlKind?
    ) : this(name, kind, 2) {
    }

    constructor(
        name: String,
        kind: SqlKind?,
        prec: Int
    ) : this(name, kind, prec, true, null, null, null) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val syntax: org.apache.calcite.sql.SqlSyntax
        get() = SqlSyntax.SPECIAL

    /**
     * Reduces a list of operators and arguments according to the rules of
     * precedence and associativity. Returns the ordinal of the node which
     * replaced the expression.
     *
     *
     * The default implementation throws
     * [UnsupportedOperationException].
     *
     * @param ordinal indicating the ordinal of the current operator in the list
     * on which a possible reduction can be made
     * @param list    List of alternating
     * [org.apache.calcite.sql.parser.SqlParserUtil.ToTreeListItem] and
     * [SqlNode]
     * @return ordinal of the node which replaced the expression
     */
    fun reduceExpr(
        ordinal: Int,
        list: TokenSequence?
    ): ReduceResult {
        throw Util.needToImplement(this)
    }

    /** List of tokens: the input to a parser. Every token is either an operator
     * ([SqlOperator]) or an expression ([SqlNode]), and every token
     * has a position.  */
    interface TokenSequence {
        fun size(): Int
        fun op(i: Int): SqlOperator?
        fun pos(i: Int): SqlParserPos?
        fun isOp(i: Int): Boolean
        fun node(i: Int): SqlNode?
        fun replaceSublist(start: Int, end: Int, e: SqlNode?)

        /** Creates a parser whose token sequence is a copy of a subset of this
         * token sequence.  */
        fun parser(
            start: Int,
            predicate: Predicate<PrecedenceClimbingParser.Token?>?
        ): PrecedenceClimbingParser?
    }

    /** Result of applying
     * [org.apache.calcite.util.PrecedenceClimbingParser.Special.apply].
     * Tells the caller which range of tokens to replace, and with what.  */
    class ReduceResult(val startOrdinal: Int, val endOrdinal: Int, node: SqlNode) {
        val node: SqlNode

        init {
            this.node = node
        }
    }
}
