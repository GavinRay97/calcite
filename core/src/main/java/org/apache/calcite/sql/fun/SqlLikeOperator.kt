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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.sql.SqlCall

/**
 * An operator describing the `LIKE` and `SIMILAR`
 * operators.
 *
 *
 * Syntax of the two operators:
 *
 *
 *  * `src-value [NOT] LIKE pattern-value [ESCAPE
 * escape-value]`
 *  * `src-value [NOT] SIMILAR pattern-value [ESCAPE
 * escape-value]`
 *
 *
 *
 * **NOTE** If the `NOT` clause is present the
 * [parser][org.apache.calcite.sql.parser.SqlParser] will generate a
 * equivalent to `NOT (src LIKE pattern ...)`
 */
class SqlLikeOperator internal constructor(
    name: String?,
    kind: SqlKind,
    negated: Boolean,
    caseSensitive: Boolean
) : SqlSpecialOperator(
    name,
    kind,
    32,
    false,
    ReturnTypes.BOOLEAN_NULLABLE,
    InferTypes.FIRST_KNOWN,
    OperandTypes.STRING_SAME_SAME_SAME
) {
    /**
     * Returns whether this is the 'NOT LIKE' operator.
     *
     * @return whether this is 'NOT LIKE'
     *
     * @see .not
     */
    //~ Instance fields --------------------------------------------------------
    val isNegated: Boolean

    /**
     * Returns whether this operator matches the case of its operands.
     * For example, returns true for `LIKE` and false for `ILIKE`.
     *
     * @return whether this operator matches the case of its operands
     */
    val isCaseSensitive: Boolean
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlLikeOperator.
     *
     * @param name        Operator name
     * @param kind        Kind
     * @param negated     Whether this is 'NOT LIKE'
     * @param caseSensitive Whether this operator ignores the case of its operands
     */
    init {
        // LIKE is right-associative, because that makes it easier to capture
        // dangling ESCAPE clauses: "a like b like c escape d" becomes
        // "a like (b like c escape d)".
        if (!caseSensitive && kind !== SqlKind.LIKE) {
            throw IllegalArgumentException(
                ("Only (possibly negated) "
                        + SqlKind.LIKE) + " can be made case-insensitive, not " + kind
            )
        }
        isNegated = negated
        isCaseSensitive = caseSensitive
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    operator fun not(): SqlOperator {
        return of(kind, !isNegated, isCaseSensitive)
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.between(2, 3)

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        when (callBinding.getOperandCount()) {
            2 -> if (!OperandTypes.STRING_SAME_SAME.checkOperandTypes(
                    callBinding,
                    throwOnFailure
                )
            ) {
                return false
            }
            3 -> if (!OperandTypes.STRING_SAME_SAME_SAME.checkOperandTypes(
                    callBinding,
                    throwOnFailure
                )
            ) {
                return false
            }
            else -> throw AssertionError(
                ("unexpected number of args to "
                        + callBinding.getCall()) + ": " + callBinding.getOperandCount()
            )
        }
        return SqlTypeUtil.isCharTypeComparable(
            callBinding,
            callBinding.operands(),
            throwOnFailure
        )
    }

    @Override
    fun validateCall(
        call: SqlCall?, validator: SqlValidator?,
        scope: SqlValidatorScope?, operandScope: SqlValidatorScope?
    ) {
        super.validateCall(call, validator, scope, operandScope)
    }

    @Override
    fun validRexOperands(count: Int, litmus: Litmus): Boolean {
        if (isNegated) {
            litmus.fail("unsupported negated operator {}", this)
        }
        return super.validRexOperands(count, litmus)
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startList("", "")
        call.operand(0).unparse(writer, getLeftPrec(), getRightPrec())
        writer.sep(getName())
        call.operand(1).unparse(writer, getLeftPrec(), getRightPrec())
        if (call.operandCount() === 3) {
            writer.sep("ESCAPE")
            call.operand(2).unparse(writer, getLeftPrec(), getRightPrec())
        }
        writer.endList(frame)
    }

    @Override
    fun reduceExpr(
        opOrdinal: Int,
        list: TokenSequence
    ): ReduceResult {
        // Example:
        //   a LIKE b || c ESCAPE d || e AND f
        // |  |    |      |      |      |
        //  exp0    exp1          exp2
        val exp0: SqlNode = list.node(opOrdinal - 1)
        val op: SqlOperator = list.op(opOrdinal)
        assert(op is SqlLikeOperator)
        val exp1: SqlNode = SqlParserUtil.toTreeEx(
            list,
            opOrdinal + 1,
            getRightPrec(),
            SqlKind.ESCAPE
        )
        var exp2: SqlNode? = null
        if (opOrdinal + 2 < list.size()) {
            if (list.isOp(opOrdinal + 2)) {
                val op2: SqlOperator = list.op(opOrdinal + 2)
                if (op2.getKind() === SqlKind.ESCAPE) {
                    exp2 = SqlParserUtil.toTreeEx(
                        list,
                        opOrdinal + 3,
                        getRightPrec(),
                        SqlKind.ESCAPE
                    )
                }
            }
        }
        val operands: Array<SqlNode>
        val end: Int
        if (exp2 != null) {
            operands = arrayOf<SqlNode>(exp0, exp1, exp2)
            end = opOrdinal + 4
        } else {
            operands = arrayOf<SqlNode>(exp0, exp1)
            end = opOrdinal + 2
        }
        val call: SqlCall = createCall(SqlParserPos.sum(operands), operands)
        return ReduceResult(opOrdinal - 1, end, call)
    }

    companion object {
        private fun of(
            kind: SqlKind, negated: Boolean,
            caseSensitive: Boolean
        ): SqlOperator {
            return when (kind) {
                SIMILAR -> if (negated) SqlStdOperatorTable.NOT_SIMILAR_TO else SqlStdOperatorTable.SIMILAR_TO
                RLIKE -> if (negated) SqlLibraryOperators.NOT_RLIKE else SqlLibraryOperators.RLIKE
                LIKE -> if (caseSensitive) {
                    if (negated) SqlStdOperatorTable.NOT_LIKE else SqlStdOperatorTable.LIKE
                } else {
                    if (negated) SqlLibraryOperators.NOT_ILIKE else SqlLibraryOperators.ILIKE
                }
                else -> throw AssertionError("unexpected $kind")
            }
        }
    }
}
