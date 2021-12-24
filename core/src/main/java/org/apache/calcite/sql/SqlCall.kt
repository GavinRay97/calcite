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

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlVisitor
import org.apache.calcite.sql.validate.SqlMoniker
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorImpl
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Litmus
import org.checkerframework.dataflow.qual.Pure
import java.util.ArrayList
import java.util.Collection
import java.util.List
import java.util.Objects
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * A `SqlCall` is a call to an [operator][SqlOperator].
 * (Operators can be used to describe any syntactic construct, so in practice,
 * every non-leaf node in a SQL parse tree is a `SqlCall` of some
 * kind.)
 */
abstract class SqlCall  //~ Constructors -----------------------------------------------------------
protected constructor(pos: SqlParserPos?) : SqlNode(pos) {
    //~ Methods ----------------------------------------------------------------
    /**
     * Whether this call was created by expanding a parentheses-free call to
     * what was syntactically an identifier.
     */
    val isExpanded: Boolean
        get() = false

    /**
     * Changes the value of an operand. Allows some rewrite by
     * [SqlValidator]; use sparingly.
     *
     * @param i Operand index
     * @param operand Operand value
     */
    fun setOperand(i: Int, @Nullable operand: SqlNode?) {
        throw UnsupportedOperationException()
    }

    @get:Override
    val kind: SqlKind
        get() = operator.getKind()

    @get:Pure
    abstract val operator: SqlOperator

    /**
     * Returns the list of operands. The set and order of operands is call-specific.
     *
     * Note: the proper type would be `List<@Nullable SqlNode>`, however,
     * it would trigger too many changes to the current codebase.
     * @return the list of call operands, never null, the operands can be null
     */
    abstract val operandList: List<Any>

    /**
     * Returns i-th operand (0-based).
     *
     * Note: the result might be null, so the proper signature would be
     * `<S extends @Nullable SqlNode>`, however, it would trigger to many changes to the current
     * codebase.
     * @param i operand index (0-based)
     * @param <S> type of the result
     * @return i-th operand (0-based), the result might be null
    </S> */
    @SuppressWarnings("unchecked")
    fun <S : SqlNode?> operand(i: Int): S {
        // Note: in general, null elements exist in the list, however, the code
        // assumes operand(..) is non-nullable, so we add a cast here
        return castNonNull(operandList[i]) as S
    }

    fun operandCount(): Int {
        return operandList.size()
    }

    @Override
    fun clone(pos: SqlParserPos?): SqlNode {
        return operator.createCall(
            functionQuantifier, pos,
            operandList
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val operator: SqlOperator = operator
        val dialect: SqlDialect = writer.getDialect()
        if (leftPrec > operator.getLeftPrec() || operator.getRightPrec() <= rightPrec && rightPrec != 0
            || writer.isAlwaysUseParentheses() && isA(SqlKind.EXPRESSION)
        ) {
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            dialect.unparseCall(writer, this, 0, 0)
            writer.endList(frame)
        } else {
            dialect.unparseCall(writer, this, leftPrec, rightPrec)
        }
    }

    /**
     * Validates this call.
     *
     *
     * The default implementation delegates the validation to the operator's
     * [SqlOperator.validateCall]. Derived classes may override (as do,
     * for example [SqlSelect] and [SqlUpdate]).
     */
    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateCall(this, scope)
    }

    @Override
    fun findValidOptions(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        pos: SqlParserPos,
        hintList: Collection<SqlMoniker?>?
    ) {
        for (operand in operandList) {
            if (operand is SqlIdentifier) {
                val id: SqlIdentifier = operand as SqlIdentifier
                val idPos: SqlParserPos = id.getParserPosition()
                if (idPos.toString().equals(pos.toString())) {
                    (validator as SqlValidatorImpl).lookupNameCompletionHints(
                        scope, id.names, pos, hintList
                    )
                    return
                }
            }
        }
        // no valid options
    }

    @Override
    fun <R> accept(visitor: SqlVisitor<R>): R {
        return visitor.visit(this)
    }

    @Override
    fun equalsDeep(@Nullable node: SqlNode, litmus: Litmus): Boolean {
        if (node === this) {
            return true
        }
        if (node !is SqlCall) {
            return litmus.fail("{} != {}", this, node)
        }
        val that = node as SqlCall

        // Compare operators by name, not identity, because they may not
        // have been resolved yet. Use case insensitive comparison since
        // this may be a case insensitive system.
        if (!operator.getName().equalsIgnoreCase(that.operator.getName())) {
            return litmus.fail("{} != {}", this, node)
        }
        return if (!equalDeep(functionQuantifier, that.functionQuantifier, litmus)) {
            litmus.fail("{} != {} (function quantifier differs)", this, node)
        } else equalDeep(operandList, that.operandList, litmus)
    }

    /**
     * Returns a string describing the actual argument types of a call, e.g.
     * "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
     */
    fun getCallSignature(
        validator: SqlValidator,
        @Nullable scope: SqlValidatorScope?
    ): String {
        val signatureList: List<String> = ArrayList()
        for (operand in operandList) {
            val argType: RelDataType = validator.deriveType(
                Objects.requireNonNull(scope, "scope"),
                operand
            ) ?: continue
            signatureList.add(argType.toString())
        }
        return SqlUtil.getOperatorSignature(operator, signatureList)
    }

    @Override
    fun getMonotonicity(@Nullable scope: SqlValidatorScope): SqlMonotonicity {
        Objects.requireNonNull(scope, "scope")
        // Delegate to operator.
        val binding = SqlCallBinding(scope.getValidator(), scope, this)
        return operator.getMonotonicity(binding)
    }

    /**
     * Returns whether it is the function `COUNT(*)`.
     *
     * @return true if function call to COUNT(*)
     */
    val isCountStar: Boolean
        get() {
            val sqlOperator: SqlOperator = operator
            if (sqlOperator.getName().equals("COUNT")
                && operandCount() == 1
            ) {
                val parm: SqlNode = operand<SqlNode>(0)
                if (parm is SqlIdentifier) {
                    val id: SqlIdentifier = parm as SqlIdentifier
                    if (id.isStar() && id.names.size() === 1) {
                        return true
                    }
                }
            }
            return false
        }

    @get:Nullable
    @get:Pure
    val functionQuantifier: SqlLiteral?
        get() = null
}
