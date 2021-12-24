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
import org.apache.calcite.runtime.CalciteException
import org.apache.calcite.runtime.Resources
import org.apache.calcite.sql.`fun`.SqlLiteralChainOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlOperandMetadata
import org.apache.calcite.sql.type.SqlOperandTypeChecker
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.sql.validate.SelectScope
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlNameMatcher
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorException
import org.apache.calcite.sql.validate.SqlValidatorNamespace
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.ImmutableNullableList
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.Pair
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import java.math.BigDecimal
import java.util.ArrayList
import java.util.List
import org.apache.calcite.util.Static.RESOURCE
import java.util.Objects.requireNonNull

/**
 * `SqlCallBinding` implements [SqlOperatorBinding] by
 * analyzing to the operands of a [SqlCall] with a [SqlValidator].
 */
class SqlCallBinding(
    validator: SqlValidator,
    @Nullable scope: SqlValidatorScope,
    call: SqlCall
) : SqlOperatorBinding(
    validator.getTypeFactory(),
    call.getOperator()
) {
    /** Static nested class required due to
     * [[CALCITE-4393]
 * ExceptionInInitializerError due to NPE in SqlCallBinding caused by circular dependency](https://issues.apache.org/jira/browse/CALCITE-4393).
     * The static field inside it cannot be part of the outer class: it must be defined
     * within a nested class in order to break the cycle during class loading.  */
    private object DefaultCallHolder {
        val DEFAULT_CALL: SqlCall = SqlStdOperatorTable.DEFAULT.createCall(SqlParserPos.ZERO)
    }

    //~ Instance fields --------------------------------------------------------
    private val validator: SqlValidator

    @Nullable
    private val scope: SqlValidatorScope
    private val call: SqlCall
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a call binding.
     *
     * @param validator Validator
     * @param scope     Scope of call
     * @param call      Call node
     */
    init {
        this.validator = validator
        this.scope = scope
        this.call = call
    }// Probably "VALUES expr". Treat same as "SELECT expr GROUP BY ()"

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val groupCount: Int
        get() {
            val selectScope: SelectScope = SqlValidatorUtil.getEnclosingSelectScope(scope)
                ?: // Probably "VALUES expr". Treat same as "SELECT expr GROUP BY ()"
                return 0
            val select: SqlSelect = selectScope.getNode()
            val group: SqlNodeList = select.getGroup()
            if (group != null) {
                var n = 0
                for (groupItem in group) {
                    if (groupItem !is SqlNodeList
                        || (groupItem as SqlNodeList).size() !== 0
                    ) {
                        ++n
                    }
                }
                return n
            }
            return if (validator.isAggregate(select)) 0 else -1
        }

    /**
     * Returns the validator.
     */
    fun getValidator(): SqlValidator {
        return validator
    }

    /**
     * Returns the scope of the call.
     */
    @Nullable
    fun getScope(): SqlValidatorScope {
        return scope
    }

    /**
     * Returns the call node.
     */
    fun getCall(): SqlCall {
        return call
    }

    /** Returns the operands to a call permuted into the same order as the
     * formal parameters of the function.  */
    fun operands(): List<SqlNode> {
        return if (hasAssignment()
            && call.getOperator() !is SqlUnresolvedFunction
        ) {
            permutedOperands(call)
        } else {
            val operandList: List<SqlNode> = call.getOperandList()
            val checker: SqlOperandTypeChecker = call.getOperator().getOperandTypeChecker() ?: return operandList
            val range: SqlOperandCountRange = checker.getOperandCountRange()
            val list: List<SqlNode> = Lists.newArrayList(operandList)
            while (list.size() < range.getMax() && checker.isOptional(list.size())
                && checker.isFixedParameters()
            ) {
                list.add(DefaultCallHolder.DEFAULT_CALL)
            }
            list
        }
    }

    /** Returns whether arguments have name assignment.  */
    private fun hasAssignment(): Boolean {
        for (operand in call.getOperandList()) {
            if (operand != null
                && operand.getKind() === SqlKind.ARGUMENT_ASSIGNMENT
            ) {
                return true
            }
        }
        return false
    }

    /** Returns the operands to a call permuted into the same order as the
     * formal parameters of the function.  */
    private fun permutedOperands(call: SqlCall): List<SqlNode> {
        val operandMetadata: SqlOperandMetadata = requireNonNull(
            call.getOperator().getOperandTypeChecker() as SqlOperandMetadata
        ) { "operandTypeChecker is null for " + call + ", operator " + call.getOperator() }
        val paramNames: List<String> = operandMetadata.paramNames()
        val permuted: List<SqlNode> = ArrayList()
        val nameMatcher: SqlNameMatcher = validator.getCatalogReader().nameMatcher()
        for (paramName in paramNames) {
            var args: Pair<String?, SqlIdentifier?>? = null
            for (j in 0 until call.getOperandList().size()) {
                val call2: SqlCall = call.operand(j)
                assert(call2.getKind() === SqlKind.ARGUMENT_ASSIGNMENT)
                val operandID: SqlIdentifier = call2.operand(1)
                val operandName: String = operandID.getSimple()
                if (nameMatcher.matches(operandName, paramName)) {
                    permuted.add(call2.operand(0))
                    break
                } else if (args == null && nameMatcher.isCaseSensitive()
                    && operandName.equalsIgnoreCase(paramName)
                ) {
                    args = Pair.of(paramName, operandID)
                }
                // the last operand, there is still no match.
                if (j == call.getOperandList().size() - 1) {
                    if (args != null) {
                        throw SqlUtil.newContextException(
                            args.right.getParserPosition(),
                            RESOURCE.paramNotFoundInFunctionDidYouMean(
                                args.right.getSimple(),
                                call.getOperator().getName(), args.left
                            )
                        )
                    }
                    if (operandMetadata.isFixedParameters()) {
                        // Not like user defined functions, we do not patch up the operands
                        // with DEFAULT and then convert to nulls during sql-to-rel conversion.
                        // Thus, there is no need to show the optional operands in the plan and
                        // decide if the optional operand is null when code generation.
                        permuted.add(DefaultCallHolder.DEFAULT_CALL)
                    }
                }
            }
        }
        return permuted
    }

    /**
     * Returns a particular operand.
     */
    fun operand(i: Int): SqlNode {
        return operands()[i]
    }

    /** Returns a call that is equivalent except that arguments have been
     * permuted into the logical order. Any arguments whose default value is being
     * used are null.  */
    fun permutedCall(): SqlCall {
        val operandList: List<SqlNode> = operands()
        return if (operandList.equals(call.getOperandList())) {
            call
        } else call.getOperator().createCall(call.pos, operandList)
    }

    @Override
    fun getOperandMonotonicity(ordinal: Int): SqlMonotonicity {
        return call.getOperandList().get(ordinal).getMonotonicity(scope)
    }

    @SuppressWarnings("deprecation")
    @Override
    @Nullable
    fun getStringLiteralOperand(ordinal: Int): String? {
        val node: SqlNode = call.operand(ordinal)
        val o: Object = SqlLiteral.value(node)
        return if (o is NlsString) (o as NlsString).getValue() else null
    }

    @SuppressWarnings("deprecation")
    @Override
    fun getIntLiteralOperand(ordinal: Int): Int {
        val node: SqlNode = call.operand(ordinal)
        val o: Object = SqlLiteral.value(node)
        if (o is BigDecimal) {
            val bd: BigDecimal = o as BigDecimal
            return try {
                bd.intValueExact()
            } catch (e: ArithmeticException) {
                throw SqlUtil.newContextException(
                    node.pos,
                    RESOURCE.numberLiteralOutOfRange(bd.toString())
                )
            }
        }
        throw AssertionError()
    }

    @Override
    fun <T : Object?> getOperandLiteralValue(
        ordinal: Int,
        clazz: Class<T>
    ): @Nullable T? {
        val node: SqlNode = operand(ordinal)
        return valueAs(node, clazz)
    }

    @Override
    fun isOperandNull(ordinal: Int, allowCast: Boolean): Boolean {
        return SqlUtil.isNullLiteral(operand(ordinal), allowCast)
    }

    @Override
    fun isOperandLiteral(ordinal: Int, allowCast: Boolean): Boolean {
        return SqlUtil.isLiteral(operand(ordinal), allowCast)
    }

    @get:Override
    val operandCount: Int
        get() = call.getOperandList().size()

    @Override
    fun getOperandType(ordinal: Int): RelDataType {
        val operand: SqlNode = call.operand(ordinal)
        val type: RelDataType = SqlTypeUtil.deriveType(this, operand)
        val namespace: SqlValidatorNamespace = validator.getNamespace(operand)
        return if (namespace != null) {
            namespace.getType()
        } else type
    }

    @Override
    @Nullable
    fun getCursorOperand(ordinal: Int): RelDataType? {
        val operand: SqlNode = call.operand(ordinal)
        if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.CURSOR)) {
            return null
        }
        val query: SqlNode = operand.operand(0)
        return SqlTypeUtil.deriveType(this, query)
    }

    @Override
    @Nullable
    fun getColumnListParamInfo(
        ordinal: Int,
        paramName: String?,
        columnList: List<String?>
    ): String? {
        val operand: SqlNode = call.operand(ordinal)
        if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.ROW)) {
            return null
        }
        columnList.addAll(
            SqlIdentifier.simpleNames((operand as SqlCall).getOperandList())
        )
        return validator.getParentCursor(paramName)
    }

    @Override
    fun newError(
        e: Resources.ExInst<SqlValidatorException?>?
    ): CalciteException {
        return validator.newValidationError(call, e)
    }

    /**
     * Constructs a new validation signature error for the call.
     *
     * @return signature exception
     */
    fun newValidationSignatureError(): CalciteException {
        return validator.newValidationError(
            call,
            RESOURCE.canNotApplyOp2Type(
                getOperator().getName(),
                call.getCallSignature(validator, scope),
                getOperator().getAllowedSignatures()
            )
        )
    }

    /**
     * Constructs a new validation error for the call. (Do not use this to
     * construct a validation error for other nodes such as an operands.)
     *
     * @param ex underlying exception
     * @return wrapped exception
     */
    fun newValidationError(
        ex: Resources.ExInst<SqlValidatorException?>?
    ): CalciteException {
        return validator.newValidationError(call, ex)
    }

    /**
     * Returns whether to allow implicit type coercion when validation.
     * This is a short-cut method.
     */
    val isTypeCoercionEnabled: Boolean
        get() = validator.config().typeCoercionEnabled()

    companion object {
        private fun <T : Object?> valueAs(node: SqlNode, clazz: Class<T>): @Nullable T? {
            val literal: SqlLiteral
            return when (node.getKind()) {
                ARRAY_VALUE_CONSTRUCTOR -> {
                    val list: List<Object> = ArrayList()
                    for (o in (node as SqlCall).getOperandList()) {
                        list.add(valueAs<T>(o, Object::class.java))
                    }
                    clazz.cast(ImmutableNullableList.copyOf(list))
                }
                MAP_VALUE_CONSTRUCTOR -> {
                    val builder2: ImmutableMap.Builder<Object, Object> = ImmutableMap.builder()
                    val operands: List<SqlNode> = (node as SqlCall).getOperandList()
                    var i = 0
                    while (i < operands.size()) {
                        val key: SqlNode = operands[i]
                        val value: SqlNode = operands[i + 1]
                        builder2.put(
                            requireNonNull(valueAs<T>(key, Object::class.java), "key"),
                            requireNonNull(valueAs<T>(value, Object::class.java), "value")
                        )
                        i += 2
                    }
                    clazz.cast(builder2.build())
                }
                CAST -> valueAs<T>((node as SqlCall).operand(0), clazz)
                LITERAL -> {
                    literal = node as SqlLiteral
                    if (literal.getTypeName() === SqlTypeName.NULL) {
                        null
                    } else literal.getValueAs(clazz)
                }
                LITERAL_CHAIN -> {
                    literal = SqlLiteralChainOperator.concatenateOperands(node as SqlCall)
                    literal.getValueAs(clazz)
                }
                INTERVAL_QUALIFIER -> {
                    val q: SqlIntervalQualifier = node as SqlIntervalQualifier
                    val intervalValue: SqlIntervalLiteral.IntervalValue = IntervalValue(q, 1, q.toString())
                    literal = SqlLiteral(intervalValue, q.typeName(), q.pos)
                    literal.getValueAs(clazz)
                }
                DEFAULT -> null // currently NULL is the only default value
                else -> {
                    if (SqlUtil.isNullLiteral(node, true)) {
                        null // NULL literal
                    } else null
                    // not a literal
                }
            }
        }
    }
}
