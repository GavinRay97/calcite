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

import org.apache.calcite.linq4j.Ord

/**
 * A `SqlOperator` is a type of node in a SQL parse tree (it is NOT a
 * node in a SQL parse tree). It includes functions, operators such as '=', and
 * syntactic constructs such as 'case' statements. Operators may represent
 * query-level expressions (e.g. [SqlSelectOperator] or row-level
 * expressions (e.g. [org.apache.calcite.sql.fun.SqlBetweenOperator].
 *
 *
 * Operators have *formal operands*, meaning ordered (and optionally
 * named) placeholders for the values they operate on. For example, the division
 * operator takes two operands; the first is the numerator and the second is the
 * denominator. In the context of subclass [SqlFunction], formal operands
 * are referred to as *parameters*.
 *
 *
 * When an operator is instantiated via a [SqlCall], it is supplied
 * with *actual operands*. For example, in the expression `3 /
 * 5`, the literal expression `3` is the actual operand
 * corresponding to the numerator, and `5` is the actual operand
 * corresponding to the denominator. In the context of SqlFunction, actual
 * operands are referred to as *arguments*
 *
 *
 * In many cases, the formal/actual distinction is clear from context, in
 * which case we drop these qualifiers.
 */
abstract class SqlOperator protected constructor(
    name: String,
    kind: SqlKind?,
    leftPrecedence: Int,
    rightPrecedence: Int,
    @Nullable returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?
) {
    //~ Instance fields --------------------------------------------------------
    /**
     * The name of the operator/function. Ex. "OVERLAY" or "TRIM"
     */
    val name: String

    /**
     * See [SqlKind]. It's possible to have a name that doesn't match the
     * kind
     */
    val kind: SqlKind?

    /**
     * The precedence with which this operator binds to the expression to the
     * left. This is less than the right precedence if the operator is
     * left-associative.
     */
    val leftPrec: Int

    /**
     * The precedence with which this operator binds to the expression to the
     * right. This is more than the left precedence if the operator is
     * left-associative.
     */
    val rightPrec: Int

    /** Used to infer the return type of a call to this operator.  */
    @Nullable
    private val returnTypeInference: SqlReturnTypeInference?

    /** Used to infer types of unknown operands.  */
    @Nullable
    private val operandTypeInference: SqlOperandTypeInference?

    /** Used to validate operand types.  */
    @Nullable
    private val operandTypeChecker: SqlOperandTypeChecker?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an operator.
     */
    init {
        var operandTypeInference: SqlOperandTypeInference? = operandTypeInference
        assert(kind != null)
        this.name = name
        this.kind = kind
        leftPrec = leftPrecedence
        rightPrec = rightPrecedence
        this.returnTypeInference = returnTypeInference
        if (operandTypeInference == null
            && operandTypeChecker != null
        ) {
            operandTypeInference = operandTypeChecker.typeInference()
        }
        this.operandTypeInference = operandTypeInference
        this.operandTypeChecker = operandTypeChecker
    }

    /**
     * Creates an operator specifying left/right associativity.
     */
    protected constructor(
        name: String,
        kind: SqlKind?,
        prec: Int,
        leftAssoc: Boolean,
        @Nullable returnTypeInference: SqlReturnTypeInference?,
        @Nullable operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?
    ) : this(
        name,
        kind,
        leftPrec(prec, leftAssoc),
        rightPrec(prec, leftAssoc),
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker
    ) {
    }

    @Nullable
    fun getOperandTypeChecker(): SqlOperandTypeChecker? {
        return operandTypeChecker
    }

    /**
     * Returns a constraint on the number of operands expected by this operator.
     * Subclasses may override this method; when they don't, the range is
     * derived from the [SqlOperandTypeChecker] associated with this
     * operator.
     *
     * @return acceptable range
     */
    val operandCountRange: org.apache.calcite.sql.SqlOperandCountRange
        get() {
            if (operandTypeChecker != null) {
                return operandTypeChecker.getOperandCountRange()
            }
            throw Util.needToImplement(this)
        }

    /**
     * Returns the fully-qualified name of this operator.
     */
    val nameAsId: SqlIdentifier
        get() = SqlIdentifier(name, SqlParserPos.ZERO)

    @Pure
    fun getKind(): SqlKind? {
        return kind
    }

    @Override
    override fun toString(): String {
        return name
    }

    /**
     * Returns the syntactic type of this operator, never null.
     */
    abstract val syntax: org.apache.calcite.sql.SqlSyntax

    /**
     * Creates a call to this operator with a list of operands.
     *
     *
     * The position of the resulting call is the union of the `pos`
     * and the positions of all of the operands.
     *
     * @param functionQualifier Function qualifier (e.g. "DISTINCT"), or null
     * @param pos               Parser position of the identifier of the call
     * @param operands          List of operands
     */
    fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?,
        operands: Iterable<SqlNode?>?
    ): SqlCall {
        return createCall(
            functionQualifier, pos,
            Iterables.toArray(operands, SqlNode::class.java)
        )
    }

    /**
     * Creates a call to this operator with an array of operands.
     *
     *
     * The position of the resulting call is the union of the `pos`
     * and the positions of all of the operands.
     *
     * @param functionQualifier Function qualifier (e.g. "DISTINCT"), or null
     * @param pos               Parser position of the identifier of the call
     * @param operands          Array of operands
     */
    fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos,
        @Nullable vararg operands: SqlNode?
    ): SqlCall {
        var pos: SqlParserPos = pos
        pos = pos.plusAll(operands)
        return SqlBasicCall(
            this, ImmutableNullableList.copyOf(operands), pos,
            functionQualifier
        )
    }

    /**
     * Creates a call to this operator with an array of operands.
     *
     *
     * The position of the resulting call is the union of the `
     * pos` and the positions of all of the operands.
     *
     * @param pos      Parser position
     * @param operands List of arguments
     * @return call to this operator
     */
    fun createCall(
        pos: SqlParserPos?,
        @Nullable vararg operands: SqlNode?
    ): SqlCall {
        return createCall(null, pos, *operands)
    }

    /**
     * Creates a call to this operator with a list of operands contained in a
     * [SqlNodeList].
     *
     *
     * The position of the resulting call is inferred from the SqlNodeList.
     *
     * @param nodeList List of arguments
     * @return call to this operator
     */
    fun createCall(
        nodeList: SqlNodeList
    ): SqlCall {
        return createCall(
            null,
            nodeList.getParserPosition(),
            nodeList.toArray(arrayOfNulls<SqlNode>(0))
        )
    }

    /**
     * Creates a call to this operator with a list of operands.
     *
     *
     * The position of the resulting call is the union of the `pos`
     * and the positions of all of the operands.
     */
    fun createCall(
        pos: SqlParserPos?,
        operandList: List<SqlNode?>
    ): SqlCall {
        return createCall(
            null,
            pos,
            operandList.toArray(arrayOfNulls<SqlNode>(0))
        )
    }

    /** Not supported. Choose between
     * [.createCall] and
     * [.createCall]. The ambiguity arises because
     * [SqlNodeList] extends [SqlNode]
     * and also implements `List<SqlNode>`.  */
    @Deprecated
    fun createCall(
        pos: SqlParserPos?,
        operands: SqlNodeList?
    ): SqlCall {
        throw UnsupportedOperationException()
    }

    /**
     * Rewrites a call to this operator. Some operators are implemented as
     * trivial rewrites (e.g. NULLIF becomes CASE). However, we don't do this at
     * createCall time because we want to preserve the original SQL syntax as
     * much as possible; instead, we do this before the call is validated (so
     * the trivial operator doesn't need its own implementation of type
     * derivation methods). The default implementation is to just return the
     * original call without any rewrite.
     *
     * @param validator Validator
     * @param call      Call to be rewritten
     * @return rewritten call
     */
    fun rewriteCall(validator: SqlValidator?, call: SqlCall): SqlNode {
        return call
    }

    /**
     * Writes a SQL representation of a call to this operator to a writer,
     * including parentheses if the operators on either side are of greater
     * precedence.
     *
     *
     * The default implementation of this method delegates to
     * [SqlSyntax.unparse].
     */
    fun unparse(
        writer: SqlWriter?,
        call: SqlCall?,
        leftPrec: Int,
        rightPrec: Int
    ) {
        syntax.unparse(writer, this, call, leftPrec, rightPrec)
    }

    @Deprecated // to be removed before 2.0
    protected fun unparseListClause(writer: SqlWriter, clause: SqlNode?) {
        val nodeList: SqlNodeList = if (clause is SqlNodeList) clause as SqlNodeList? else SqlNodeList.of(clause)
        writer.list(SqlWriter.FrameTypeEnum.SIMPLE, SqlWriter.COMMA, nodeList)
    }

    @Deprecated // to be removed before 2.0
    protected fun unparseListClause(
        writer: SqlWriter,
        clause: SqlNode?,
        @Nullable sepKind: SqlKind?
    ) {
        val nodeList: SqlNodeList = if (clause is SqlNodeList) clause as SqlNodeList? else SqlNodeList.of(clause)
        val sepOp: SqlBinaryOperator
        sepOp = if (sepKind == null) {
            SqlWriter.COMMA
        } else {
            when (sepKind) {
                AND -> SqlStdOperatorTable.AND
                OR -> SqlStdOperatorTable.OR
                else -> throw AssertionError()
            }
        }
        writer.list(SqlWriter.FrameTypeEnum.SIMPLE, sepOp, nodeList)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (obj !is SqlOperator) {
            return false
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false
        }
        val other = obj as SqlOperator
        return name.equals(other.name) && kind === other.kind
    }

    fun isName(testName: String?, caseSensitive: Boolean): Boolean {
        return if (caseSensitive) name.equals(testName) else name.equalsIgnoreCase(testName)
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(kind, name)
    }

    /**
     * Validates a call to this operator.
     *
     *
     * This method should not perform type-derivation or perform validation
     * related related to types. That is done later, by
     * [.deriveType]. This method
     * should focus on structural validation.
     *
     *
     * A typical implementation of this method first validates the operands,
     * then performs some operator-specific logic. The default implementation
     * just validates the operands.
     *
     *
     * This method is the default implementation of [SqlCall.validate];
     * but note that some sub-classes of [SqlCall] never call this method.
     *
     * @param call         the call to this operator
     * @param validator    the active validator
     * @param scope        validator scope
     * @param operandScope validator scope in which to validate operands to this
     * call; usually equal to scope, but not always because
     * some operators introduce new scopes
     * @see SqlNode.validateExpr
     * @see .deriveType
     */
    fun validateCall(
        call: SqlCall,
        validator: SqlValidator?,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        assert(call.getOperator() === this)
        for (operand in call.getOperandList()) {
            operand.validateExpr(validator, operandScope)
        }
    }

    /**
     * Validates the operands of a call, inferring the return type in the
     * process.
     *
     * @param validator active validator
     * @param scope     validation scope
     * @param call      call to be validated
     * @return inferred type
     */
    fun validateOperands(
        validator: SqlValidator,
        @Nullable scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType? {
        // Let subclasses know what's up.
        preValidateCall(validator, scope, call)

        // Check the number of operands
        checkOperandCount(validator, operandTypeChecker, call)
        val opBinding = SqlCallBinding(validator, scope, call)
        checkOperandTypes(
            opBinding,
            true
        )

        // Now infer the result type.
        val ret: RelDataType? = inferReturnType(opBinding)
        validator.setValidatedNodeType(call, ret)
        return ret
    }

    /**
     * Receives notification that validation of a call to this operator is
     * beginning. Subclasses can supply custom behavior; default implementation
     * does nothing.
     *
     * @param validator invoking validator
     * @param scope     validation scope
     * @param call      the call being validated
     */
    protected fun preValidateCall(
        validator: SqlValidator?,
        @Nullable scope: SqlValidatorScope?,
        call: SqlCall?
    ) {
    }

    /**
     * Infers the return type of an invocation of this operator; only called
     * after the number and types of operands have already been validated.
     * Subclasses must either override this method or supply an instance of
     * [SqlReturnTypeInference] to the constructor.
     *
     * @param opBinding description of invocation (not necessarily a
     * [SqlCall])
     * @return inferred return type
     */
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType? {
        if (returnTypeInference != null) {
            val returnType: RelDataType = returnTypeInference.inferReturnType(opBinding)
                ?: throw IllegalArgumentException(
                    ("Cannot infer return type for "
                            + opBinding.getOperator()) + "; operand types: "
                            + opBinding.collectOperandTypes()
                )
            if (operandTypeInference != null && opBinding is SqlCallBinding
                && this is SqlFunction
            ) {
                val callBinding: SqlCallBinding = opBinding as SqlCallBinding
                val operandTypes: List<RelDataType> = opBinding.collectOperandTypes()
                if (operandTypes.stream().anyMatch { t -> t.getSqlTypeName() === SqlTypeName.ANY }) {
                    val operandTypes2: Array<RelDataType> = operandTypes.toArray(arrayOfNulls<RelDataType>(0))
                    operandTypeInference.inferOperandTypes(callBinding, returnType, operandTypes2)
                    (callBinding.getValidator() as SqlValidatorImpl).callToOperandTypesMap
                        .put(callBinding.getCall(), ImmutableList.copyOf(operandTypes2))
                }
            }
            return returnType
        }
        throw Util.needToImplement(this)
    }

    /**
     * Derives the type of a call to this operator.
     *
     *
     * This method is an intrinsic part of the validation process so, unlike
     * [.inferReturnType], specific operators would not typically override
     * this method.
     *
     * @param validator Validator
     * @param scope     Scope of validation
     * @param call      Call to this operator
     * @return Type of call
     */
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope,
        call: SqlCall
    ): RelDataType? {
        for (operand in call.getOperandList()) {
            val nodeType: RelDataType = validator.deriveType(scope, operand)
            assert(nodeType != null)
        }
        val args: List<SqlNode> = constructOperandList(validator, call, null)
        val argTypes: List<RelDataType> = constructArgTypeList(
            validator, scope,
            call, args, false
        )

        // Always disable type coercion for builtin operator operands,
        // they are handled by the TypeCoercion specifically.
        val sqlOperator: SqlOperator = SqlUtil.lookupRoutine(
            validator.getOperatorTable(),
            validator.getTypeFactory(), nameAsId,
            argTypes, null, null, syntax, getKind(),
            validator.getCatalogReader().nameMatcher(), false
        )
            ?: throw validator.handleUnresolvedFunction(call, this, argTypes, null)
        (call as SqlBasicCall).setOperator(castNonNull(sqlOperator))
        var type: RelDataType? = call.getOperator().validateOperands(validator, scope, call)

        // Validate and determine coercibility and resulting collation
        // name of binary operator if needed.
        type = adjustType(validator, call, type)
        SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type)
        return type
    }

    @Nullable
    protected fun constructArgNameList(call: SqlCall): List<String>? {
        // If any arguments are named, construct a map.
        val nameBuilder: ImmutableList.Builder<String> = ImmutableList.builder()
        for (operand in call.getOperandList()) {
            if (operand.getKind() === SqlKind.ARGUMENT_ASSIGNMENT) {
                val operandList: List<SqlNode> = (operand as SqlCall).getOperandList()
                nameBuilder.add((operandList[1] as SqlIdentifier).getSimple())
            }
        }
        val argNames: ImmutableList<String> = nameBuilder.build()
        return if (argNames.isEmpty()) {
            null
        } else {
            argNames
        }
    }

    protected fun constructOperandList(
        validator: SqlValidator,
        call: SqlCall,
        @Nullable argNames: List<String?>?
    ): List<SqlNode> {
        if (argNames == null) {
            return call.getOperandList()
        }
        if (argNames.size() < call.getOperandList().size()) {
            throw validator.newValidationError(
                call,
                RESOURCE.someButNotAllArgumentsAreNamed()
            )
        }
        val duplicate: Int = Util.firstDuplicate(argNames)
        if (duplicate >= 0) {
            throw validator.newValidationError(
                call,
                RESOURCE.duplicateArgumentName(argNames[duplicate])
            )
        }
        val argBuilder: ImmutableList.Builder<SqlNode> = ImmutableList.builder()
        for (operand in call.getOperandList()) {
            if (operand.getKind() === SqlKind.ARGUMENT_ASSIGNMENT) {
                val operandList: List<SqlNode> = (operand as SqlCall).getOperandList()
                argBuilder.add(operandList[0])
            }
        }
        return argBuilder.build()
    }

    protected fun constructArgTypeList(
        validator: SqlValidator,
        scope: SqlValidatorScope,
        call: SqlCall?,
        args: List<SqlNode?>,
        convertRowArgToColumnList: Boolean
    ): List<RelDataType> {
        // Scope for operands. Usually the same as 'scope'.
        val operandScope: SqlValidatorScope = scope.getOperandScope(call)
        val argTypeBuilder: ImmutableList.Builder<RelDataType> = ImmutableList.builder()
        for (operand in args) {
            var nodeType: RelDataType
            // for row arguments that should be converted to ColumnList
            // types, set the nodeType to a ColumnList type but defer
            // validating the arguments of the row constructor until we know
            // for sure that the row argument maps to a ColumnList type
            if (operand.getKind() === SqlKind.ROW && convertRowArgToColumnList) {
                val typeFactory: RelDataTypeFactory = validator.getTypeFactory()
                nodeType = typeFactory.createSqlType(SqlTypeName.COLUMN_LIST)
                (validator as SqlValidatorImpl).setValidatedNodeType(operand, nodeType)
            } else {
                nodeType = validator.deriveType(operandScope, operand)
            }
            argTypeBuilder.add(nodeType)
        }
        return argTypeBuilder.build()
    }

    /**
     * Returns whether this operator should be surrounded by space when
     * unparsed.
     *
     * @return whether this operator should be surrounded by space
     */
    fun needsSpace(): Boolean {
        return true
    }

    /**
     * Validates and determines coercibility and resulting collation name of
     * binary operator if needed.
     */
    protected fun adjustType(
        validator: SqlValidator?,
        call: SqlCall?,
        type: RelDataType?
    ): RelDataType? {
        return type
    }

    /**
     * Infers the type of a call to this operator with a given set of operand
     * types. Shorthand for [.inferReturnType].
     */
    fun inferReturnType(
        typeFactory: RelDataTypeFactory?,
        operandTypes: List<RelDataType?>?
    ): RelDataType? {
        return inferReturnType(
            ExplicitOperatorBinding(
                typeFactory,
                this,
                operandTypes
            )
        )
    }

    /**
     * Checks that the operand values in a [SqlCall] to this operator are
     * valid. Subclasses must either override this method or supply an instance
     * of [SqlOperandTypeChecker] to the constructor.
     *
     * @param callBinding    description of call
     * @param throwOnFailure whether to throw an exception if check fails
     * (otherwise returns false in that case)
     * @return whether check succeeded
     */
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        // Check that all of the operands are of the right type.
        if (null == operandTypeChecker) {
            // If you see this you must either give operandTypeChecker a value
            // or override this method.
            throw Util.needToImplement(this)
        }
        if (kind !== SqlKind.ARGUMENT_ASSIGNMENT) {
            for (operand in Ord.zip(callBinding.operands())) {
                if (operand.e != null && operand.e.getKind() === SqlKind.DEFAULT && !operandTypeChecker.isOptional(
                        operand.i
                    )
                ) {
                    throw callBinding.newValidationError(RESOURCE.defaultForOptionalParameter())
                }
            }
        }
        return operandTypeChecker.checkOperandTypes(
            callBinding,
            throwOnFailure
        )
    }

    protected fun checkOperandCount(
        validator: SqlValidator,
        @Nullable argType: SqlOperandTypeChecker?,
        call: SqlCall
    ) {
        val od: SqlOperandCountRange = call.getOperator().getOperandCountRange()
        if (od.isValidCount(call.operandCount())) {
            return
        }
        if (od.getMin() === od.getMax()) {
            throw validator.newValidationError(
                call,
                RESOURCE.invalidArgCount(call.getOperator().getName(), od.getMin())
            )
        } else {
            throw validator.newValidationError(call, RESOURCE.wrongNumOfArguments())
        }
    }

    /**
     * Returns whether the given operands are valid. If not valid and
     * `fail`, throws an assertion error.
     *
     *
     * Similar to [.checkOperandCount], but some operators may have
     * different valid operands in [SqlNode] and `RexNode` formats
     * (some examples are CAST and AND), and this method throws internal errors,
     * not user errors.
     */
    fun validRexOperands(count: Int, litmus: Litmus?): Boolean {
        return true
    }

    /**
     * Returns a template describing how the operator signature is to be built.
     * E.g for the binary + operator the template looks like "{1} {0} {2}" {0}
     * is the operator, subsequent numbers are operands.
     *
     * @param operandsCount is used with functions that can take a variable
     * number of operands
     * @return signature template, or null to indicate that a default template
     * will suffice
     */
    @Nullable
    fun getSignatureTemplate(operandsCount: Int): String? {
        return null
    }

    /**
     * Returns a string describing the expected operand types of a call, e.g.
     * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     */
    val allowedSignatures: String
        get() = getAllowedSignatures(name)

    /**
     * Returns a string describing the expected operand types of a call, e.g.
     * "SUBSTRING(VARCHAR, INTEGER, INTEGER)" where the name (SUBSTRING in this
     * example) can be replaced by a specified name.
     */
    fun getAllowedSignatures(opNameToUse: String?): String {
        requireNonNull(
            operandTypeChecker, "If you see this, assign operandTypeChecker a value "
                    + "or override this function"
        )
        return operandTypeChecker.getAllowedSignatures(this, opNameToUse)
            .trim()
    }

    @Nullable
    fun getOperandTypeInference(): SqlOperandTypeInference? {
        return operandTypeInference
    }

    /**
     * Returns whether this operator is an aggregate function. By default,
     * subclass type is used (an instance of SqlAggFunction is assumed to be an
     * aggregator; anything else is not).
     *
     *
     * Per SQL:2011, there are <dfn>aggregate functions</dfn> and
     * <dfn>window functions</dfn>.
     * Every aggregate function (e.g. SUM) is also a window function.
     * There are window functions that are not aggregate functions, e.g. RANK,
     * NTILE, LEAD, FIRST_VALUE.
     *
     *
     * Collectively, aggregate and window functions are called <dfn>analytic
     * functions</dfn>. Despite its name, this method returns true for every
     * analytic function.
     *
     * @see .requiresOrder
     * @return whether this operator is an analytic function (aggregate function
     * or window function)
     */
    @get:Pure
    val isAggregator: Boolean
        get() = false

    /** Returns whether this is a window function that requires an OVER clause.
     *
     *
     * For example, returns true for `RANK`, `DENSE_RANK` and
     * other ranking functions; returns false for `SUM`, `COUNT`,
     * `MIN`, `MAX`, `AVG` (they can be used as non-window
     * aggregate functions).
     *
     *
     * If `requiresOver` returns true, then [.isAggregator] must
     * also return true.
     *
     * @see .allowsFraming
     * @see .requiresOrder
     */
    fun requiresOver(): Boolean {
        return false
    }

    /**
     * Returns whether this is a window function that requires ordering.
     *
     *
     * Per SQL:2011, 2, 6.10: "If &lt;ntile function&gt;, &lt;lead or lag
     * function&gt;, RANK or DENSE_RANK is specified, then the window ordering
     * clause shall be present."
     *
     * @see .isAggregator
     */
    fun requiresOrder(): Boolean {
        return false
    }

    /**
     * Returns whether this is a window function that allows framing (i.e. a
     * ROWS or RANGE clause in the window specification).
     */
    fun allowsFraming(): Boolean {
        return true
    }

    /**
     * Returns whether this is a group function.
     *
     *
     * Group functions can only appear in the GROUP BY clause.
     *
     *
     * Examples are `HOP`, `TUMBLE`, `SESSION`.
     *
     *
     * Group functions have auxiliary functions, e.g. `HOP_START`, but
     * these are not group functions.
     */
    val isGroup: Boolean
        get() = false

    /**
     * Returns whether this is an group auxiliary function.
     *
     *
     * Examples are `HOP_START` and `HOP_END` (both auxiliary to
     * `HOP`).
     *
     * @see .isGroup
     */
    val isGroupAuxiliary: Boolean
        get() = false

    /**
     * Accepts a [SqlVisitor], visiting each operand of a call. Returns
     * null.
     *
     * @param visitor Visitor
     * @param call    Call to visit
     */
    fun <R> acceptCall(visitor: SqlVisitor<R>?, call: SqlCall): @Nullable R? {
        for (operand in call.getOperandList()) {
            if (operand == null) {
                continue
            }
            operand.accept(visitor)
        }
        return null
    }

    /**
     * Accepts a [SqlVisitor], directing an
     * [org.apache.calcite.sql.util.SqlBasicVisitor.ArgHandler]
     * to visit an operand of a call.
     *
     *
     * The argument handler allows fine control about how the operands are
     * visited, and how the results are combined.
     *
     * @param visitor         Visitor
     * @param call            Call to visit
     * @param onlyExpressions If true, ignores operands which are not
     * expressions. For example, in the call to the
     * `AS` operator
     * @param argHandler      Called for each operand
     */
    fun <R> acceptCall(
        visitor: SqlVisitor<R>?,
        call: SqlCall,
        onlyExpressions: Boolean,
        argHandler: SqlBasicVisitor.ArgHandler<R>
    ) {
        val operands: List<SqlNode> = call.getOperandList()
        for (i in 0 until operands.size()) {
            argHandler.visitChild(visitor, call, i, operands[i])
        }
    }

    /** Returns the return type inference strategy for this operator, or null if
     * return type inference is implemented by a subclass override.  */
    @Nullable
    fun getReturnTypeInference(): SqlReturnTypeInference? {
        return returnTypeInference
    }

    /** Returns the operator that is the logical inverse of this operator.
     *
     *
     * For example, `SqlStdOperatorTable.LIKE.not()` returns
     * `SqlStdOperatorTable.NOT_LIKE`, and vice versa.
     *
     *
     * By default, returns `null`, which means there is no inverse
     * operator.
     *
     * @see .reverse
     */
    @Nullable
    operator fun not(): SqlOperator? {
        return null
    }

    /** Returns the operator that has the same effect as this operator
     * if its arguments are reversed.
     *
     *
     * For example, `SqlStdOperatorTable.GREATER_THAN.reverse()` returns
     * `SqlStdOperatorTable.LESS_THAN`, and vice versa,
     * because `a > b` is equivalent to `b < a`.
     *
     *
     * `SqlStdOperatorTable.EQUALS.reverse()` returns itself.
     *
     *
     * By default, returns `null`, which means there is no inverse
     * operator.
     *
     * @see SqlOperator.not
     * @see SqlKind.reverse
     */
    @Nullable
    fun reverse(): SqlOperator? {
        return null
    }

    /**
     * Returns the [Strong.Policy] strategy for this operator, or null if
     * there is no particular strategy, in which case this policy will be deducted
     * from the operator's [SqlKind].
     *
     * @see Strong
     */
    @get:Nullable
    @get:Pure
    val strongPolicyInference: Supplier<Strong.Policy>?
        get() = null

    /**
     * Returns whether this operator is monotonic.
     *
     *
     * Default implementation returns [SqlMonotonicity.NOT_MONOTONIC].
     *
     * @param call  Call to this operator
     * @param scope Scope in which the call occurs
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #getMonotonicity(SqlOperatorBinding)}")
    fun getMonotonicity(
        call: SqlCall?,
        scope: SqlValidatorScope
    ): SqlMonotonicity {
        return getMonotonicity(
            SqlCallBinding(scope.getValidator(), scope, call)
        )
    }

    /**
     * Returns whether a call to this operator is monotonic.
     *
     *
     * Default implementation returns [SqlMonotonicity.NOT_MONOTONIC].
     *
     * @param call Call to this operator with particular arguments and information
     * about the monotonicity of the arguments
     */
    fun getMonotonicity(call: SqlOperatorBinding?): SqlMonotonicity {
        return SqlMonotonicity.NOT_MONOTONIC
    }

    /**
     * Returns whether a call to this operator is guaranteed to always return
     * the same result given the same operands; true is assumed by default.
     */
    val isDeterministic: Boolean
        get() = true

    /**
     * Returns whether a call to this operator is not sensitive to the operands input order.
     * An operator is symmetrical if the call returns the same result when
     * the operands are shuffled.
     *
     *
     * By default, returns true for [SqlKind.SYMMETRICAL].
     */
    val isSymmetrical: Boolean
        get() = SqlKind.SYMMETRICAL.contains(kind)

    /**
     * Returns whether it is unsafe to cache query plans referencing this
     * operator; false is assumed by default.
     */
    val isDynamicFunction: Boolean
        get() = false

    /**
     * Method to check if call requires expansion when it has decimal operands.
     * The default implementation is to return true.
     */
    fun requiresDecimalExpansion(): Boolean {
        return true
    }

    /**
     * Returns whether the `ordinal`th argument to this operator must
     * be scalar (as opposed to a query).
     *
     *
     * If true (the default), the validator will attempt to convert the
     * argument into a scalar sub-query, which must have one column and return at
     * most one row.
     *
     *
     * Operators such as `SELECT` and `EXISTS` override
     * this method.
     */
    fun argumentMustBeScalar(ordinal: Int): Boolean {
        return true
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val NL: String = System.getProperty("line.separator")

        /**
         * Maximum precedence.
         */
        const val MDX_PRECEDENCE = 200

        //~ Methods ----------------------------------------------------------------
        protected fun leftPrec(prec: Int, leftAssoc: Boolean): Int {
            var prec = prec
            assert(prec % 2 == 0)
            if (!leftAssoc) {
                ++prec
            }
            return prec
        }

        protected fun rightPrec(prec: Int, leftAssoc: Boolean): Int {
            var prec = prec
            assert(prec % 2 == 0)
            if (leftAssoc) {
                ++prec
            }
            return prec
        }

        /** Not supported. Choose between
         * [.createCall] and
         * [.createCall]. The ambiguity arises because
         * [SqlNodeList] extends [SqlNode]
         * and also implements `List<SqlNode>`.  */
        @Deprecated
        fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?,
            operands: SqlNodeList?
        ): SqlCall {
            throw UnsupportedOperationException()
        }
    }
}
