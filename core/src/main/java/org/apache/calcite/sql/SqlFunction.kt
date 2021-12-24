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

import org.apache.calcite.linq4j.function.Functions

/**
 * A `SqlFunction` is a type of operator which has conventional
 * function-call syntax.
 */
class SqlFunction protected constructor(
    name: String?,
    @Nullable sqlIdentifier: SqlIdentifier?,
    kind: SqlKind?,
    @Nullable returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?,
    category: SqlFunctionCategory?
) : SqlOperator(
    name, kind, 100, 100, returnTypeInference, operandTypeInference,
    operandTypeChecker
) {
    //~ Instance fields --------------------------------------------------------
    private val category: SqlFunctionCategory

    @Nullable
    private val sqlIdentifier: SqlIdentifier?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a new SqlFunction for a call to a built-in function.
     *
     * @param name                 Name of built-in function
     * @param kind                 kind of operator implemented by function
     * @param returnTypeInference  strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker   strategy to use for parameter type checking
     * @param category             categorization for function
     */
    constructor(
        name: String?,
        kind: SqlKind?,
        @Nullable returnTypeInference: SqlReturnTypeInference?,
        @Nullable operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        category: SqlFunctionCategory
    ) : this(
        name, null, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, category
    ) {
        // We leave sqlIdentifier as null to indicate
        // that this is a built-in.
        assert(
            !(category === SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR
                    && returnTypeInference == null)
        )
    }

    /**
     * Creates a placeholder SqlFunction for an invocation of a function with a
     * possibly qualified name. This name must be resolved into either a built-in
     * function or a user-defined function.
     *
     * @param sqlIdentifier        possibly qualified identifier for function
     * @param returnTypeInference  strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker   strategy to use for parameter type checking
     * @param paramTypes           array of parameter types
     * @param funcType             function category
     */
    constructor(
        sqlIdentifier: SqlIdentifier,
        @Nullable returnTypeInference: SqlReturnTypeInference?,
        @Nullable operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        @Nullable paramTypes: List<RelDataType?>?,
        funcType: SqlFunctionCategory?
    ) : this(
        Util.last(sqlIdentifier.names), sqlIdentifier, SqlKind.OTHER_FUNCTION,
        returnTypeInference, operandTypeInference, operandTypeChecker,
        paramTypes, funcType
    ) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        name: String?,
        @Nullable sqlIdentifier: SqlIdentifier?,
        kind: SqlKind?,
        @Nullable returnTypeInference: SqlReturnTypeInference?,
        @Nullable operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        @Nullable paramTypes: List<RelDataType?>?,
        category: SqlFunctionCategory?
    ) : this(
        name, sqlIdentifier, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, category
    ) {
    }

    /**
     * Internal constructor.
     */
    init {
        this.sqlIdentifier = sqlIdentifier
        this.category = Objects.requireNonNull(category, "category")
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.FUNCTION

    /**
     * Returns the fully-qualified name of function, or null for a built-in
     * function.
     */
    @Nullable
    fun getSqlIdentifier(): SqlIdentifier? {
        return sqlIdentifier
    }

    @get:Override
    val nameAsId: SqlIdentifier
        get() = if (sqlIdentifier != null) {
            sqlIdentifier
        } else super.getNameAsId()// to be removed before 2.0

    /** Use [SqlOperandMetadata.paramTypes] on the
     * result of [.getOperandTypeChecker].  */
    @get:Nullable
    @get:Deprecated
    val paramTypes: List<Any>?
    // to be removed before 2.0 get() {
    return null
}

/** Use [SqlOperandMetadata.paramNames] on the result of
 * [.getOperandTypeChecker].  */ // to be removed before 2.0
@get:Deprecated
val paramNames: List<String>
    @Deprecated get() = Functions.generate(castNonNull(paramTypes).size()) { i -> "arg$i" }

@Override
fun unparse(
    writer: SqlWriter?,
    call: SqlCall?,
    leftPrec: Int,
    rightPrec: Int
) {
    syntax.unparse(writer, this, call, leftPrec, rightPrec)
}

/**
 * Return function category.
 */
val functionType: org.apache.calcite.sql.SqlFunctionCategory
    get() = this.category

/**
 * Returns whether this function allows a `DISTINCT` or `
 * ALL` quantifier. The default is `false`; some aggregate
 * functions return `true`.
 */
val isQuantifierAllowed: Boolean
    @Pure get() = false

@Override
fun validateCall(
    call: SqlCall,
    validator: SqlValidator,
    scope: SqlValidatorScope?,
    operandScope: SqlValidatorScope?
) {
    // This implementation looks for the quantifier keywords DISTINCT or
    // ALL as the first operand in the list.  If found then the literal is
    // not called to validate itself.  Further the function is checked to
    // make sure that a quantifier is valid for that particular function.
    //
    // If the first operand does not appear to be a quantifier then the
    // parent ValidateCall is invoked to do normal function validation.
    super.validateCall(call, validator, scope, operandScope)
    validateQuantifier(validator, call)
}

/**
 * Throws a validation error if a DISTINCT or ALL quantifier is present but
 * not allowed.
 */
fun validateQuantifier(validator: SqlValidator, call: SqlCall) {
    val functionQuantifier: SqlLiteral = call.getFunctionQuantifier()
    if (null != functionQuantifier && !isQuantifierAllowed) {
        throw validator.newValidationError(
            functionQuantifier,
            RESOURCE.functionQuantifierNotAllowed(call.getOperator().getName())
        )
    }
}

@Override
fun deriveType(
    validator: SqlValidator,
    scope: SqlValidatorScope,
    call: SqlCall
): RelDataType {
    return deriveType(validator, scope, call, true)
}

private fun deriveType(
    validator: SqlValidator,
    scope: SqlValidatorScope,
    call: SqlCall,
    convertRowArgToColumnList: Boolean
): RelDataType {
    // Scope for operands. Usually the same as 'scope'.
    val operandScope: SqlValidatorScope = scope.getOperandScope(call)

    // Indicate to the validator that we're validating a new function call
    validator.pushFunctionCall()
    val argNames: List<String> = constructArgNameList(call)
    val args: List<SqlNode> = constructOperandList(validator, call, argNames)
    val argTypes: List<RelDataType> = constructArgTypeList(
        validator, scope,
        call, args, convertRowArgToColumnList
    )
    var function: SqlFunction? = SqlUtil.lookupRoutine(
        validator.getOperatorTable(),
        validator.getTypeFactory(), nameAsId, argTypes, argNames,
        functionType, SqlSyntax.FUNCTION, getKind(),
        validator.getCatalogReader().nameMatcher(), false
    )
    return try {
        // if we have a match on function name and parameter count, but
        // couldn't find a function with  a COLUMN_LIST type, retry, but
        // this time, don't convert the row argument to a COLUMN_LIST type;
        // if we did find a match, go back and re-validate the row operands
        // (corresponding to column references), now that we can set the
        // scope to that of the source cursor referenced by that ColumnList
        // type
        if (convertRowArgToColumnList && SqlFunction.Companion.containsRowArg(args)) {
            if (function == null
                && SqlUtil.matchRoutinesByParameterCount(
                    validator.getOperatorTable(), nameAsId, argTypes,
                    functionType,
                    validator.getCatalogReader().nameMatcher()
                )
            ) {
                // remove the already validated node types corresponding to
                // row arguments before re-validating
                for (operand in args) {
                    if (operand.getKind() === SqlKind.ROW) {
                        validator.removeValidatedNodeType(operand)
                    }
                }
                return deriveType(validator, scope, call, false)
            } else if (function != null) {
                validator.validateColumnListParams(function, argTypes, args)
            }
        }
        if (functionType === SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR) {
            return validator.deriveConstructorType(
                scope, call, this, function,
                argTypes
            )
        }
        validCoercionType@ if (function == null) {
            if (validator.config().typeCoercionEnabled()) {
                // try again if implicit type coercion is allowed.
                function = SqlUtil.lookupRoutine(
                    validator.getOperatorTable(),
                    validator.getTypeFactory(),
                    nameAsId,
                    argTypes, argNames, functionType, SqlSyntax.FUNCTION,
                    getKind(), validator.getCatalogReader().nameMatcher(), true
                )
                // try to coerce the function arguments to the declared sql type name.
                // if we succeed, the arguments would be wrapped with CAST operator.
                if (function != null) {
                    val typeCoercion: TypeCoercion = validator.getTypeCoercion()
                    if (typeCoercion.userDefinedFunctionCoercion(scope, call, function)) {
                        break@validCoercionType
                    }
                }
            }

            // check if the identifier represents type
            val x = call.getOperator() as SqlFunction
            val identifier: SqlIdentifier = Util.first(
                x.getSqlIdentifier(),
                SqlIdentifier(x.getName(), SqlParserPos.ZERO)
            )
            val type: RelDataType = validator.getCatalogReader().getNamedType(identifier)
            if (type != null) {
                function = SqlTypeConstructorFunction(identifier, type)
                break@validCoercionType
            }

            // if function doesn't exist within operator table and known function
            // handling is turned off then create a more permissive function
            if (function == null && validator.config().lenientOperatorLookup()) {
                function = SqlUnresolvedFunction(
                    identifier, null,
                    null, OperandTypes.VARIADIC, null, x.functionType
                )
                break@validCoercionType
            }
            throw validator.handleUnresolvedFunction(
                call, this, argTypes,
                argNames
            )
        }

        // REVIEW jvs 25-Mar-2005:  This is, in a sense, expanding
        // identifiers, but we ignore shouldExpandIdentifiers()
        // because otherwise later validation code will
        // choke on the unresolved function.
        (call as SqlBasicCall).setOperator(function)
        function.validateOperands(
            validator,
            operandScope,
            call
        )
    } finally {
        validator.popFunctionCall()
    }
}

companion object {
    private fun containsRowArg(args: List<SqlNode>): Boolean {
        for (operand in args) {
            if (operand.getKind() === SqlKind.ROW) {
                return true
            }
        }
        return false
    }
}
}
