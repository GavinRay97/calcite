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

import org.apache.calcite.sql.validate.SqlValidatorException
import org.apache.calcite.runtime.Resources.BaseMessage
import org.apache.calcite.runtime.Resources.ExInst
import org.apache.calcite.runtime.Resources.ExInstWithCause
import org.apache.calcite.runtime.Resources.Inst
import org.apache.calcite.runtime.Resources.Property

/**
 * Compiler-checked resources for the Calcite project.
 */
interface CalciteResource {
    @BaseMessage("line {0,number,#}, column {1,number,#}")
    fun parserContext(a0: Int, a1: Int): Inst?

    @BaseMessage("Bang equal ''!='' is not allowed under the current SQL conformance level")
    fun bangEqualNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("Percent remainder ''%'' is not allowed under the current SQL conformance level")
    fun percentRemainderNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("''LIMIT start, count'' is not allowed under the current SQL conformance level")
    fun limitStartCountNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("APPLY operator is not allowed under the current SQL conformance level")
    fun applyNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("JSON path expression must be specified after the JSON value expression")
    fun jsonPathMustBeSpecified(): ExInst<CalciteException?>?

    @BaseMessage("Illegal {0} literal {1}: {2}")
    fun illegalLiteral(a0: String?, a1: String?, a2: String?): ExInst<CalciteException?>?

    @BaseMessage("Length of identifier ''{0}'' must be less than or equal to {1,number,#} characters")
    fun identifierTooLong(a0: String?, a1: Int): ExInst<CalciteException?>?

    @BaseMessage("not in format ''{0}''")
    fun badFormat(a0: String?): Inst?

    @BaseMessage("BETWEEN operator has no terminating AND")
    fun betweenWithoutAnd(): ExInst<SqlValidatorException?>?

    @BaseMessage("Geo-spatial extensions and the GEOMETRY data type are not enabled")
    fun geometryDisabled(): ExInst<SqlValidatorException?>?

    @BaseMessage("Illegal INTERVAL literal {0}; at {1}")
    @Property(name = "SQLSTATE", value = "42000")
    fun illegalIntervalLiteral(a0: String?, a1: String?): ExInst<CalciteException?>?

    @BaseMessage("Illegal expression. Was expecting \"(DATETIME - DATETIME) INTERVALQUALIFIER\"")
    fun illegalMinusDate(): ExInst<CalciteException?>?

    @BaseMessage("Illegal overlaps expression. Was expecting expression on the form \"(DATETIME, EXPRESSION) OVERLAPS (DATETIME, EXPRESSION)\"")
    fun illegalOverlaps(): ExInst<CalciteException?>?

    @BaseMessage("Non-query expression encountered in illegal context")
    fun illegalNonQueryExpression(): ExInst<CalciteException?>?

    @BaseMessage("Query expression encountered in illegal context")
    fun illegalQueryExpression(): ExInst<CalciteException?>?

    @BaseMessage("CURSOR expression encountered in illegal context")
    fun illegalCursorExpression(): ExInst<CalciteException?>?

    @BaseMessage("ORDER BY unexpected")
    fun illegalOrderBy(): ExInst<CalciteException?>?

    @BaseMessage("Illegal binary string {0}")
    fun illegalBinaryString(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("''FROM'' without operands preceding it is illegal")
    fun illegalFromEmpty(): ExInst<CalciteException?>?

    @BaseMessage("ROW expression encountered in illegal context")
    fun illegalRowExpression(): ExInst<CalciteException?>?

    @BaseMessage("Illegal identifier '':''. Was expecting ''VALUE''")
    fun illegalColon(): ExInst<CalciteException?>?

    @BaseMessage("TABLESAMPLE percentage must be between 0 and 100, inclusive")
    @Property(name = "SQLSTATE", value = "2202H")
    fun invalidSampleSize(): ExInst<CalciteException?>?

    @BaseMessage("Literal ''{0}'' can not be parsed to type ''{1}''")
    fun invalidLiteral(a0: String?, a1: String?): ExInst<CalciteException?>?

    @BaseMessage("Unknown character set ''{0}''")
    fun unknownCharacterSet(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("Failed to encode ''{0}'' in character set ''{1}''")
    fun charsetEncoding(a0: String?, a1: String?): ExInst<CalciteException?>?

    @BaseMessage("UESCAPE ''{0}'' must be exactly one character")
    fun unicodeEscapeCharLength(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("UESCAPE ''{0}'' may not be hex digit, whitespace, plus sign, or double quote")
    fun unicodeEscapeCharIllegal(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("UESCAPE cannot be specified without Unicode literal introducer")
    fun unicodeEscapeUnexpected(): ExInst<CalciteException?>?

    @BaseMessage("Unicode escape sequence starting at character {0,number,#} is not exactly four hex digits")
    fun unicodeEscapeMalformed(a0: Int): ExInst<SqlValidatorException?>?

    @BaseMessage("No match found for function signature {0}")
    fun validatorUnknownFunction(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Invalid number of arguments to function ''{0}''. Was expecting {1,number,#} arguments")
    fun invalidArgCount(a0: String?, a1: Int): ExInst<SqlValidatorException?>?

    @BaseMessage("At line {0,number,#}, column {1,number,#}")
    fun validatorContextPoint(
        a0: Int,
        a1: Int
    ): ExInstWithCause<CalciteContextException?>?

    @BaseMessage("From line {0,number,#}, column {1,number,#} to line {2,number,#}, column {3,number,#}")
    fun validatorContext(
        a0: Int, a1: Int,
        a2: Int,
        a3: Int
    ): ExInstWithCause<CalciteContextException?>?

    @BaseMessage("Cast function cannot convert value of type {0} to type {1}")
    fun cannotCastValue(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Unknown datatype name ''{0}''")
    fun unknownDatatypeName(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Values passed to {0} operator must have compatible types")
    fun incompatibleValueType(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Values in expression list must have compatible types")
    fun incompatibleTypesInList(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot apply {0} to the two different charsets {1} and {2}")
    fun incompatibleCharset(
        a0: String?, a1: String?,
        a2: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("ORDER BY is only allowed on top-level SELECT")
    fun invalidOrderByPos(): ExInst<SqlValidatorException?>?

    @BaseMessage("Unknown identifier ''{0}''")
    fun unknownIdentifier(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Unknown field ''{0}''")
    fun unknownField(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Unknown target column ''{0}''")
    fun unknownTargetColumn(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Target column ''{0}'' is assigned more than once")
    fun duplicateTargetColumn(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Number of INSERT target columns ({0,number}) does not equal number of source items ({1,number})")
    fun unmatchInsertColumn(a0: Int, a1: Int): ExInst<SqlValidatorException?>?

    @BaseMessage("Column ''{0}'' has no default value and does not allow NULLs")
    fun columnNotNullable(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot assign to target field ''{0}'' of type {1} from source field ''{2}'' of type {3}")
    fun typeNotAssignable(
        a0: String?, a1: String?,
        a2: String?, a3: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Table ''{0}'' not found")
    fun tableNameNotFound(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Table ''{0}'' not found; did you mean ''{1}''?")
    fun tableNameNotFoundDidYouMean(
        a0: String?,
        a1: String?
    ): ExInst<SqlValidatorException?>?

    /** Same message as [.tableNameNotFound] but a different kind
     * of exception, so it can be used in `RelBuilder`.  */
    @BaseMessage("Table ''{0}'' not found")
    fun tableNotFound(tableName: String?): ExInst<CalciteException?>?

    @BaseMessage("Object ''{0}'' not found")
    fun objectNotFound(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Object ''{0}'' not found within ''{1}''")
    fun objectNotFoundWithin(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Object ''{0}'' not found; did you mean ''{1}''?")
    fun objectNotFoundDidYouMean(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Object ''{0}'' not found within ''{1}''; did you mean ''{2}''?")
    fun objectNotFoundWithinDidYouMean(
        a0: String?,
        a1: String?, a2: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Table ''{0}'' is not a sequence")
    fun notASequence(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Column ''{0}'' not found in any table")
    fun columnNotFound(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Column ''{0}'' not found in any table; did you mean ''{1}''?")
    fun columnNotFoundDidYouMean(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Column ''{0}'' not found in table ''{1}''")
    fun columnNotFoundInTable(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Column ''{0}'' not found in table ''{1}''; did you mean ''{2}''?")
    fun columnNotFoundInTableDidYouMean(
        a0: String?,
        a1: String?, a2: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Column ''{0}'' is ambiguous")
    fun columnAmbiguous(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Param ''{0}'' not found in function ''{1}''; did you mean ''{2}''?")
    fun paramNotFoundInFunctionDidYouMean(
        a0: String?,
        a1: String?, a2: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Operand {0} must be a query")
    fun needQueryOp(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Parameters must be of the same type")
    fun needSameTypeParameter(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot apply ''{0}'' to arguments of type {1}. Supported form(s): {2}")
    fun canNotApplyOp2Type(
        a0: String?, a1: String?,
        a2: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Expected a boolean type")
    fun expectedBoolean(): ExInst<SqlValidatorException?>?

    @BaseMessage("Expected a character type")
    fun expectedCharacter(): ExInst<SqlValidatorException?>?

    @BaseMessage("ELSE clause or at least one THEN clause must be non-NULL")
    fun mustNotNullInElse(): ExInst<SqlValidatorException?>?

    @BaseMessage("Function ''{0}'' is not defined")
    fun functionUndefined(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Encountered {0} with {1,number} parameter(s); was expecting {2}")
    fun wrongNumberOfParam(
        a0: String?, a1: Int,
        a2: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Illegal mixing of types in CASE or COALESCE statement")
    fun illegalMixingOfTypes(): ExInst<SqlValidatorException?>?

    @BaseMessage("Invalid compare. Comparing (collation, coercibility): ({0}, {1} with ({2}, {3}) is illegal")
    fun invalidCompare(
        a0: String?, a1: String?, a2: String?,
        a3: String?
    ): ExInst<CalciteException?>?

    @BaseMessage("Invalid syntax. Two explicit different collations ({0}, {1}) are illegal")
    fun differentCollations(a0: String?, a1: String?): ExInst<CalciteException?>?

    @BaseMessage("{0} is not comparable to {1}")
    fun typeNotComparable(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot compare values of types ''{0}'', ''{1}''")
    fun typeNotComparableNear(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Wrong number of arguments to expression")
    fun wrongNumOfArguments(): ExInst<SqlValidatorException?>?

    @BaseMessage("Operands {0} not comparable to each other")
    fun operandNotComparable(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Types {0} not comparable to each other")
    fun typeNotComparableEachOther(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Numeric literal ''{0}'' out of range")
    fun numberLiteralOutOfRange(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Date literal ''{0}'' out of range")
    fun dateLiteralOutOfRange(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("String literal continued on same line")
    fun stringFragsOnSameLine(): ExInst<SqlValidatorException?>?

    @BaseMessage("Table or column alias must be a simple identifier")
    fun aliasMustBeSimpleIdentifier(): ExInst<SqlValidatorException?>?

    @BaseMessage("Expecting alias, found character literal")
    fun charLiteralAliasNotValid(): ExInst<SqlValidatorException?>?

    @BaseMessage("List of column aliases must have same degree as table; table has {0,number,#} columns {1}, whereas alias list has {2,number,#} columns")
    fun aliasListDegree(a0: Int, a1: String?, a2: Int): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate name ''{0}'' in column alias list")
    fun aliasListDuplicate(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("INNER, LEFT, RIGHT or FULL join requires a condition (NATURAL keyword or ON or USING clause)")
    fun joinRequiresCondition(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot qualify common column ''{0}''")
    fun disallowsQualifyingCommonColumn(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot specify condition (NATURAL keyword, or ON or USING clause) following CROSS JOIN")
    fun crossJoinDisallowsCondition(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot specify NATURAL keyword with ON or USING clause")
    fun naturalDisallowsOnOrUsing(): ExInst<SqlValidatorException?>?

    @BaseMessage("Column name ''{0}'' in USING clause is not unique on one side of join")
    fun columnInUsingNotUnique(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Column ''{0}'' matched using NATURAL keyword or USING clause has incompatible types: cannot compare ''{1}'' to ''{2}''")
    fun naturalOrUsingColumnNotCompatible(
        a0: String?,
        a1: String?, a2: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("OVER clause is necessary for window functions")
    fun absentOverClause(): ExInst<SqlValidatorException?>?

    @BaseMessage("Window ''{0}'' not found")
    fun windowNotFound(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot specify IGNORE NULLS or RESPECT NULLS following ''{0}''")
    fun disallowsNullTreatment(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Expression ''{0}'' is not being grouped")
    fun notGroupExpr(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Argument to {0} operator must be a grouped expression")
    fun groupingArgument(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("{0} operator may only occur in an aggregate query")
    fun groupingInAggregate(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("{0} operator may only occur in SELECT, HAVING or ORDER BY clause")
    fun groupingInWrongClause(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Expression ''{0}'' is not in the select clause")
    fun notSelectDistinctExpr(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Aggregate expression is illegal in {0} clause")
    fun aggregateIllegalInClause(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Windowed aggregate expression is illegal in {0} clause")
    fun windowedAggregateIllegalInClause(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Aggregate expressions cannot be nested")
    fun nestedAggIllegal(): ExInst<SqlValidatorException?>?

    @BaseMessage("FILTER must not contain aggregate expression")
    fun aggregateInFilterIllegal(): ExInst<SqlValidatorException?>?

    @BaseMessage("WITHIN GROUP must not contain aggregate expression")
    fun aggregateInWithinGroupIllegal(): ExInst<SqlValidatorException?>?

    @BaseMessage("WITHIN DISTINCT must not contain aggregate expression")
    fun aggregateInWithinDistinctIllegal(): ExInst<SqlValidatorException?>?

    @BaseMessage("Aggregate expression ''{0}'' must contain a WITHIN GROUP clause")
    fun aggregateMissingWithinGroupClause(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Aggregate expression ''{0}'' must not contain a WITHIN GROUP clause")
    fun withinGroupClauseIllegalInAggregate(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT")
    fun aggregateIllegalInOrderBy(): ExInst<SqlValidatorException?>?

    @BaseMessage("{0} clause must be a condition")
    fun condMustBeBoolean(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("HAVING clause must be a condition")
    fun havingMustBeBoolean(): ExInst<SqlValidatorException?>?

    @BaseMessage("OVER must be applied to aggregate function")
    fun overNonAggregate(): ExInst<SqlValidatorException?>?

    @BaseMessage("FILTER must be applied to aggregate function")
    fun filterNonAggregate(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot override window attribute")
    fun cannotOverrideWindowAttribute(): ExInst<SqlValidatorException?>?

    @BaseMessage("Column count mismatch in {0}")
    fun columnCountMismatchInSetop(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Type mismatch in column {0,number} of {1}")
    fun columnTypeMismatchInSetop(a0: Int, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Binary literal string must contain an even number of hexits")
    fun binaryLiteralOdd(): ExInst<SqlValidatorException?>?

    @BaseMessage("Binary literal string must contain only characters ''0'' - ''9'', ''A'' - ''F''")
    fun binaryLiteralInvalid(): ExInst<SqlValidatorException?>?

    @BaseMessage("Illegal interval literal format {0} for {1}")
    fun unsupportedIntervalLiteral(
        a0: String?,
        a1: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Interval field value {0,number} exceeds precision of {1} field")
    fun intervalFieldExceedsPrecision(
        a0: Number?,
        a1: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("RANGE clause cannot be used with compound ORDER BY clause")
    fun compoundOrderByProhibitsRange(): ExInst<SqlValidatorException?>?

    @BaseMessage("Data type of ORDER BY prohibits use of RANGE clause")
    fun orderByDataTypeProhibitsRange(): ExInst<SqlValidatorException?>?

    @BaseMessage("Data Type mismatch between ORDER BY and RANGE clause")
    fun orderByRangeMismatch(): ExInst<SqlValidatorException?>?

    @BaseMessage("Window ORDER BY expression of type DATE requires range of type INTERVAL")
    fun dateRequiresInterval(): ExInst<SqlValidatorException?>?

    @BaseMessage("ROWS value must be a non-negative integral constant")
    fun rowMustBeNonNegativeIntegral(): ExInst<SqlValidatorException?>?

    @BaseMessage("Window specification must contain an ORDER BY clause")
    fun overMissingOrderBy(): ExInst<SqlValidatorException?>?

    @BaseMessage("PARTITION BY expression should not contain OVER clause")
    fun partitionbyShouldNotContainOver(): ExInst<SqlValidatorException?>?

    @BaseMessage("ORDER BY expression should not contain OVER clause")
    fun orderbyShouldNotContainOver(): ExInst<SqlValidatorException?>?

    @BaseMessage("UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary")
    fun badLowerBoundary(): ExInst<SqlValidatorException?>?

    @BaseMessage("UNBOUNDED PRECEDING cannot be specified for the upper frame boundary")
    fun badUpperBoundary(): ExInst<SqlValidatorException?>?

    @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW")
    fun currentRowPrecedingError(): ExInst<SqlValidatorException?>?

    @BaseMessage("Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING")
    fun currentRowFollowingError(): ExInst<SqlValidatorException?>?

    @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING")
    fun followingBeforePrecedingError(): ExInst<SqlValidatorException?>?

    @BaseMessage("Window name must be a simple identifier")
    fun windowNameMustBeSimple(): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate window names not allowed")
    fun duplicateWindowName(): ExInst<SqlValidatorException?>?

    @BaseMessage("Empty window specification not allowed")
    fun emptyWindowSpec(): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate window specification not allowed in the same window clause")
    fun dupWindowSpec(): ExInst<SqlValidatorException?>?

    @BaseMessage("ROW/RANGE not allowed with RANK, DENSE_RANK or ROW_NUMBER functions")
    fun rankWithFrame(): ExInst<SqlValidatorException?>?

    @BaseMessage("RANK or DENSE_RANK functions require ORDER BY clause in window specification")
    fun funcNeedsOrderBy(): ExInst<SqlValidatorException?>?

    @BaseMessage("PARTITION BY not allowed with existing window reference")
    fun partitionNotAllowed(): ExInst<SqlValidatorException?>?

    @BaseMessage("ORDER BY not allowed in both base and referenced windows")
    fun orderByOverlap(): ExInst<SqlValidatorException?>?

    @BaseMessage("Referenced window cannot have framing declarations")
    fun refWindowWithFrame(): ExInst<SqlValidatorException?>?

    @BaseMessage("Type ''{0}'' is not supported")
    fun typeNotSupported(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Invalid type ''{0}'' in ORDER BY clause of ''{1}'' function. Only NUMERIC types are supported")
    fun unsupportedTypeInOrderBy(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("''{0}'' requires precisely one ORDER BY key")
    fun orderByRequiresOneKey(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("DISTINCT/ALL not allowed with {0} function")
    fun functionQuantifierNotAllowed(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("WITHIN GROUP not allowed with {0} function")
    fun withinGroupNotAllowed(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("WITHIN DISTINCT not allowed with {0} function")
    fun withinDistinctNotAllowed(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Some but not all arguments are named")
    fun someButNotAllArgumentsAreNamed(): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate argument name ''{0}''")
    fun duplicateArgumentName(name: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("DEFAULT is only allowed for optional parameters")
    fun defaultForOptionalParameter(): ExInst<SqlValidatorException?>?

    @BaseMessage("DEFAULT not allowed here")
    fun defaultNotAllowed(): ExInst<SqlValidatorException?>?

    @BaseMessage("Not allowed to perform {0} on {1}")
    fun accessNotAllowed(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("The {0} function does not support the {1} data type.")
    fun minMaxBadType(a0: String?, a1: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Only scalar sub-queries allowed in select list.")
    fun onlyScalarSubQueryAllowed(): ExInst<SqlValidatorException?>?

    @BaseMessage("Ordinal out of range")
    fun orderByOrdinalOutOfRange(): ExInst<SqlValidatorException?>?

    @BaseMessage("Window has negative size")
    fun windowHasNegativeSize(): ExInst<SqlValidatorException?>?

    @BaseMessage("UNBOUNDED FOLLOWING window not supported")
    fun unboundedFollowingWindowNotSupported(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot use DISALLOW PARTIAL with window based on RANGE")
    fun cannotUseDisallowPartialWithRange(): ExInst<SqlValidatorException?>?

    @BaseMessage("Interval leading field precision ''{0,number,#}'' out of range for {1}")
    fun intervalStartPrecisionOutOfRange(
        a0: Int,
        a1: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Interval fractional second precision ''{0,number,#}'' out of range for {1}")
    fun intervalFractionalSecondPrecisionOutOfRange(
        a0: Int, a1: String?
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate relation name ''{0}'' in FROM clause")
    fun fromAliasDuplicate(@Nullable a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate column name ''{0}'' in output")
    fun duplicateColumnName(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate name ''{0}'' in column list")
    fun duplicateNameInColumnList(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Internal error: {0}")
    fun internal(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("Argument to function ''{0}'' must be a literal")
    fun argumentMustBeLiteral(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Argument to function ''{0}'' must be a positive integer literal")
    fun argumentMustBePositiveInteger(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Argument to function ''{0}'' must be a numeric literal between {1,number,#} and {2,number,#}")
    fun argumentMustBeNumericLiteralInRange(a0: String?, min: Int, max: Int): ExInst<SqlValidatorException?>?

    @BaseMessage("Validation Error: {0}")
    fun validationError(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("Locale ''{0}'' in an illegal format")
    fun illegalLocaleFormat(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("Argument to function ''{0}'' must not be NULL")
    fun argumentMustNotBeNull(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Illegal use of ''NULL''")
    fun nullIllegal(): ExInst<SqlValidatorException?>?

    @BaseMessage("Illegal use of dynamic parameter")
    fun dynamicParamIllegal(): ExInst<SqlValidatorException?>?

    @BaseMessage("''{0}'' is not a valid boolean value")
    fun invalidBoolean(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("Argument to function ''{0}'' must be a valid precision between ''{1,number,#}'' and ''{2,number,#}''")
    fun argumentMustBeValidPrecision(
        a0: String?, a1: Int,
        a2: Int
    ): ExInst<SqlValidatorException?>?

    @BaseMessage("Wrong arguments for table function ''{0}'' call. Expected ''{1}'', actual ''{2}''")
    fun illegalArgumentForTableFunctionCall(
        a0: String?,
        a1: String?, a2: String?
    ): ExInst<CalciteException?>?

    @BaseMessage("Cannot call table function here: ''{0}''")
    fun cannotCallTableFunctionHere(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("''{0}'' is not a valid datetime format")
    fun invalidDatetimeFormat(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("Cannot INSERT into generated column ''{0}''")
    fun insertIntoAlwaysGenerated(a0: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Argument to function ''{0}'' must have a scale of 0")
    fun argumentMustHaveScaleZero(a0: String?): ExInst<CalciteException?>?

    @BaseMessage("Statement preparation aborted")
    fun preparationAborted(): ExInst<CalciteException?>?

    @BaseMessage("Warning: use of non-standard feature ''{0}''")
    fun nonStandardFeatureUsed(feature: String?): ExInst<CalciteException?>?

    @BaseMessage("SELECT DISTINCT not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_E051_01(): Feature?

    @BaseMessage("EXCEPT not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_E071_03(): Feature?

    @BaseMessage("UPDATE not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_E101_03(): Feature?

    @BaseMessage("Transactions not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_E151(): Feature?

    @BaseMessage("INTERSECT not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_F302(): Feature?

    @BaseMessage("MERGE not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_F312(): Feature?

    @BaseMessage("Basic multiset not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_S271(): Feature?

    @BaseMessage("TABLESAMPLE not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    fun sQLFeature_T613(): Feature?

    @BaseMessage("Execution of a new autocommit statement while a cursor is still open on same connection is not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    fun sQLConformance_MultipleActiveAutocommitStatements(): ExInst<CalciteException?>?

    @BaseMessage("Descending sort (ORDER BY DESC) not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    fun sQLConformance_OrderByDesc(): Feature?

    @BaseMessage("Sharing of cached statement plans not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    fun sharedStatementPlans(): ExInst<CalciteException?>?

    @BaseMessage("TABLESAMPLE SUBSTITUTE not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    fun sQLFeatureExt_T613_Substitution(): Feature?

    @BaseMessage("Personality does not maintain table''s row count in the catalog")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    fun personalityManagesRowCount(): ExInst<CalciteException?>?

    @BaseMessage("Personality does not support snapshot reads")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    fun personalitySupportsSnapshots(): ExInst<CalciteException?>?

    @BaseMessage("Personality does not support labels")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    fun personalitySupportsLabels(): ExInst<CalciteException?>?

    @BaseMessage("Require at least 1 argument")
    fun requireAtLeastOneArg(): ExInst<SqlValidatorException?>?

    @BaseMessage("Map requires at least 2 arguments")
    fun mapRequiresTwoOrMoreArgs(): ExInst<SqlValidatorException?>?

    @BaseMessage("Map requires an even number of arguments")
    fun mapRequiresEvenArgCount(): ExInst<SqlValidatorException?>?

    @BaseMessage("Incompatible types")
    fun incompatibleTypes(): ExInst<SqlValidatorException?>?

    @BaseMessage("Number of columns must match number of query columns")
    fun columnCountMismatch(): ExInst<SqlValidatorException?>?

    @BaseMessage("Column has duplicate column name ''{0}'' and no column list specified")
    fun duplicateColumnAndNoColumnList(s: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Declaring class ''{0}'' of non-static user-defined function must have a public constructor with zero parameters")
    fun requireDefaultConstructor(className: String?): ExInst<RuntimeException?>?

    @BaseMessage("In user-defined aggregate class ''{0}'', first parameter to ''add'' method must be the accumulator (the return type of the ''init'' method)")
    fun firstParameterOfAdd(className: String?): ExInst<RuntimeException?>?

    @BaseMessage("FilterableTable.scan returned a filter that was not in the original list: {0}")
    fun filterableTableInventedFilter(s: String?): ExInst<CalciteException?>?

    @BaseMessage("FilterableTable.scan must not return null")
    fun filterableTableScanReturnedNull(): ExInst<CalciteException?>?

    @BaseMessage("Cannot convert table ''{0}'' to stream")
    fun cannotConvertToStream(tableName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot convert stream ''{0}'' to relation")
    fun cannotConvertToRelation(tableName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Streaming aggregation requires at least one monotonic expression in GROUP BY clause")
    fun streamMustGroupByMonotonic(): ExInst<SqlValidatorException?>?

    @BaseMessage("Streaming ORDER BY must start with monotonic expression")
    fun streamMustOrderByMonotonic(): ExInst<SqlValidatorException?>?

    @BaseMessage("Set operator cannot combine streaming and non-streaming inputs")
    fun streamSetOpInconsistentInputs(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot stream VALUES")
    fun cannotStreamValues(): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot resolve ''{0}''; it references view ''{1}'', whose definition is cyclic")
    fun cyclicDefinition(id: String?, view: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Modifiable view must be based on a single table")
    fun modifiableViewMustBeBasedOnSingleTable(): ExInst<SqlValidatorException?>?

    @BaseMessage("Modifiable view must be predicated only on equality expressions")
    fun modifiableViewMustHaveOnlyEqualityPredicates(): ExInst<SqlValidatorException?>?

    @BaseMessage("View is not modifiable. More than one expression maps to column ''{0}'' of base table ''{1}''")
    fun moreThanOneMappedColumn(columnName: String?, tableName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("View is not modifiable. No value is supplied for NOT NULL column ''{0}'' of base table ''{1}''")
    fun noValueSuppliedForViewColumn(columnName: String?, tableName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Modifiable view constraint is not satisfied for column ''{0}'' of base table ''{1}''")
    fun viewConstraintNotSatisfied(columnName: String?, tableName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Not a record type. The ''*'' operator requires a record")
    fun starRequiresRecordType(): ExInst<SqlValidatorException?>?

    @BaseMessage("FILTER expression must be of type BOOLEAN")
    fun filterMustBeBoolean(): ExInst<CalciteException?>?

    @BaseMessage("Cannot stream results of a query with no streaming inputs: ''{0}''. At least one input should be convertible to a stream")
    fun cannotStreamResultsForNonStreamingInputs(inputs: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("MINUS is not allowed under the current SQL conformance level")
    fun minusNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("SELECT must have a FROM clause")
    fun selectMissingFrom(): ExInst<SqlValidatorException?>?

    @BaseMessage("Group function ''{0}'' can only appear in GROUP BY clause")
    fun groupFunctionMustAppearInGroupByClause(funcName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Call to auxiliary group function ''{0}'' must have matching call to group function ''{1}'' in GROUP BY clause")
    fun auxiliaryWithoutMatchingGroupCall(func1: String?, func2: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Measure expression in PIVOT must use aggregate function")
    fun pivotAggMalformed(): ExInst<SqlValidatorException?>?

    @BaseMessage("Value count in PIVOT ({0,number,#}) must match number of FOR columns ({1,number,#})")
    fun pivotValueArityMismatch(valueCount: Int, forCount: Int): ExInst<SqlValidatorException?>?

    @BaseMessage("Duplicate column name ''{0}'' in UNPIVOT")
    fun unpivotDuplicate(columnName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Value count in UNPIVOT ({0,number,#}) must match number of FOR columns ({1,number,#})")
    fun unpivotValueArityMismatch(valueCount: Int, forCount: Int): ExInst<SqlValidatorException?>?

    @BaseMessage("In UNPIVOT, cannot derive type for measure ''{0}'' because source columns have different data types")
    fun unpivotCannotDeriveMeasureType(measureName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("In UNPIVOT, cannot derive type for axis ''{0}''")
    fun unpivotCannotDeriveAxisType(axisName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Pattern variable ''{0}'' has already been defined")
    fun patternVarAlreadyDefined(varName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot use PREV/NEXT in MEASURE ''{0}''")
    fun patternPrevFunctionInMeasure(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot nest PREV/NEXT under LAST/FIRST ''{0}''")
    fun patternPrevFunctionOrder(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot use aggregation in navigation ''{0}''")
    fun patternAggregationInNavigation(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Invalid number of parameters to COUNT method")
    fun patternCountFunctionArg(): ExInst<SqlValidatorException?>?

    @BaseMessage("The system time period specification expects Timestamp type but is ''{0}''")
    fun illegalExpressionForTemporal(type: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Table ''{0}'' is not a temporal table, can not be queried in system time period specification")
    fun notTemporalTable(tableName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Cannot use RUNNING/FINAL in DEFINE ''{0}''")
    fun patternRunningFunctionInDefine(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Multiple pattern variables in ''{0}''")
    fun patternFunctionVariableCheck(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Function ''{0}'' can only be used in MATCH_RECOGNIZE")
    fun functionMatchRecognizeOnly(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Null parameters in ''{0}''")
    fun patternFunctionNullCheck(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Unknown pattern ''{0}''")
    fun unknownPattern(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Interval must be non-negative ''{0}''")
    fun intervalMustBeNonNegative(call: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Must contain an ORDER BY clause when WITHIN is used")
    fun cannotUseWithinWithoutOrderBy(): ExInst<SqlValidatorException?>?

    @BaseMessage("First column of ORDER BY must be of type TIMESTAMP")
    fun firstColumnOfOrderByMustBeTimestamp(): ExInst<SqlValidatorException?>?

    @BaseMessage("Extended columns not allowed under the current SQL conformance level")
    fun extendNotAllowed(): ExInst<SqlValidatorException?>?

    @BaseMessage("Rolled up column ''{0}'' is not allowed in {1}")
    fun rolledUpNotAllowed(column: String?, context: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Schema ''{0}'' already exists")
    fun schemaExists(name: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Invalid schema type ''{0}''; valid values: {1}")
    fun schemaInvalidType(type: String?, values: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Table ''{0}'' already exists")
    fun tableExists(name: String?): ExInst<SqlValidatorException?>?

    // If CREATE TABLE does not have "AS query", there must be a column list
    @BaseMessage("Missing column list")
    fun createTableRequiresColumnList(): ExInst<SqlValidatorException?>?

    // If CREATE TABLE does not have "AS query", a type must be specified for each
    // column
    @BaseMessage("Type required for column ''{0}'' in CREATE TABLE without AS")
    fun createTableRequiresColumnTypes(columnName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("View ''{0}'' already exists and REPLACE not specified")
    fun viewExists(name: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Schema ''{0}'' not found")
    fun schemaNotFound(name: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("View ''{0}'' not found")
    fun viewNotFound(name: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Type ''{0}'' not found")
    fun typeNotFound(name: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Function ''{0}'' not found")
    fun functionNotFound(name: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Dialect does not support feature: ''{0}''")
    fun dialectDoesNotSupportFeature(featureName: String?): ExInst<SqlValidatorException?>?

    @BaseMessage("Substring error: negative substring length not allowed")
    fun illegalNegativeSubstringLength(): ExInst<CalciteException?>?

    @BaseMessage("Trim error: trim character must be exactly 1 character")
    fun trimError(): ExInst<CalciteException?>?

    @BaseMessage("Invalid types for arithmetic: {0} {1} {2}")
    fun invalidTypesForArithmetic(
        clazzName0: String?, op: String?,
        clazzName1: String?
    ): ExInst<CalciteException?>?

    @BaseMessage("Invalid types for comparison: {0} {1} {2}")
    fun invalidTypesForComparison(
        clazzName0: String?, op: String?,
        clazzName1: String?
    ): ExInst<CalciteException?>?

    @BaseMessage("Cannot convert {0} to {1}")
    fun cannotConvert(o: String?, toType: String?): ExInst<CalciteException?>?

    @BaseMessage("Invalid character for cast: {0}")
    fun invalidCharacterForCast(s: String?): ExInst<CalciteException?>?

    @BaseMessage("More than one value in list: {0}")
    fun moreThanOneValueInList(list: String?): ExInst<CalciteException?>?

    @BaseMessage("Failed to access field ''{0}'', index {1,number,#} of object of type {2}")
    fun failedToAccessField(
        @Nullable fieldName: String?, fieldIndex: Int, typeName: String?
    ): ExInstWithCause<CalciteException?>?

    @BaseMessage("Illegal jsonpath spec ''{0}'', format of the spec should be: ''<lax|strict> $'{'expr'}'''")
    fun illegalJsonPathSpec(pathSpec: String?): ExInst<CalciteException?>?

    @BaseMessage("Illegal jsonpath mode ''{0}''")
    fun illegalJsonPathMode(pathMode: String?): ExInst<CalciteException?>?

    @BaseMessage("Illegal jsonpath mode ''{0}'' in jsonpath spec: ''{1}''")
    fun illegalJsonPathModeInPathSpec(pathMode: String?, pathSpec: String?): ExInst<CalciteException?>?

    @BaseMessage("Strict jsonpath mode requires a non empty returned value, but is null")
    fun strictPathModeRequiresNonEmptyValue(): ExInst<CalciteException?>?

    @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_EXISTS function")
    fun illegalErrorBehaviorInJsonExistsFunc(errorBehavior: String?): ExInst<CalciteException?>?

    @BaseMessage("Empty result of JSON_VALUE function is not allowed")
    fun emptyResultOfJsonValueFuncNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("Illegal empty behavior ''{0}'' specified in JSON_VALUE function")
    fun illegalEmptyBehaviorInJsonValueFunc(emptyBehavior: String?): ExInst<CalciteException?>?

    @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_VALUE function")
    fun illegalErrorBehaviorInJsonValueFunc(errorBehavior: String?): ExInst<CalciteException?>?

    @BaseMessage("Strict jsonpath mode requires scalar value, and the actual value is: ''{0}''")
    fun scalarValueRequiredInStrictModeOfJsonValueFunc(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Illegal wrapper behavior ''{0}'' specified in JSON_QUERY function")
    fun illegalWrapperBehaviorInJsonQueryFunc(wrapperBehavior: String?): ExInst<CalciteException?>?

    @BaseMessage("Empty result of JSON_QUERY function is not allowed")
    fun emptyResultOfJsonQueryFuncNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("Illegal empty behavior ''{0}'' specified in JSON_VALUE function")
    fun illegalEmptyBehaviorInJsonQueryFunc(emptyBehavior: String?): ExInst<CalciteException?>?

    @BaseMessage("Strict jsonpath mode requires array or object value, and the actual value is: ''{0}''")
    fun arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_VALUE function")
    fun illegalErrorBehaviorInJsonQueryFunc(errorBehavior: String?): ExInst<CalciteException?>?

    @BaseMessage("Null key of JSON object is not allowed")
    fun nullKeyOfJsonObjectNotAllowed(): ExInst<CalciteException?>?

    @BaseMessage("Timeout of ''{0}'' ms for query execution is reached. Query execution started at ''{1}''")
    fun queryExecutionTimeoutReached(timeout: String?, queryStart: String?): ExInst<CalciteException?>?

    @BaseMessage("Including both WITHIN GROUP(...) and inside ORDER BY in a single JSON_ARRAYAGG call is not allowed")
    fun ambiguousSortOrderInJsonArrayAggFunc(): ExInst<CalciteException?>?

    @BaseMessage("While executing SQL [{0}] on JDBC sub-schema")
    fun exceptionWhilePerformingQueryOnJdbcSubSchema(sql: String?): ExInst<RuntimeException?>?

    @BaseMessage("Not a valid input for JSON_TYPE: ''{0}''")
    fun invalidInputForJsonType(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Not a valid input for JSON_DEPTH: ''{0}''")
    fun invalidInputForJsonDepth(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Cannot serialize object to JSON: ''{0}''")
    fun exceptionWhileSerializingToJson(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Not a valid input for JSON_LENGTH: ''{0}''")
    fun invalidInputForJsonLength(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Not a valid input for JSON_KEYS: ''{0}''")
    fun invalidInputForJsonKeys(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Invalid input for JSON_REMOVE: document: ''{0}'', jsonpath expressions: ''{1}''")
    fun invalidInputForJsonRemove(value: String?, pathSpecs: String?): ExInst<CalciteException?>?

    @BaseMessage("Not a valid input for JSON_STORAGE_SIZE: ''{0}''")
    fun invalidInputForJsonStorageSize(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Not a valid input for REGEXP_REPLACE: ''{0}''")
    fun invalidInputForRegexpReplace(value: String?): ExInst<CalciteException?>?

    @BaseMessage("Illegal xslt specified : ''{0}''")
    fun illegalXslt(xslt: String?): ExInst<CalciteException?>?

    @BaseMessage("Invalid input for XMLTRANSFORM xml: ''{0}''")
    fun invalidInputForXmlTransform(xml: String?): ExInst<CalciteException?>?

    @BaseMessage("Invalid input for EXTRACT xpath: ''{0}'', namespace: ''{1}''")
    fun invalidInputForExtractXml(xpath: String?, @Nullable namespace: String?): ExInst<CalciteException?>?

    @BaseMessage("Invalid input for EXISTSNODE xpath: ''{0}'', namespace: ''{1}''")
    fun invalidInputForExistsNode(xpath: String?, @Nullable namespace: String?): ExInst<CalciteException?>?

    @BaseMessage("Invalid input for EXTRACTVALUE: xml: ''{0}'', xpath expression: ''{1}''")
    fun invalidInputForExtractValue(xml: String?, xpath: String?): ExInst<CalciteException?>?

    @BaseMessage("Different length for bitwise operands: the first: {0,number,#}, the second: {1,number,#}")
    fun differentLengthForBitwiseOperands(l0: Int, l1: Int): ExInst<CalciteException?>?

    @BaseMessage("No operator for ''{0}'' with kind: ''{1}'', syntax: ''{2}'' during JSON deserialization")
    fun noOperator(name: String?, kind: String?, syntax: String?): ExInst<CalciteException?>?
}
