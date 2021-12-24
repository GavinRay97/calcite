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

import org.apache.calcite.avatica.util.ByteString

/**
 * Contains utility functions related to SQL parsing, all static.
 */
object SqlUtil {
    //~ Constants --------------------------------------------------------------
    /** Prefix for generated column aliases. Ends with '$' so that human-written
     * queries are unlikely to accidentally reference the generated name.  */
    const val GENERATED_EXPR_ALIAS_PREFIX = "EXPR$"
    //~ Methods ----------------------------------------------------------------
    /** Returns the AND of two expressions.
     *
     *
     * If `node1` is null, returns `node2`.
     * Flattens if either node is an AND.  */
    fun andExpressions(
        @Nullable node1: SqlNode?,
        node2: SqlNode
    ): SqlNode {
        if (node1 == null) {
            return node2
        }
        val list: ArrayList<SqlNode> = ArrayList()
        if (node1.getKind() === SqlKind.AND) {
            list.addAll((node1 as SqlCall).getOperandList())
        } else {
            list.add(node1)
        }
        if (node2.getKind() === SqlKind.AND) {
            list.addAll((node2 as SqlCall).getOperandList())
        } else {
            list.add(node2)
        }
        return SqlStdOperatorTable.AND.createCall(
            SqlParserPos.ZERO,
            list
        )
    }

    fun flatten(node: SqlNode?): ArrayList<SqlNode> {
        val list: ArrayList<SqlNode> = ArrayList()
        flatten(node, list)
        return list
    }

    /**
     * Returns the `n`th (0-based) input to a join expression.
     */
    fun getFromNode(
        query: SqlSelect,
        ordinal: Int
    ): SqlNode {
        val from: SqlNode = query.getFrom()
        assert(from != null) { "from must not be null for $query" }
        val list: ArrayList<SqlNode> = flatten(from)
        return list.get(ordinal)
    }

    private fun flatten(
        node: SqlNode?,
        list: ArrayList<SqlNode>
    ) {
        when (node.getKind()) {
            JOIN -> {
                val join: SqlJoin? = node as SqlJoin?
                flatten(
                    join.getLeft(),
                    list
                )
                flatten(
                    join.getRight(),
                    list
                )
                return
            }
            AS -> {
                val call: SqlCall? = node as SqlCall?
                flatten(call.operand(0), list)
                return
            }
            else -> {
                list.add(node)
                return
            }
        }
    }

    /** Converts a SqlNode array to a SqlNodeList.  */
    fun toNodeList(operands: Array<SqlNode?>): SqlNodeList {
        val ret = SqlNodeList(SqlParserPos.ZERO)
        for (node in operands) {
            ret.add(node)
        }
        return ret
    }

    /**
     * Returns whether a node represents the NULL value.
     *
     *
     * Examples:
     *
     *
     *  * For [SqlLiteral] Unknown, returns false.
     *  * For `CAST(NULL AS *type*)`, returns true if `
     * allowCast` is true, false otherwise.
     *  * For `CAST(CAST(NULL AS *type*) AS *type*))`,
     * returns false.
     *
     */
    fun isNullLiteral(
        @Nullable node: SqlNode?,
        allowCast: Boolean
    ): Boolean {
        if (node is SqlLiteral) {
            val literal: SqlLiteral? = node as SqlLiteral?
            return if (literal.getTypeName() === SqlTypeName.NULL) {
                assert(null == literal.getValue())
                true
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as
                // NULL.
                false
            }
        }
        if (allowCast && node != null) {
            if (node.getKind() === SqlKind.CAST) {
                val call: SqlCall = node as SqlCall
                if (isNullLiteral(call.operand(0), false)) {
                    // node is "CAST(NULL as type)"
                    return true
                }
            }
        }
        return false
    }

    /**
     * Returns whether a node represents the NULL value or a series of nested
     * `CAST(NULL AS type)` calls. For example:
     * `isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1)))`
     * returns `true`.
     */
    fun isNull(node: SqlNode): Boolean {
        return (isNullLiteral(node, false)
                || node.getKind() === SqlKind.CAST
                && isNull((node as SqlCall).operand(0)))
    }

    /**
     * Returns whether a node is a literal.
     *
     *
     * Examples:
     *
     *
     *  * For `CAST(literal AS *type*)`, returns true if `
     * allowCast` is true, false otherwise.
     *  * For `CAST(CAST(literal AS *type*) AS *type*))`,
     * returns false.
     *
     *
     * @param node The node, never null.
     * @param allowCast whether to regard CAST(literal) as a literal
     * @return Whether the node is a literal
     */
    fun isLiteral(node: SqlNode?, allowCast: Boolean): Boolean {
        assert(node != null)
        if (node is SqlLiteral) {
            return true
        }
        return if (!allowCast) {
            false
        } else when (node.getKind()) {
            CAST ->       // "CAST(e AS type)" is literal if "e" is literal
                isLiteral((node as SqlCall?).operand(0), true)
            MAP_VALUE_CONSTRUCTOR, ARRAY_VALUE_CONSTRUCTOR -> (node as SqlCall?).getOperandList().stream()
                .allMatch { o -> isLiteral(o, true) }
            DEFAULT -> true // DEFAULT is always NULL
            else -> false
        }
    }

    /**
     * Returns whether a node is a literal.
     *
     *
     * Many constructs which require literals also accept `CAST(NULL AS
     * *type*)`. This method does not accept casts, so you should
     * call [.isNullLiteral] first.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal
     */
    fun isLiteral(node: SqlNode?): Boolean {
        return isLiteral(node, false)
    }

    /**
     * Returns whether a node is a literal chain which is used to represent a
     * continued string literal.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal chain
     */
    fun isLiteralChain(node: SqlNode?): Boolean {
        assert(node != null)
        return if (node is SqlCall) {
            val call: SqlCall? = node as SqlCall?
            call.getKind() === SqlKind.LITERAL_CHAIN
        } else {
            false
        }
    }

    @Deprecated // to be removed before 2.0
    fun unparseFunctionSyntax(
        operator: SqlOperator,
        writer: SqlWriter,
        call: SqlCall
    ) {
        unparseFunctionSyntax(operator, writer, call, false)
    }

    /**
     * Unparses a call to an operator that has function syntax.
     *
     * @param operator    The operator
     * @param writer      Writer
     * @param call        List of 0 or more operands
     * @param ordered     Whether argument list may end with ORDER BY
     */
    fun unparseFunctionSyntax(
        operator: SqlOperator,
        writer: SqlWriter, call: SqlCall, ordered: Boolean
    ) {
        if (operator is SqlFunction) {
            val function: SqlFunction = operator as SqlFunction
            if (function.getFunctionType().isSpecific()) {
                writer.keyword("SPECIFIC")
            }
            val id: SqlIdentifier = function.getSqlIdentifier()
            if (id == null) {
                writer.keyword(operator.getName())
            } else {
                unparseSqlIdentifierSyntax(writer, id, true)
            }
        } else {
            writer.print(operator.getName())
        }
        if (call.operandCount() === 0) {
            when (call.getOperator().getSyntax()) {
                FUNCTION_ID ->         // For example, the "LOCALTIME" function appears as "LOCALTIME"
                    // when it has 0 args, not "LOCALTIME()".
                    return
                FUNCTION_STAR, FUNCTION, ORDERED_FUNCTION -> {}
                else -> {}
            }
        }
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")")
        val quantifier: SqlLiteral = call.getFunctionQuantifier()
        if (quantifier != null) {
            quantifier.unparse(writer, 0, 0)
        }
        if (call.operandCount() === 0) {
            when (call.getOperator().getSyntax()) {
                FUNCTION_STAR -> writer.sep("*")
                else -> {}
            }
        }
        for (operand in call.getOperandList()) {
            if (ordered && operand is SqlNodeList) {
                writer.sep("ORDER BY")
            } else if (ordered && operand.getKind() === SqlKind.SEPARATOR) {
                writer.sep("SEPARATOR")
                (operand as SqlCall).operand(0).unparse(writer, 0, 0)
                continue
            } else {
                writer.sep(",")
            }
            operand.unparse(writer, 0, 0)
        }
        writer.endList(frame)
    }

    /**
     * Unparse a SqlIdentifier syntax.
     *
     * @param writer       Writer
     * @param identifier   SqlIdentifier
     * @param asFunctionID Whether this identifier comes from a SqlFunction
     */
    fun unparseSqlIdentifierSyntax(
        writer: SqlWriter,
        identifier: SqlIdentifier,
        asFunctionID: Boolean
    ) {
        val isUnquotedSimple = (identifier.isSimple()
                && !identifier.getParserPosition().isQuoted())
        val operator: SqlOperator? = if (isUnquotedSimple) SqlValidatorUtil.lookupSqlFunctionByID(
            SqlStdOperatorTable.instance(),
            identifier,
            null
        ) else null
        var unparsedAsFunc = false
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER)
        if (isUnquotedSimple && operator != null) {
            // Unparse conditions:
            // 1. If the identifier is quoted or is component, unparse as normal.
            // 2. If the identifier comes from a sql function, lookup in the
            // standard sql operator table to see if the function is a builtin,
            // unparse without quoting for builtins.

            // 3. If the identifier does not come from a function(resolved as a SqlIdentifier),
            // look up in the standard sql operator table to see if it is a function
            // with empty argument list, e.g. LOCALTIME, we should not quote
            // such identifier cause quoted `LOCALTIME` always represents a sql identifier.
            if (asFunctionID
                || operator.getSyntax() === SqlSyntax.FUNCTION_ID
            ) {
                writer.keyword(identifier.getSimple())
                unparsedAsFunc = true
            }
        }
        if (!unparsedAsFunc) {
            for (i in 0 until identifier.names.size()) {
                writer.sep(".")
                val name: String = identifier.names.get(i)
                val pos: SqlParserPos = identifier.getComponentParserPosition(i)
                if (name.equals("")) {
                    writer.print("*")
                    writer.setNeedWhitespace(true)
                } else {
                    writer.identifier(name, pos.isQuoted())
                }
            }
        }
        if (null != identifier.getCollation()) {
            identifier.getCollation().unparse(writer)
        }
        writer.endList(frame)
    }

    fun unparseBinarySyntax(
        operator: SqlOperator,
        call: SqlCall,
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() === 2)
        val frame: SqlWriter.Frame = writer.startList(
            if (operator is SqlSetOperator) SqlWriter.FrameTypeEnum.SETOP else SqlWriter.FrameTypeEnum.SIMPLE
        )
        call.operand(0).unparse(writer, leftPrec, operator.getLeftPrec())
        val needsSpace: Boolean = operator.needsSpace()
        writer.setNeedWhitespace(needsSpace)
        writer.sep(operator.getName())
        writer.setNeedWhitespace(needsSpace)
        call.operand(1).unparse(writer, operator.getRightPrec(), rightPrec)
        writer.endList(frame)
    }

    /**
     * Concatenates string literals.
     *
     *
     * This method takes an array of arguments, since pairwise concatenation
     * means too much string copying.
     *
     * @param lits an array of [SqlLiteral], not empty, all of the same
     * class
     * @return a new [SqlLiteral], of that same class, whose value is the
     * string concatenation of the values of the literals
     * @throws ClassCastException             if the lits are not homogeneous.
     * @throws ArrayIndexOutOfBoundsException if lits is an empty array.
     */
    fun concatenateLiterals(lits: List<SqlLiteral>): SqlLiteral {
        return if (lits.size() === 1) {
            lits[0] // nothing to do
        } else (lits[0] as SqlAbstractStringLiteral).concat1(lits)
    }

    /**
     * Looks up a (possibly overloaded) routine based on name and argument
     * types.
     *
     * @param opTab         operator table to search
     * @param typeFactory   Type factory
     * @param funcName      name of function being invoked
     * @param argTypes      argument types
     * @param argNames      argument names, or null if call by position
     * @param category      whether a function or a procedure. (If a procedure is
     * being invoked, the overload rules are simpler.)
     * @param nameMatcher   Whether to look up the function case-sensitively
     * @param coerce        Whether to allow type coercion when do filter routines
     * by parameter types
     * @return matching routine, or null if none found
     *
     * @see Glossary.SQL99 SQL:1999 Part 2 Section 10.4
     */
    @Nullable
    fun lookupRoutine(
        opTab: SqlOperatorTable,
        typeFactory: RelDataTypeFactory?,
        funcName: SqlIdentifier?, argTypes: List<RelDataType?>,
        @Nullable argNames: List<String?>?, @Nullable category: SqlFunctionCategory,
        syntax: SqlSyntax, sqlKind: SqlKind, nameMatcher: SqlNameMatcher?,
        coerce: Boolean
    ): SqlOperator? {
        val list: Iterator<SqlOperator> = lookupSubjectRoutines(
            opTab,
            typeFactory,
            funcName,
            argTypes,
            argNames,
            syntax,
            sqlKind,
            category,
            nameMatcher,
            coerce
        )
        return if (list.hasNext()) {
            // return first on schema path
            list.next()
        } else null
    }

    private fun filterOperatorRoutinesByKind(
        routines: Iterator<SqlOperator>, sqlKind: SqlKind
    ): Iterator<SqlOperator> {
        return Iterators.filter(
            routines
        ) { operator -> Objects.requireNonNull(operator, "operator").getKind() === sqlKind }
    }

    /**
     * Looks up all subject routines matching the given name and argument types.
     *
     * @param opTab       operator table to search
     * @param typeFactory Type factory
     * @param funcName    name of function being invoked
     * @param argTypes    argument types
     * @param argNames    argument names, or null if call by position
     * @param sqlSyntax   the SqlSyntax of the SqlOperator being looked up
     * @param sqlKind     the SqlKind of the SqlOperator being looked up
     * @param category    Category of routine to look up
     * @param nameMatcher Whether to look up the function case-sensitively
     * @param coerce      Whether to allow type coercion when do filter routine
     * by parameter types
     * @return list of matching routines
     * @see Glossary.SQL99 SQL:1999 Part 2 Section 10.4
     */
    fun lookupSubjectRoutines(
        opTab: SqlOperatorTable, typeFactory: RelDataTypeFactory?,
        funcName: SqlIdentifier?, argTypes: List<RelDataType?>, @Nullable argNames: List<String?>?,
        sqlSyntax: SqlSyntax, sqlKind: SqlKind,
        @Nullable category: SqlFunctionCategory, nameMatcher: SqlNameMatcher?,
        coerce: Boolean
    ): Iterator<SqlOperator> {
        // start with all routines matching by name
        var routines: Iterator<SqlOperator> = lookupSubjectRoutinesByName(
            opTab, funcName, sqlSyntax, category,
            nameMatcher
        )

        // first pass:  eliminate routines which don't accept the given
        // number of arguments
        routines = filterRoutinesByParameterCount(routines, argTypes)

        // NOTE: according to SQL99, procedures are NOT overloaded on type,
        // only on number of arguments.
        if (category === SqlFunctionCategory.USER_DEFINED_PROCEDURE) {
            return routines
        }

        // second pass:  eliminate routines which don't accept the given
        // argument types and parameter names if specified
        routines = filterRoutinesByParameterTypeAndName(
            typeFactory, sqlSyntax, routines,
            argTypes, argNames, coerce
        )

        // see if we can stop now; this is necessary for the case
        // of builtin functions where we don't have param type info,
        // or UDF whose operands can make type coercion.
        val list: List<SqlOperator> = Lists.newArrayList(routines)
        routines = list.iterator()
        if (list.size() < 2 || coerce) {
            return routines
        }

        // third pass:  for each parameter from left to right, eliminate
        // all routines except those with the best precedence match for
        // the given arguments
        routines = filterRoutinesByTypePrecedence(sqlSyntax, typeFactory, routines, argTypes, argNames)

        // fourth pass: eliminate routines which do not have the same
        // SqlKind as requested
        return filterOperatorRoutinesByKind(routines, sqlKind)
    }

    /**
     * Determines whether there is a routine matching the given name and number
     * of arguments.
     *
     * @param opTab         operator table to search
     * @param funcName      name of function being invoked
     * @param argTypes      argument types
     * @param category      category of routine to look up
     * @param nameMatcher   Whether to look up the function case-sensitively
     * @return true if match found
     */
    fun matchRoutinesByParameterCount(
        opTab: SqlOperatorTable,
        funcName: SqlIdentifier?,
        argTypes: List<RelDataType?>,
        category: SqlFunctionCategory,
        nameMatcher: SqlNameMatcher?
    ): Boolean {
        // start with all routines matching by name
        var routines: Iterator<SqlOperator> = lookupSubjectRoutinesByName(
            opTab, funcName, SqlSyntax.FUNCTION,
            category, nameMatcher
        )

        // first pass:  eliminate routines which don't accept the given
        // number of arguments
        routines = filterRoutinesByParameterCount(routines, argTypes)
        return routines.hasNext()
    }

    private fun lookupSubjectRoutinesByName(
        opTab: SqlOperatorTable,
        funcName: SqlIdentifier?,
        syntax: SqlSyntax,
        @Nullable category: SqlFunctionCategory,
        nameMatcher: SqlNameMatcher?
    ): Iterator<SqlOperator> {
        val sqlOperators: List<SqlOperator> = ArrayList()
        opTab.lookupOperatorOverloads(
            funcName, category, syntax, sqlOperators,
            nameMatcher
        )
        return when (syntax) {
            FUNCTION -> Iterators.filter(
                sqlOperators.iterator(),
                Predicates.instanceOf(SqlFunction::class.java)
            )
            else -> Iterators.filter(
                sqlOperators.iterator()
            ) { operator -> Objects.requireNonNull(operator, "operator").getSyntax() === syntax }
        }
    }

    private fun filterRoutinesByParameterCount(
        routines: Iterator<SqlOperator>,
        argTypes: List<RelDataType?>
    ): Iterator<SqlOperator> {
        return Iterators.filter(
            routines
        ) { operator ->
            Objects.requireNonNull(operator, "operator")
                .getOperandCountRange().isValidCount(argTypes.size())
        }
    }

    /**
     * Filters an iterator of routines, keeping only those that have the required
     * argument types and names.
     *
     * @see Glossary.SQL99 SQL:1999 Part 2 Section 10.4 Syntax Rule 6.b.iii.2.B
     */
    private fun filterRoutinesByParameterTypeAndName(
        typeFactory: RelDataTypeFactory?, syntax: SqlSyntax,
        routines: Iterator<SqlOperator>, argTypes: List<RelDataType?>,
        @Nullable argNames: List<String?>?, coerce: Boolean
    ): Iterator<SqlOperator> {
        return if (syntax !== SqlSyntax.FUNCTION) {
            routines
        } else Iterators.filter(
            Iterators.filter(routines, SqlFunction::class.java)
        ) { function ->
            val operandTypeChecker: SqlOperandTypeChecker =
                Objects.requireNonNull(function, "function").getOperandTypeChecker()
            if (operandTypeChecker == null
                || !operandTypeChecker.isFixedParameters()
            ) {
                // no parameter information for builtins; keep for now,
                // the type coerce will not work here.
                return@filter true
            }
            val operandMetadata: SqlOperandMetadata = operandTypeChecker as SqlOperandMetadata
            @SuppressWarnings("assignment.type.incompatible") val paramTypes: List<RelDataType> =
                operandMetadata.paramTypes(typeFactory)
            val permutedArgTypes: List<RelDataType>?
            if (argNames != null) {
                val paramNames: List<String> = operandMetadata.paramNames()
                permutedArgTypes = permuteArgTypes(paramNames, argNames, argTypes)
                if (permutedArgTypes == null) {
                    return@filter false
                }
            } else {
                permutedArgTypes = Lists.newArrayList(argTypes)
                while (permutedArgTypes.size() < argTypes.size()) {
                    paramTypes.add(null)
                }
            }
            for (p in Pair.zip(paramTypes, permutedArgTypes)) {
                val argType: RelDataType = p.right
                val paramType: RelDataType = p.left
                if (argType != null && paramType != null && !SqlTypeUtil.canCastFrom(paramType, argType, coerce)) {
                    return@filter false
                }
            }
            true
        }
    }

    /**
     * Permutes argument types to correspond to the order of parameter names.
     */
    @Nullable
    private fun permuteArgTypes(
        paramNames: List<String>,
        argNames: List<String?>, argTypes: List<RelDataType?>
    ): List<RelDataType>? {
        // Arguments passed by name. Make sure that the function has
        // parameters of all of these names.
        val map: Map<Integer, Integer> = HashMap()
        for (argName in Ord.zip(argNames)) {
            val i = paramNames.indexOf(argName.e)
            if (i < 0) {
                return null
            }
            map.put(i, argName.i)
        }
        return Functions.< RelDataType > generate < RelDataType ? > paramNames.size(), { index ->
            val argIndex: Integer? = map[index]
            if (argIndex != null) argTypes[argIndex] else null
        })
    }

    /**
     * Filters an iterator of routines, keeping only those with the best match for
     * the actual argument types.
     *
     * @see Glossary.SQL99 SQL:1999 Part 2 Section 9.4
     */
    private fun filterRoutinesByTypePrecedence(
        sqlSyntax: SqlSyntax,
        typeFactory: RelDataTypeFactory?,
        routines: Iterator<SqlOperator>,
        argTypes: List<RelDataType?>,
        @Nullable argNames: List<String?>?
    ): Iterator<SqlOperator> {
        if (sqlSyntax !== SqlSyntax.FUNCTION) {
            return routines
        }
        var sqlFunctions: List<SqlFunction> = Lists.newArrayList(Iterators.filter(routines, SqlFunction::class.java))
        for (argType in Ord.zip(argTypes)) {
            val precList: RelDataTypePrecedenceList = argType.e.getPrecedenceList()
            val bestMatch: RelDataType? = bestMatch(typeFactory, sqlFunctions, argType.i, argNames, precList)
            if (bestMatch != null) {
                sqlFunctions = sqlFunctions.stream()
                    .filter { function ->
                        val operandTypeChecker: SqlOperandTypeChecker = function.getOperandTypeChecker()
                        if (operandTypeChecker == null || !operandTypeChecker.isFixedParameters()) {
                            return@filter false
                        }
                        val operandMetadata: SqlOperandMetadata = operandTypeChecker as SqlOperandMetadata
                        val paramNames: List<String?> = operandMetadata.paramNames()
                        val paramTypes: List<RelDataType> = operandMetadata.paramTypes(typeFactory)
                        val index = if (argNames != null) paramNames.indexOf(argNames[argType.i]) else argType.i
                        val paramType: RelDataType = paramTypes[index]
                        precList.compareTypePrecedence(paramType, bestMatch) >= 0
                    }
                    .collect(Collectors.toList())
            }
        }
        return sqlFunctions.iterator()
    }

    @Nullable
    private fun bestMatch(
        typeFactory: RelDataTypeFactory?,
        sqlFunctions: List<SqlFunction>, i: Int,
        @Nullable argNames: List<String?>?, precList: RelDataTypePrecedenceList
    ): RelDataType? {
        var bestMatch: RelDataType? = null
        for (function in sqlFunctions) {
            val operandTypeChecker: SqlOperandTypeChecker = function.getOperandTypeChecker()
            if (operandTypeChecker == null || !operandTypeChecker.isFixedParameters()) {
                continue
            }
            val operandMetadata: SqlOperandMetadata = operandTypeChecker as SqlOperandMetadata
            val paramTypes: List<RelDataType> = operandMetadata.paramTypes(typeFactory)
            val paramNames: List<String?> = operandMetadata.paramNames()
            val paramType: RelDataType =
                if (argNames != null) paramTypes[paramNames.indexOf(argNames[i])] else paramTypes[i]
            if (bestMatch == null) {
                bestMatch = paramType
            } else {
                val c: Int = precList.compareTypePrecedence(
                    bestMatch,
                    paramType
                )
                if (c < 0) {
                    bestMatch = paramType
                }
            }
        }
        return bestMatch
    }

    /**
     * Returns the `i`th select-list item of a query.
     */
    fun getSelectListItem(query: SqlNode, i: Int): SqlNode {
        var i = i
        return when (query.getKind()) {
            SELECT -> {
                val select: SqlSelect = query as SqlSelect
                val from: SqlNode? = stripAs(select.getFrom())
                if (from != null && from.getKind() === SqlKind.VALUES) {
                    // They wrote "VALUES (x, y)", but the validator has
                    // converted this into "SELECT * FROM VALUES (x, y)".
                    return getSelectListItem(from, i)
                }
                val fields: SqlNodeList = select.getSelectList()
                assert(fields != null) { "fields must not be null in $select" }
                // Range check the index to avoid index out of range.  This
                // could be expanded to actually check to see if the select
                // list is a "*"
                if (i >= fields.size()) {
                    i = 0
                }
                fields.get(i)
            }
            VALUES -> {
                val call: SqlCall = query as SqlCall
                assert(call.operandCount() > 0) { "VALUES must have at least one operand" }
                val row: SqlCall = call.operand(0)
                assert(row.operandCount() > i) { "VALUES has too few columns" }
                row.operand(i)
            }
            else -> throw Util.needToImplement(query)
        }
    }

    fun deriveAliasFromOrdinal(ordinal: Int): String {
        return GENERATED_EXPR_ALIAS_PREFIX + ordinal
    }

    /**
     * Whether the alias is generated by calcite.
     * @param alias not null
     * @return true if alias is generated by calcite, otherwise false
     */
    fun isGeneratedAlias(alias: String?): Boolean {
        assert(alias != null)
        return alias.toUpperCase(Locale.ROOT).startsWith(GENERATED_EXPR_ALIAS_PREFIX)
    }

    /**
     * Constructs an operator signature from a type list.
     *
     * @param op       operator
     * @param typeList list of types to use for operands. Types may be
     * represented as [String], [SqlTypeFamily], or
     * any object with a valid [Object.toString] method.
     * @return constructed signature
     */
    fun getOperatorSignature(op: SqlOperator, typeList: List<*>): String {
        return getAliasedSignature(op, op.getName(), typeList)
    }

    /**
     * Constructs an operator signature from a type list, substituting an alias
     * for the operator name.
     *
     * @param op       operator
     * @param opName   name to use for operator
     * @param typeList list of [SqlTypeName] or [String] to use for
     * operands
     * @return constructed signature
     */
    fun getAliasedSignature(
        op: SqlOperator,
        opName: String?,
        typeList: List<*>
    ): String {
        val ret = StringBuilder()
        val template: String = op.getSignatureTemplate(typeList.size())
        if (null == template) {
            ret.append("'")
            ret.append(opName)
            ret.append("(")
            for (i in 0 until typeList.size()) {
                if (i > 0) {
                    ret.append(", ")
                }
                val t: String = String.valueOf(typeList[i]).toUpperCase(Locale.ROOT)
                ret.append("<").append(t).append(">")
            }
            ret.append(")'")
        } else {
            val values: Array<Object?> = arrayOfNulls<Object>(typeList.size() + 1)
            values[0] = opName
            ret.append("'")
            for (i in 0 until typeList.size()) {
                val t: String = String.valueOf(typeList[i]).toUpperCase(Locale.ROOT)
                values[i + 1] = "<$t>"
            }
            ret.append(MessageFormat(template, Locale.ROOT).format(values))
            ret.append("'")
            assert(typeList.size() + 1 === values.size)
        }
        return ret.toString()
    }

    /**
     * Wraps an exception with context.
     */
    fun newContextException(
        pos: SqlParserPos,
        e: Resources.ExInst<*>,
        inputText: String?
    ): CalciteException {
        val ex: CalciteContextException = newContextException(pos, e)
        ex.setOriginalStatement(inputText)
        return ex
    }

    /**
     * Wraps an exception with context.
     */
    fun newContextException(
        pos: SqlParserPos,
        e: Resources.ExInst<*>
    ): CalciteContextException {
        val line: Int = pos.getLineNum()
        val col: Int = pos.getColumnNum()
        val endLine: Int = pos.getEndLineNum()
        val endCol: Int = pos.getEndColumnNum()
        return newContextException(line, col, endLine, endCol, e)
    }

    /**
     * Wraps an exception with context.
     */
    fun newContextException(
        line: Int,
        col: Int,
        endLine: Int,
        endCol: Int,
        e: Resources.ExInst<*>
    ): CalciteContextException {
        val contextExcn: CalciteContextException =
            (if (line == endLine && col == endCol) RESOURCE.validatorContextPoint(
                line,
                col
            ) else RESOURCE.validatorContext(line, col, endLine, endCol)).ex(e.ex())
        contextExcn.setPosition(line, col, endLine, endCol)
        return contextExcn
    }

    /**
     * Returns whether a [node][SqlNode] is a [call][SqlCall] to a
     * given [operator][SqlOperator].
     */
    fun isCallTo(node: SqlNode, operator: SqlOperator): Boolean {
        return (node is SqlCall
                && (node as SqlCall).getOperator() === operator)
    }

    /**
     * Creates the type of an [org.apache.calcite.util.NlsString].
     *
     *
     * The type inherits the The NlsString's [Charset] and
     * [SqlCollation], if they are set, otherwise it gets the system
     * defaults.
     *
     * @param typeFactory Type factory
     * @param str         String
     * @return Type, including collation and charset
     */
    fun createNlsStringType(
        typeFactory: RelDataTypeFactory,
        str: NlsString
    ): RelDataType {
        var charset: Charset = str.getCharset()
        if (null == charset) {
            charset = typeFactory.getDefaultCharset()
        }
        var collation: SqlCollation = str.getCollation()
        if (null == collation) {
            collation = SqlCollation.COERCIBLE
        }
        var type: RelDataType = typeFactory.createSqlType(
            SqlTypeName.CHAR,
            str.getValue().length()
        )
        type = typeFactory.createTypeWithCharsetAndCollation(
            type,
            charset,
            collation
        )
        return type
    }

    /**
     * Translates a character set name from a SQL-level name into a Java-level
     * name.
     *
     * @param name SQL-level name
     * @return Java-level name, or null if SQL-level name is unknown
     */
    @Nullable
    fun translateCharacterSetName(name: String?): String? {
        return when (name) {
            "BIG5" -> "Big5"
            "LATIN1" -> "ISO-8859-1"
            "UTF8" -> "UTF-8"
            "UTF16", "UTF-16" -> ConversionUtil.NATIVE_UTF16_CHARSET_NAME
            "GB2312", "GBK", "UTF-16BE", "UTF-16LE", "ISO-8859-1", "UTF-8" -> name
            else -> null
        }
    }

    /**
     * Returns the Java-level [Charset] based on given SQL-level name.
     *
     * @param charsetName Sql charset name, must not be null.
     * @return charset, or default charset if charsetName is null.
     * @throws UnsupportedCharsetException If no support for the named charset
     * is available in this instance of the Java virtual machine
     */
    fun getCharset(charsetName: String?): Charset {
        var charsetName = charsetName
        assert(charsetName != null)
        charsetName = charsetName.toUpperCase(Locale.ROOT)
        val javaCharsetName = translateCharacterSetName(charsetName)
            ?: throw UnsupportedCharsetException(charsetName)
        return Charset.forName(javaCharsetName)
    }

    /**
     * Validate if value can be decoded by given charset.
     *
     * @param value nls string in byte array
     * @param charset charset
     * @throws RuntimeException If the given value cannot be represented in the
     * given charset
     */
    @SuppressWarnings("BetaApi")
    fun validateCharset(value: ByteString, charset: Charset) {
        if (charset === StandardCharsets.UTF_8) {
            val bytes: ByteArray = value.getBytes()
            if (!Utf8.isWellFormed(bytes)) {
                //CHECKSTYLE: IGNORE 1
                val string = String(bytes, charset)
                throw RESOURCE.charsetEncoding(string, charset.name()).ex()
            }
        }
    }

    /** If a node is "AS", returns the underlying expression; otherwise returns
     * the node. Returns null if and only if the node is null.  */
    @PolyNull
    fun stripAs(@PolyNull node: SqlNode?): SqlNode? {
        return if (node != null && node.getKind() === SqlKind.AS) {
            (node as SqlCall).operand(0)
        } else node
    }

    /** Modifies a list of nodes, removing AS from each if present.
     *
     * @see .stripAs
     */
    fun stripListAs(nodeList: SqlNodeList): SqlNodeList {
        for (i in 0 until nodeList.size()) {
            val n: SqlNode = nodeList.get(i)
            val n2: SqlNode? = stripAs(n)
            if (n !== n2) {
                nodeList.set(i, n2)
            }
        }
        return nodeList
    }

    /** Returns a list of ancestors of `predicate` within a given
     * `SqlNode` tree.
     *
     *
     * The first element of the list is `root`, and the last is
     * the node that matched `predicate`. Throws if no node matches.
     */
    fun getAncestry(
        root: SqlNode,
        predicate: Predicate<SqlNode?>, postPredicate: Predicate<SqlNode?>
    ): ImmutableList<SqlNode> {
        return try {
            Genealogist(predicate, postPredicate).visitChild(root)
            throw AssertionError("not found: $predicate in $root")
        } catch (e: Util.FoundOne) {
            Objects.requireNonNull(
                e.getNode(),
                "Genealogist result"
            ) as ImmutableList<SqlNode>
        }
    }

    /**
     * Returns an immutable list of [RelHint] from sql hints, with a given
     * inherit path from the root node.
     *
     *
     * The inherit path would be empty list.
     *
     * @param hintStrategies The hint strategies to validate the sql hints
     * @param sqlHints       The sql hints nodes
     * @return the `RelHint` list
     */
    fun getRelHint(
        hintStrategies: HintStrategyTable,
        @Nullable sqlHints: SqlNodeList?
    ): List<RelHint> {
        if (sqlHints == null || sqlHints.size() === 0) {
            return ImmutableList.of()
        }
        val relHints: ImmutableList.Builder<RelHint> = ImmutableList.builder()
        for (node in sqlHints) {
            assert(node is SqlHint)
            val sqlHint: SqlHint = node as SqlHint
            val hintName: String = sqlHint.getName()
            val builder: RelHint.Builder = RelHint.builder(hintName)
            when (sqlHint.getOptionFormat()) {
                EMPTY -> {}
                LITERAL_LIST, ID_LIST -> builder.hintOptions(sqlHint.getOptionList())
                KV_LIST -> builder.hintOptions(sqlHint.getOptionKVPairs())
                else -> throw AssertionError("Unexpected hint option format")
            }
            val relHint: RelHint = builder.build()
            if (hintStrategies.validateHint(relHint)) {
                // Skips the hint if the validation fails.
                relHints.add(relHint)
            }
        }
        return relHints.build()
    }

    /**
     * Attach the `hints` to `rel` with specified hint strategies.
     *
     * @param hintStrategies The strategies to filter the hints
     * @param hints          The original hints to be attached
     * @return A copy of `rel` if there are any hints can be attached given
     * the hint strategies, or the original node if such hints don't exist
     */
    fun attachRelHint(
        hintStrategies: HintStrategyTable,
        hints: List<RelHint?>?,
        rel: Hintable
    ): RelNode {
        val relHints: List<RelHint> = hintStrategies.apply(hints, rel as RelNode)
        return if (relHints.size() > 0) {
            rel.attachHints(relHints)
        } else rel as RelNode
    }

    /** Creates a call to an operator.
     *
     *
     * Deals with the fact the AND and OR are binary.  */
    fun createCall(
        op: SqlOperator, pos: SqlParserPos,
        operands: List<SqlNode>
    ): SqlNode {
        when (op.kind) {
            OR, AND -> when (operands.size()) {
                0 -> return SqlLiteral.createBoolean(op.kind === SqlKind.AND, pos)
                1 -> return operands[0]
                2, 3, 4, 5 -> {}
                else -> return createBalancedCall(op, pos, operands, 0, operands.size())
            }
            else -> {}
        }
        return if (op is SqlBinaryOperator && operands.size() > 2) {
            createLeftCall(op, pos, operands)
        } else op.createCall(pos, operands)
    }

    private fun createLeftCall(
        op: SqlOperator, pos: SqlParserPos,
        nodeList: List<SqlNode>
    ): SqlNode {
        var node: SqlNode = op.createCall(pos, nodeList.subList(0, 2))
        for (i in 2 until nodeList.size()) {
            node = op.createCall(pos, node, nodeList[i])
        }
        return node
    }

    /**
     * Creates a balanced binary call from sql node list,
     * start inclusive, end exclusive.
     */
    private fun createBalancedCall(
        op: SqlOperator, pos: SqlParserPos,
        operands: List<SqlNode>, start: Int, end: Int
    ): SqlNode {
        assert(start < end && end <= operands.size())
        if (start + 1 == end) {
            return operands[start]
        }
        val mid = (end - start) / 2 + start
        val leftNode: SqlNode = createBalancedCall(op, pos, operands, start, mid)
        val rightNode: SqlNode = createBalancedCall(op, pos, operands, mid, end)
        return op.createCall(pos, leftNode, rightNode)
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Handles particular [DatabaseMetaData] methods; invocations of other
     * methods will fall through to the base class,
     * [org.apache.calcite.util.BarfingInvocationHandler], which will throw
     * an error.
     */
    class DatabaseMetaDataInvocationHandler(
        @get:Throws(SQLException::class) val databaseProductName: String,
        @get:Throws(SQLException::class) val identifierQuoteString: String
    ) : BarfingInvocationHandler()

    /** Walks over a [org.apache.calcite.sql.SqlNode] tree and returns the
     * ancestry stack when it finds a given node.  */
    private class Genealogist internal constructor(
        predicate: Predicate<SqlNode?>,
        postPredicate: Predicate<SqlNode?>
    ) : SqlBasicVisitor<Void?>() {
        private val ancestors: List<SqlNode> = ArrayList()
        private val predicate: Predicate<SqlNode>
        private val postPredicate: Predicate<SqlNode>

        init {
            this.predicate = predicate
            this.postPredicate = postPredicate
        }

        private fun check(node: SqlNode): Void? {
            preCheck(node)
            postCheck(node)
            return null
        }

        private fun preCheck(node: SqlNode): Void? {
            if (predicate.test(node)) {
                throw FoundOne(ImmutableList.copyOf(ancestors))
            }
            return null
        }

        private fun postCheck(node: SqlNode): Void? {
            if (postPredicate.test(node)) {
                throw FoundOne(ImmutableList.copyOf(ancestors))
            }
            return null
        }

        fun visitChild(@Nullable node: SqlNode?) {
            if (node == null) {
                return
            }
            ancestors.add(node)
            node.accept(this)
            ancestors.remove(ancestors.size() - 1)
        }

        @Override
        fun visit(id: SqlIdentifier): Void? {
            return check(id)
        }

        @Override
        fun visit(call: SqlCall): Void? {
            preCheck(call)
            for (node in call.getOperandList()) {
                visitChild(node)
            }
            return postCheck(call)
        }

        @Override
        fun visit(intervalQualifier: SqlIntervalQualifier): Void? {
            return check(intervalQualifier)
        }

        @Override
        fun visit(literal: SqlLiteral): Void? {
            return check(literal)
        }

        @Override
        fun visit(nodeList: SqlNodeList): Void? {
            preCheck(nodeList)
            for (node in nodeList) {
                visitChild(node)
            }
            return postCheck(nodeList)
        }

        @Override
        fun visit(param: SqlDynamicParam): Void? {
            return check(param)
        }

        @Override
        fun visit(type: SqlDataTypeSpec): Void? {
            return check(type)
        }
    }
}
