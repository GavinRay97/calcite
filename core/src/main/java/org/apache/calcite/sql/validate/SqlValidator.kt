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
package org.apache.calcite.sql.validate

import org.apache.calcite.config.NullCollation

/**
 * Validates the parse tree of a SQL statement, and provides semantic
 * information about the parse tree.
 *
 *
 * To create an instance of the default validator implementation, call
 * [SqlValidatorUtil.newValidator].
 *
 * <h2>Visitor pattern</h2>
 *
 *
 * The validator interface is an instance of the
 * [visitor pattern][org.apache.calcite.util.Glossary.VISITOR_PATTERN].
 * Implementations
 * of the [SqlNode.validate] method call the `validateXxx`
 * method appropriate to the kind of node:
 *
 *  * [SqlLiteral.validate]
 * calls
 * [.validateLiteral];
 *  * [SqlCall.validate]
 * calls
 * [.validateCall];
 *  * and so forth.
 *
 *
 * The [SqlNode.validateExpr] method
 * is as [SqlNode.validate] but is called
 * when the node is known to be a scalar expression.
 *
 * <h2>Scopes and namespaces</h2>
 *
 *
 * In order to resolve names to objects, the validator builds a map of the
 * structure of the query. This map consists of two types of objects. A
 * [SqlValidatorScope] describes the tables and columns accessible at a
 * particular point in the query; and a [SqlValidatorNamespace] is a
 * description of a data source used in a query.
 *
 *
 * There are different kinds of namespace for different parts of the query.
 * for example [IdentifierNamespace] for table names,
 * [SelectNamespace] for SELECT queries,
 * [SetopNamespace] for UNION, EXCEPT
 * and INTERSECT. A validator is allowed to wrap namespaces in other objects
 * which implement [SqlValidatorNamespace], so don't try to cast your
 * namespace or use `instanceof`; use
 * [SqlValidatorNamespace.unwrap] and
 * [SqlValidatorNamespace.isWrapperFor] instead.
 *
 *
 * The validator builds the map by making a quick scan over the query when
 * the root [SqlNode] is first provided. Thereafter, it supplies the
 * correct scope or namespace object when it calls validation methods.
 *
 *
 * The methods [.getSelectScope], [.getFromScope],
 * [.getWhereScope], [.getGroupScope], [.getHavingScope],
 * [.getOrderScope] and [.getJoinScope] get the correct scope
 * to resolve
 * names in a particular clause of a SQL statement.
 */
@Value.Enclosing
interface SqlValidator {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the catalog reader used by this validator.
     *
     * @return catalog reader
     */
    @get:Pure
    val catalogReader: org.apache.calcite.sql.validate.SqlValidatorCatalogReader

    /**
     * Returns the operator table used by this validator.
     *
     * @return operator table
     */
    @get:Pure
    val operatorTable: SqlOperatorTable

    /**
     * Validates an expression tree. You can call this method multiple times,
     * but not reentrantly.
     *
     * @param topNode top of expression tree to be validated
     * @return validated tree (possibly rewritten)
     */
    fun validate(topNode: SqlNode?): SqlNode

    /**
     * Validates an expression tree. You can call this method multiple times,
     * but not reentrantly.
     *
     * @param topNode       top of expression tree to be validated
     * @param nameToTypeMap map of simple name to [RelDataType]; used to
     * resolve [SqlIdentifier] references
     * @return validated tree (possibly rewritten)
     */
    fun validateParameterizedExpression(
        topNode: SqlNode?,
        nameToTypeMap: Map<String?, RelDataType?>?
    ): SqlNode?

    /**
     * Checks that a query is valid.
     *
     *
     * Valid queries include:
     *
     *
     *  * `SELECT` statement,
     *  * set operation (`UNION`, `INTERSECT`, `
     * EXCEPT`)
     *  * identifier (e.g. representing use of a table in a FROM clause)
     *  * query aliased with the `AS` operator
     *
     *
     * @param node  Query node
     * @param scope Scope in which the query occurs
     * @param targetRowType Desired row type, must not be null, may be the data
     * type 'unknown'.
     * @throws RuntimeException if the query is not valid
     */
    fun validateQuery(
        node: SqlNode?, @Nullable scope: SqlValidatorScope?,
        targetRowType: RelDataType?
    )

    /**
     * Returns the type assigned to a node by validation.
     *
     * @param node the node of interest
     * @return validated type, never null
     */
    fun getValidatedNodeType(node: SqlNode?): RelDataType

    /**
     * Returns the type assigned to a node by validation, or null if unknown.
     * This allows for queries against nodes such as aliases, which have no type
     * of their own. If you want to assert that the node of interest must have a
     * type, use [.getValidatedNodeType] instead.
     *
     * @param node the node of interest
     * @return validated type, or null if unknown or not applicable
     */
    @Nullable
    fun getValidatedNodeTypeIfKnown(node: SqlNode?): RelDataType?

    /**
     * Returns the types of a call's operands.
     *
     *
     * Returns null if the call has not been validated, or if the operands'
     * types do not differ from their types as expressions.
     *
     *
     * This method is most useful when some of the operands are of type ANY,
     * or if they need to be coerced to be consistent with other operands, or
     * with the needs of the function.
     *
     * @param call Call
     * @return List of operands' types, or null if not known or 'obvious'
     */
    @Nullable
    fun getValidatedOperandTypes(call: SqlCall?): List<RelDataType?>?

    /**
     * Resolves an identifier to a fully-qualified name.
     *
     * @param id    Identifier
     * @param scope Naming scope
     */
    fun validateIdentifier(id: SqlIdentifier?, scope: SqlValidatorScope?)

    /**
     * Validates a literal.
     *
     * @param literal Literal
     */
    fun validateLiteral(literal: SqlLiteral?)

    /**
     * Validates a [SqlIntervalQualifier].
     *
     * @param qualifier Interval qualifier
     */
    fun validateIntervalQualifier(qualifier: SqlIntervalQualifier?)

    /**
     * Validates an INSERT statement.
     *
     * @param insert INSERT statement
     */
    fun validateInsert(insert: SqlInsert?)

    /**
     * Validates an UPDATE statement.
     *
     * @param update UPDATE statement
     */
    fun validateUpdate(update: SqlUpdate?)

    /**
     * Validates a DELETE statement.
     *
     * @param delete DELETE statement
     */
    fun validateDelete(delete: SqlDelete?)

    /**
     * Validates a MERGE statement.
     *
     * @param merge MERGE statement
     */
    fun validateMerge(merge: SqlMerge?)

    /**
     * Validates a data type expression.
     *
     * @param dataType Data type
     */
    fun validateDataType(dataType: SqlDataTypeSpec?)

    /**
     * Validates a dynamic parameter.
     *
     * @param dynamicParam Dynamic parameter
     */
    fun validateDynamicParam(dynamicParam: SqlDynamicParam?)

    /**
     * Validates the right-hand side of an OVER expression. It might be either
     * an [identifier][SqlIdentifier] referencing a window, or an
     * [inline window specification][SqlWindow].
     *
     * @param windowOrId SqlNode that can be either SqlWindow with all the
     * components of a window spec or a SqlIdentifier with the
     * name of a window spec.
     * @param scope      Naming scope
     * @param call       the SqlNode if a function call if the window is attached
     * to one.
     */
    fun validateWindow(
        windowOrId: SqlNode?,
        scope: SqlValidatorScope?,
        @Nullable call: SqlCall?
    )

    /**
     * Validates a MATCH_RECOGNIZE clause.
     *
     * @param pattern MATCH_RECOGNIZE clause
     */
    fun validateMatchRecognize(pattern: SqlCall?)

    /**
     * Validates a call to an operator.
     *
     * @param call  Operator call
     * @param scope Naming scope
     */
    fun validateCall(
        call: SqlCall?,
        scope: SqlValidatorScope?
    )

    /**
     * Validates parameters for aggregate function.
     *
     * @param aggCall      Call to aggregate function
     * @param filter       Filter (`FILTER (WHERE)` clause), or null
     * @param distinctList Distinct specification (`WITHIN DISTINCT`
     * clause), or null
     * @param orderList    Ordering specification (`WITHIN GROUP` clause),
     * or null
     * @param scope        Syntactic scope
     */
    fun validateAggregateParams(
        aggCall: SqlCall?, @Nullable filter: SqlNode?,
        @Nullable distinctList: SqlNodeList?, @Nullable orderList: SqlNodeList?,
        scope: SqlValidatorScope?
    )

    /**
     * Validates a COLUMN_LIST parameter.
     *
     * @param function function containing COLUMN_LIST parameter
     * @param argTypes function arguments
     * @param operands operands passed into the function call
     */
    fun validateColumnListParams(
        function: SqlFunction?,
        argTypes: List<RelDataType?>?,
        operands: List<SqlNode?>?
    )

    /**
     * If an identifier is a legitimate call to a function that has no
     * arguments and requires no parentheses (for example "CURRENT_USER"),
     * returns a call to that function, otherwise returns null.
     */
    @Nullable
    fun makeNullaryCall(id: SqlIdentifier?): SqlCall?

    /**
     * Derives the type of a node in a given scope. If the type has already been
     * inferred, returns the previous type.
     *
     * @param scope   Syntactic scope
     * @param operand Parse tree node
     * @return Type of the SqlNode. Should never return `NULL`
     */
    fun deriveType(
        scope: SqlValidatorScope?,
        operand: SqlNode?
    ): RelDataType?

    /**
     * Adds "line x, column y" context to a validator exception.
     *
     *
     * Note that the input exception is checked (it derives from
     * [Exception]) and the output exception is unchecked (it derives from
     * [RuntimeException]). This is intentional -- it should remind code
     * authors to provide context for their validation errors.
     *
     * @param node The place where the exception occurred, not null
     * @param e    The validation error
     * @return Exception containing positional information, never null
     */
    fun newValidationError(
        node: SqlNode?,
        e: Resources.ExInst<SqlValidatorException?>?
    ): CalciteContextException?

    /**
     * Returns whether a SELECT statement is an aggregation. Criteria are: (1)
     * contains GROUP BY, or (2) contains HAVING, or (3) SELECT or ORDER BY
     * clause contains aggregate functions. (Windowed aggregate functions, such
     * as `SUM(x) OVER w`, don't count.)
     *
     * @param select SELECT statement
     * @return whether SELECT statement is an aggregation
     */
    fun isAggregate(select: SqlSelect?): Boolean

    /**
     * Returns whether a select list expression is an aggregate function.
     *
     * @param selectNode Expression in SELECT clause
     * @return whether expression is an aggregate function
     */
    @Deprecated
    fun  // to be removed before 2.0
            isAggregate(selectNode: SqlNode?): Boolean

    /**
     * Converts a window specification or window name into a fully-resolved
     * window specification. For example, in `SELECT sum(x) OVER (PARTITION
     * BY x ORDER BY y), sum(y) OVER w1, sum(z) OVER (w ORDER BY y) FROM t
     * WINDOW w AS (PARTITION BY x)` all aggregations have the same
     * resolved window specification `(PARTITION BY x ORDER BY y)`.
     *
     * @param windowOrRef    Either the name of a window (a [SqlIdentifier])
     * or a window specification (a [SqlWindow]).
     * @param scope          Scope in which to resolve window names
     * @return A window
     * @throws RuntimeException Validation exception if window does not exist
     */
    fun resolveWindow(
        windowOrRef: SqlNode?,
        scope: SqlValidatorScope?
    ): SqlWindow?

    /**
     * Converts a window specification or window name into a fully-resolved
     * window specification.
     *
     * @param populateBounds Whether to populate bounds. Doing so may alter the
     * definition of the window. It is recommended that
     * populate bounds when translating to physical algebra,
     * but not when validating.
     */
    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Use {@link #resolveWindow(SqlNode, SqlValidatorScope)}, which
    does not have the deprecated {@code populateBounds} parameter.

    """
    )
    fun resolveWindow(
        windowOrRef: SqlNode?,
        scope: SqlValidatorScope?,
        populateBounds: Boolean
    ): SqlWindow? {
        return resolveWindow(windowOrRef, scope)
    }

    /**
     * Finds the namespace corresponding to a given node.
     *
     *
     * For example, in the query `SELECT * FROM (SELECT * FROM t), t1 AS
     * alias`, the both items in the FROM clause have a corresponding
     * namespace.
     *
     * @param node Parse tree node
     * @return namespace of node
     */
    @Nullable
    fun getNamespace(node: SqlNode?): SqlValidatorNamespace?

    /**
     * Derives an alias for an expression. If no alias can be derived, returns
     * null if `ordinal` is less than zero, otherwise generates an
     * alias `EXPR$*ordinal*`.
     *
     * @param node    Expression
     * @param ordinal Ordinal of expression
     * @return derived alias, or null if no alias can be derived and ordinal is
     * less than zero
     */
    @Nullable
    fun deriveAlias(
        node: SqlNode?,
        ordinal: Int
    ): String?

    /**
     * Returns a list of expressions, with every occurrence of "&#42;" or
     * "TABLE.&#42;" expanded.
     *
     * @param selectList        Select clause to be expanded
     * @param query             Query
     * @param includeSystemVars Whether to include system variables
     * @return expanded select clause
     */
    fun expandStar(
        selectList: SqlNodeList?,
        query: SqlSelect?,
        includeSystemVars: Boolean
    ): SqlNodeList?

    /**
     * Returns the scope that expressions in the WHERE and GROUP BY clause of
     * this query should use. This scope consists of the tables in the FROM
     * clause, and the enclosing scope.
     *
     * @param select Query
     * @return naming scope of WHERE clause
     */
    fun getWhereScope(select: SqlSelect?): SqlValidatorScope?

    /**
     * Returns the type factory used by this validator.
     *
     * @return type factory
     */
    @get:Pure
    val typeFactory: RelDataTypeFactory

    /**
     * Saves the type of a [SqlNode], now that it has been validated.
     *
     *
     * This method is only for internal use. The validator should drive the
     * type-derivation process, and store nodes' types when they have been derived.
     *
     * @param node A SQL parse tree node, never null
     * @param type Its type; must not be null
     */
    @API(status = API.Status.INTERNAL, since = "1.24")
    fun setValidatedNodeType(
        node: SqlNode?,
        type: RelDataType?
    )

    /**
     * Removes a node from the set of validated nodes.
     *
     * @param node node to be removed
     */
    fun removeValidatedNodeType(node: SqlNode?)

    /**
     * Returns an object representing the "unknown" type.
     *
     * @return unknown type
     */
    val unknownType: RelDataType?

    /**
     * Returns the appropriate scope for validating a particular clause of a
     * SELECT statement.
     *
     *
     * Consider
     *
     * <blockquote><pre>`SELECT *
     * FROM foo
     * WHERE EXISTS (
     * SELECT deptno AS x
     * FROM emp
     * JOIN dept ON emp.deptno = dept.deptno
     * WHERE emp.deptno = 5
     * GROUP BY deptno
     * ORDER BY x)`</pre></blockquote>
     *
     *
     * What objects can be seen in each part of the sub-query?
     *
     *
     *  * In FROM ([.getFromScope] , you can only see 'foo'.
     *
     *  * In WHERE ([.getWhereScope]), GROUP BY ([.getGroupScope]),
     * SELECT (`getSelectScope`), and the ON clause of the JOIN
     * ([.getJoinScope]) you can see 'emp', 'dept', and 'foo'.
     *
     *  * In ORDER BY ([.getOrderScope]), you can see the column alias 'x';
     * and tables 'emp', 'dept', and 'foo'.
     *
     *
     *
     * @param select SELECT statement
     * @return naming scope for SELECT statement
     */
    fun getSelectScope(select: SqlSelect?): SqlValidatorScope

    /**
     * Returns the scope for resolving the SELECT, GROUP BY and HAVING clauses.
     * Always a [SelectScope]; if this is an aggregation query, the
     * [AggregatingScope] is stripped away.
     *
     * @param select SELECT statement
     * @return naming scope for SELECT statement, sans any aggregating scope
     */
    @Nullable
    fun getRawSelectScope(select: SqlSelect?): SelectScope?

    /**
     * Returns a scope containing the objects visible from the FROM clause of a
     * query.
     *
     * @param select SELECT statement
     * @return naming scope for FROM clause
     */
    @Nullable
    fun getFromScope(select: SqlSelect?): SqlValidatorScope?

    /**
     * Returns a scope containing the objects visible from the ON and USING
     * sections of a JOIN clause.
     *
     * @param node The item in the FROM clause which contains the ON or USING
     * expression
     * @return naming scope for JOIN clause
     * @see .getFromScope
     */
    @Nullable
    fun getJoinScope(node: SqlNode?): SqlValidatorScope?

    /**
     * Returns a scope containing the objects visible from the GROUP BY clause
     * of a query.
     *
     * @param select SELECT statement
     * @return naming scope for GROUP BY clause
     */
    fun getGroupScope(select: SqlSelect?): SqlValidatorScope?

    /**
     * Returns a scope containing the objects visible from the HAVING clause of
     * a query.
     *
     * @param select SELECT statement
     * @return naming scope for HAVING clause
     */
    fun getHavingScope(select: SqlSelect?): SqlValidatorScope?

    /**
     * Returns the scope that expressions in the SELECT and HAVING clause of
     * this query should use. This scope consists of the FROM clause and the
     * enclosing scope. If the query is aggregating, only columns in the GROUP
     * BY clause may be used.
     *
     * @param select SELECT statement
     * @return naming scope for ORDER BY clause
     */
    fun getOrderScope(select: SqlSelect?): SqlValidatorScope?

    /**
     * Returns a scope match recognize clause.
     *
     * @param node Match recognize
     * @return naming scope for Match recognize clause
     */
    fun getMatchRecognizeScope(node: SqlMatchRecognize?): SqlValidatorScope?

    /**
     * Declares a SELECT expression as a cursor.
     *
     * @param select select expression associated with the cursor
     * @param scope  scope of the parent query associated with the cursor
     */
    fun declareCursor(select: SqlSelect?, scope: SqlValidatorScope?)

    /**
     * Pushes a new instance of a function call on to a function call stack.
     */
    fun pushFunctionCall()

    /**
     * Removes the topmost entry from the function call stack.
     */
    fun popFunctionCall()

    /**
     * Retrieves the name of the parent cursor referenced by a column list
     * parameter.
     *
     * @param columnListParamName name of the column list parameter
     * @return name of the parent cursor
     */
    @Nullable
    fun getParentCursor(columnListParamName: String?): String?

    /**
     * Derives the type of a constructor.
     *
     * @param scope                 Scope
     * @param call                  Call
     * @param unresolvedConstructor TODO
     * @param resolvedConstructor   TODO
     * @param argTypes              Types of arguments
     * @return Resolved type of constructor
     */
    fun deriveConstructorType(
        scope: SqlValidatorScope?,
        call: SqlCall?,
        unresolvedConstructor: SqlFunction?,
        @Nullable resolvedConstructor: SqlFunction?,
        argTypes: List<RelDataType?>?
    ): RelDataType?

    /**
     * Handles a call to a function which cannot be resolved. Returns an
     * appropriately descriptive error, which caller must throw.
     *
     * @param call               Call
     * @param unresolvedFunction Overloaded function which is the target of the
     * call
     * @param argTypes           Types of arguments
     * @param argNames           Names of arguments, or null if call by position
     */
    fun handleUnresolvedFunction(
        call: SqlCall?,
        unresolvedFunction: SqlOperator?, argTypes: List<RelDataType?>?,
        @Nullable argNames: List<String?>?
    ): CalciteException?

    /**
     * Expands an expression in the ORDER BY clause into an expression with the
     * same semantics as expressions in the SELECT clause.
     *
     *
     * This is made necessary by a couple of dialect 'features':
     *
     *
     *  * **ordinal expressions**: In "SELECT x, y FROM t ORDER BY 2", the
     * expression "2" is shorthand for the 2nd item in the select clause, namely
     * "y".
     *  * **alias references**: In "SELECT x AS a, y FROM t ORDER BY a", the
     * expression "a" is shorthand for the item in the select clause whose alias
     * is "a"
     *
     *
     * @param select    Select statement which contains ORDER BY
     * @param orderExpr Expression in the ORDER BY clause.
     * @return Expression translated into SELECT clause semantics
     */
    fun expandOrderExpr(select: SqlSelect?, orderExpr: SqlNode?): SqlNode?

    /**
     * Expands an expression.
     *
     * @param expr  Expression
     * @param scope Scope
     * @return Expanded expression
     */
    fun expand(expr: SqlNode?, scope: SqlValidatorScope?): SqlNode

    /**
     * Returns whether a field is a system field. Such fields may have
     * particular properties such as sortedness and nullability.
     *
     *
     * In the default implementation, always returns `false`.
     *
     * @param field Field
     * @return whether field is a system field
     */
    fun isSystemField(field: RelDataTypeField?): Boolean

    /**
     * Returns a description of how each field in the row type maps to a
     * catalog, schema, table and column in the schema.
     *
     *
     * The returned list is never null, and has one element for each field
     * in the row type. Each element is a list of four elements (catalog,
     * schema, table, column), or may be null if the column is an expression.
     *
     * @param sqlQuery Query
     * @return Description of how each field in the row type maps to a schema
     * object
     */
    fun getFieldOrigins(sqlQuery: SqlNode?): List<List<String?>?>?

    /**
     * Returns a record type that contains the name and type of each parameter.
     * Returns a record type with no fields if there are no parameters.
     *
     * @param sqlQuery Query
     * @return Record type
     */
    fun getParameterRowType(sqlQuery: SqlNode?): RelDataType?

    /**
     * Returns the scope of an OVER or VALUES node.
     *
     * @param node Node
     * @return Scope
     */
    fun getOverScope(node: SqlNode?): SqlValidatorScope?

    /**
     * Validates that a query is capable of producing a return of given modality
     * (relational or streaming).
     *
     * @param select Query
     * @param modality Modality (streaming or relational)
     * @param fail Whether to throw a user error if does not support required
     * modality
     * @return whether query supports the given modality
     */
    fun validateModality(
        select: SqlSelect?, modality: SqlModality?,
        fail: Boolean
    ): Boolean

    fun validateWith(with: SqlWith?, scope: SqlValidatorScope?)
    fun validateWithItem(withItem: SqlWithItem?)
    fun validateSequenceValue(scope: SqlValidatorScope?, id: SqlIdentifier?)

    @Nullable
    fun getWithScope(withItem: SqlNode?): SqlValidatorScope?

    /** Get the type coercion instance.  */
    val typeCoercion: TypeCoercion?

    /** Returns the config of the validator.  */
    fun config(): Config?

    /**
     * Returns this SqlValidator, with the same state, applying
     * a transform to the config.
     *
     *
     * This is mainly used for tests, otherwise constructs a [Config] directly
     * through the constructor.
     */
    @API(status = API.Status.INTERNAL, since = "1.23")
    fun transform(transform: UnaryOperator<Config?>?): SqlValidator?
    //~ Inner Class ------------------------------------------------------------
    /**
     * Interface to define the configuration for a SqlValidator.
     * Provides methods to set each configuration option.
     */
    @Value.Immutable(singleton = false)
    interface Config {
        /**
         * Returns whether to enable rewrite of "macro-like" calls such as COALESCE.
         */
        @Value.Default
        fun callRewrite(): Boolean {
            return true
        }

        /**
         * Sets whether to enable rewrite of "macro-like" calls such as COALESCE.
         */
        fun withCallRewrite(rewrite: Boolean): Config?

        /** Returns how NULL values should be collated if an ORDER BY item does not
         * contain NULLS FIRST or NULLS LAST.  */
        @Value.Default
        fun defaultNullCollation(): NullCollation? {
            return NullCollation.HIGH
        }

        /** Sets how NULL values should be collated if an ORDER BY item does not
         * contain NULLS FIRST or NULLS LAST.  */
        fun withDefaultNullCollation(nullCollation: NullCollation?): Config?

        /** Returns whether column reference expansion is enabled.  */
        @Value.Default
        fun columnReferenceExpansion(): Boolean {
            return true
        }

        /**
         * Sets whether to enable expansion of column references. (Currently this does
         * not apply to the ORDER BY clause; may be fixed in the future.)
         */
        fun withColumnReferenceExpansion(expand: Boolean): Config?

        /**
         * Returns whether to expand identifiers other than column
         * references.
         *
         *
         * REVIEW jvs 30-June-2006: subclasses may override shouldExpandIdentifiers
         * in a way that ignores this; we should probably get rid of the protected
         * method and always use this variable (or better, move preferences like
         * this to a separate "parameter" class).
         */
        @Value.Default
        fun identifierExpansion(): Boolean {
            return false
        }

        /**
         * Sets whether to enable expansion of identifiers other than column
         * references.
         */
        fun withIdentifierExpansion(expand: Boolean): Config?

        /**
         * Returns whether this validator should be lenient upon encountering an
         * unknown function, default false.
         *
         *
         * If true, if a statement contains a call to a function that is not
         * present in the operator table, or if the call does not have the required
         * number or types of operands, the validator nevertheless regards the
         * statement as valid. The type of the function call will be
         * [UNKNOWN][.getUnknownType].
         *
         *
         * If false (the default behavior), an unknown function call causes a
         * validation error to be thrown.
         */
        @Value.Default
        fun lenientOperatorLookup(): Boolean {
            return false
        }

        /**
         * Sets whether this validator should be lenient upon encountering an unknown
         * function.
         *
         * @param lenient Whether to be lenient when encountering an unknown function
         */
        fun withLenientOperatorLookup(lenient: Boolean): Config?

        /** Returns whether the validator supports implicit type coercion.  */
        @Value.Default
        fun typeCoercionEnabled(): Boolean {
            return true
        }

        /**
         * Sets whether to enable implicit type coercion for validation, default true.
         *
         * @see org.apache.calcite.sql.validate.implicit.TypeCoercionImpl TypeCoercionImpl
         */
        fun withTypeCoercionEnabled(enabled: Boolean): Config?

        /** Returns the type coercion factory.  */
        fun typeCoercionFactory(): TypeCoercionFactory?

        /**
         * Sets a factory to create type coercion instance that overrides the
         * default coercion rules defined in
         * [org.apache.calcite.sql.validate.implicit.TypeCoercionImpl].
         *
         * @param factory Factory to create [TypeCoercion] instance
         */
        fun withTypeCoercionFactory(factory: TypeCoercionFactory?): Config?

        /** Returns the type coercion rules for explicit type coercion.  */
        @Nullable
        fun typeCoercionRules(): SqlTypeCoercionRule?

        /**
         * Sets the [SqlTypeCoercionRule] instance which defines the type conversion matrix
         * for the explicit type coercion.
         *
         *
         * The `rules` setting should be thread safe. In the default implementation,
         * it is set to a ThreadLocal variable.
         *
         * @param rules The [SqlTypeCoercionRule] instance,
         * see its documentation for how to customize the rules
         */
        fun withTypeCoercionRules(@Nullable rules: SqlTypeCoercionRule?): Config?

        /** Returns the dialect of SQL (SQL:2003, etc.) this validator recognizes.
         * Default is [SqlConformanceEnum.DEFAULT].  */
        @SuppressWarnings("deprecation")
        @Value.Default
        fun sqlConformance(): SqlConformance? {
            return SqlConformance.DEFAULT
        }

        /** Sets up the sql conformance of the validator.  */
        fun withSqlConformance(conformance: SqlConformance?): Config?

        companion object {
            /** Default configuration.  */
            val DEFAULT: Config = ImmutableSqlValidator.Config.builder()
                .withTypeCoercionFactory(TypeCoercions::createTypeCoercion)
                .build()
        }
    }
}
