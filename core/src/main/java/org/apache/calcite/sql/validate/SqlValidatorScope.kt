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

import org.apache.calcite.rel.type.RelDataType

/**
 * Name-resolution scope. Represents any position in a parse tree than an
 * expression can be, or anything in the parse tree which has columns.
 *
 *
 * When validating an expression, say "foo"."bar", you first use the
 * [.resolve] method of the scope where the expression is defined to
 * locate "foo". If successful, this returns a
 * [namespace][SqlValidatorNamespace] describing the type of the resulting
 * object.
 */
interface SqlValidatorScope {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the validator which created this scope.
     */
    val validator: org.apache.calcite.sql.validate.SqlValidator

    /**
     * Returns the root node of this scope. Never null.
     */
    val node: SqlNode

    /**
     * Looks up a node with a given name. Returns null if none is found.
     *
     * @param names       Name of node to find, maybe partially or fully qualified
     * @param nameMatcher Name matcher
     * @param deep        Whether to look more than one level deep
     * @param resolved    Callback wherein to write the match(es) we find
     */
    fun resolve(
        names: List<String?>?, nameMatcher: SqlNameMatcher?, deep: Boolean,
        resolved: Resolved?
    )
    // CHECKSTYLE: IGNORE 1

    @Deprecated
    @Deprecated(
        """Use
    {@link #findQualifyingTableNames(String, SqlNode, SqlNameMatcher)} """
    )
    fun  // to be removed before 2.0
            findQualifyingTableName(
        columnName: String?,
        ctx: SqlNode?
    ): Pair<String?, SqlValidatorNamespace?>?

    /**
     * Finds all table aliases which are implicitly qualifying an unqualified
     * column name.
     *
     *
     * This method is only implemented in scopes (such as
     * [org.apache.calcite.sql.validate.SelectScope]) which can be the
     * context for name-resolution. In scopes such as
     * [org.apache.calcite.sql.validate.IdentifierNamespace], it throws
     * [UnsupportedOperationException].
     *
     * @param columnName Column name
     * @param ctx        Validation context, to appear in any error thrown
     * @param nameMatcher Name matcher
     *
     * @return Map of applicable table alias and namespaces, never null, empty
     * if no aliases found
     */
    fun findQualifyingTableNames(
        columnName: String?,
        ctx: SqlNode?, nameMatcher: SqlNameMatcher?
    ): Map<String?, ScopeChild?>?

    /**
     * Collects the [SqlMoniker]s of all possible columns in this scope.
     *
     * @param result an array list of strings to add the result to
     */
    fun findAllColumnNames(result: List<SqlMoniker?>?)

    /**
     * Collects the [SqlMoniker]s of all table aliases (uses of tables in
     * query FROM clauses) available in this scope.
     *
     * @param result a list of monikers to add the result to
     */
    fun findAliases(result: Collection<SqlMoniker?>?)

    /**
     * Converts an identifier into a fully-qualified identifier. For example,
     * the "empno" in "select empno from emp natural join dept" becomes
     * "emp.empno".
     *
     * @return A qualified identifier, never null
     */
    fun fullyQualify(identifier: SqlIdentifier?): SqlQualified

    /**
     * Registers a relation in this scope.
     *
     * @param ns    Namespace representing the result-columns of the relation
     * @param alias Alias with which to reference the relation, must not be null
     * @param nullable Whether this is a null-generating side of a join
     */
    fun addChild(ns: SqlValidatorNamespace?, alias: String?, nullable: Boolean)

    /**
     * Finds a window with a given name. Returns null if not found.
     */
    @Nullable
    fun lookupWindow(name: String?): SqlWindow?

    /**
     * Returns whether an expression is monotonic in this scope. For example, if
     * the scope has previously been sorted by columns X, Y, then X is monotonic
     * in this scope, but Y is not.
     */
    fun getMonotonicity(expr: SqlNode?): SqlMonotonicity?

    /**
     * Returns the expressions by which the rows in this scope are sorted. If
     * the rows are unsorted, returns null.
     */
    @get:Nullable
    val orderList: SqlNodeList?

    /**
     * Resolves a single identifier to a column, and returns the datatype of
     * that column.
     *
     *
     * If it cannot find the column, returns null. If the column is
     * ambiguous, throws an error with context `ctx`.
     *
     * @param name Name of column
     * @param ctx  Context for exception
     * @return Type of column, if found and unambiguous; null if not found
     */
    @Nullable
    fun resolveColumn(name: String?, ctx: SqlNode?): RelDataType?

    /**
     * Returns the scope within which operands to a call are to be validated.
     * Usually it is this scope, but when the call is to an aggregate function
     * and this is an aggregating scope, it will be a a different scope.
     *
     * @param call Call
     * @return Scope within which to validate arguments to call.
     */
    fun getOperandScope(call: SqlCall?): SqlValidatorScope

    /**
     * Performs any scope-specific validation of an expression. For example, an
     * aggregating scope requires that expressions are valid aggregations. The
     * expression has already been validated.
     */
    fun validateExpr(expr: SqlNode?)
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Nullable
    @Deprecated(
        """Use
    {@link #resolveTable(List, SqlNameMatcher, Path, Resolved)}. """
    )
    fun getTableNamespace(names: List<String?>?): SqlValidatorNamespace?

    /**
     * Looks up a table in this scope from its name. If found, calls
     * [Resolved.resolve].
     * [TableNamespace] that wraps it. If the "table" is defined in a
     * `WITH` clause it may be a query, not a table after all.
     *
     *
     * The name matcher is not null, and one typically uses
     * [SqlValidatorCatalogReader.nameMatcher].
     *
     * @param names Name of table, may be qualified or fully-qualified
     * @param nameMatcher Name matcher
     * @param path List of names that we have traversed through so far
     */
    fun resolveTable(
        names: List<String?>?, nameMatcher: SqlNameMatcher?, path: Path?,
        resolved: Resolved?
    )

    /** Converts the type of an expression to nullable, if the context
     * warrants it.  */
    fun nullifyType(node: SqlNode?, type: RelDataType?): RelDataType?

    /** Returns whether this scope is enclosed within `scope2` in such
     * a way that it can see the contents of `scope2`.  */
    fun isWithin(@Nullable scope2: SqlValidatorScope): Boolean {
        return this === scope2
    }

    /** Callback from [SqlValidatorScope.resolve].  */
    interface Resolved {
        fun found(
            namespace: SqlValidatorNamespace?, nullable: Boolean,
            @Nullable scope: SqlValidatorScope?, path: Path?, @Nullable remainingNames: List<String?>?
        )

        fun count(): Int
    }

    /** A sequence of steps by which an identifier was resolved. Immutable.  */
    abstract class Path {
        /** Creates a path that consists of this path plus one additional step.  */
        fun plus(@Nullable rowType: RelDataType?, i: Int, name: String?, kind: StructKind?): Step {
            return Step(this, rowType, i, name, kind)
        }

        /** Number of steps in this path.  */
        fun stepCount(): Int {
            return 0
        }

        /** Returns the steps in this path.  */
        fun steps(): List<Step> {
            val paths: ImmutableList.Builder<Step?> = Builder()
            build(paths)
            return paths.build()
        }

        /** Returns a list ["step1", "step2"].  */
        fun stepNames(): List<String> {
            return Util.transform(steps()) { input -> input.name }
        }

        fun build(paths: ImmutableList.Builder<Step?>?) {}

        @Override
        override fun toString(): String {
            return stepNames().toString()
        }

        companion object {
            /** The empty path.  */
            @SuppressWarnings("StaticInitializerReferencesSubClass")
            val EMPTY = EmptyPath()
        }
    }

    /** A path that has no steps.  */
    class EmptyPath : Path()

    /** A step in resolving an identifier.  */
    class Step internal constructor(
        parent: Path?, @Nullable rowType: RelDataType?, i: Int, name: String?,
        kind: StructKind?
    ) : Path() {
        val parent: Path

        @Nullable
        val rowType: RelDataType?
        val i: Int
        val name: String?
        val kind: StructKind

        init {
            this.parent = Objects.requireNonNull(parent, "parent")
            this.rowType = rowType // may be null
            this.i = i
            this.name = name
            this.kind = Objects.requireNonNull(kind, "kind")
        }

        @Override
        override fun stepCount(): Int {
            return 1 + parent.stepCount()
        }

        @Override
        protected override fun build(paths: ImmutableList.Builder<Step?>) {
            parent.build(paths)
            paths.add(this)
        }
    }

    /** Default implementation of
     * [org.apache.calcite.sql.validate.SqlValidatorScope.Resolved].  */
    class ResolvedImpl : Resolved {
        val resolves: List<Resolve> = ArrayList()
        @Override
        override fun found(
            namespace: SqlValidatorNamespace?, nullable: Boolean,
            @Nullable scope: SqlValidatorScope, path: Path?, @Nullable remainingNames: List<String?>?
        ) {
            var scope = scope
            if (scope is TableScope) {
                scope = scope.validator.getSelectScope(scope.node as SqlSelect)
            }
            if (scope is AggregatingSelectScope) {
                scope = (scope as AggregatingSelectScope).parent
                assert(scope is SelectScope)
            }
            resolves.add(
                Resolve(namespace, nullable, scope, path, remainingNames)
            )
        }

        @Override
        override fun count(): Int {
            return resolves.size()
        }

        fun only(): Resolve {
            return Iterables.getOnlyElement(resolves)
        }

        /** Resets all state.  */
        fun clear() {
            resolves.clear()
        }
    }

    /** A match found when looking up a name.  */
    class Resolve internal constructor(
        namespace: SqlValidatorNamespace?, nullable: Boolean,
        @Nullable scope: SqlValidatorScope, path: Path?, @Nullable remainingNames: List<String?>?
    ) {
        val namespace: SqlValidatorNamespace
        private val nullable: Boolean

        @Nullable
        val scope // may be null
                : SqlValidatorScope
        val path: Path

        /** Names not matched; empty if it was a full match.  */
        val remainingNames: List<String>

        init {
            this.namespace = Objects.requireNonNull(namespace, "namespace")
            this.nullable = nullable
            this.scope = scope
            assert(scope !is TableScope)
            this.path = Objects.requireNonNull(path, "path")
            this.remainingNames =
                if (remainingNames == null) ImmutableList.of() else ImmutableList.copyOf(remainingNames)
        }

        /** The row type of the found namespace, nullable if the lookup has
         * looked into outer joins.  */
        fun rowType(): RelDataType {
            return namespace.getValidator().getTypeFactory()
                .createTypeWithNullability(namespace.getRowType(), nullable)
        }
    }
}
