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
 * A namespace describes the relation returned by a section of a SQL query.
 *
 *
 * For example, in the query `SELECT emp.deptno, age FROM emp,
 * dept`, the FROM clause forms a namespace consisting of two tables EMP
 * and DEPT, and a row type consisting of the combined columns of those tables.
 *
 *
 * Other examples of namespaces include a table in the from list (the
 * namespace contains the constituent columns) and a sub-query (the namespace
 * contains the columns in the SELECT clause of the sub-query).
 *
 *
 * These various kinds of namespace are implemented by classes
 * [IdentifierNamespace] for table names, [SelectNamespace] for
 * SELECT queries, [SetopNamespace] for UNION, EXCEPT and INTERSECT, and
 * so forth. But if you are looking at a SELECT query and call
 * [SqlValidator.getNamespace], you may
 * not get a SelectNamespace. Why? Because the validator is allowed to wrap
 * namespaces in other objects which implement
 * [SqlValidatorNamespace]. Your SelectNamespace will be there somewhere,
 * but might be one or two levels deep.  Don't try to cast the namespace or use
 * `instanceof`; use [SqlValidatorNamespace.unwrap] and
 * [SqlValidatorNamespace.isWrapperFor] instead.
 *
 * @see SqlValidator
 *
 * @see SqlValidatorScope
 */
interface SqlValidatorNamespace {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the validator.
     *
     * @return validator
     */
    val validator: org.apache.calcite.sql.validate.SqlValidator

    /**
     * Returns the underlying table, or null if there is none.
     */
    @get:Nullable
    val table: org.apache.calcite.sql.validate.SqlValidatorTable?

    /**
     * Returns the row type of this namespace, which comprises a list of names
     * and types of the output columns. If the scope's type has not yet been
     * derived, derives it.
     *
     * @return Row type of this namespace, never null, always a struct
     */
    val rowType: RelDataType
    /**
     * Returns the type of this namespace.
     *
     * @return Row type converted to struct
     */
    /**
     * Sets the type of this namespace.
     *
     *
     * Allows the type for the namespace to be explicitly set, but usually is
     * called during [.validate].
     *
     *
     * Implicitly also sets the row type. If the type is not a struct, then
     * the row type is the type wrapped as a struct with a single column,
     * otherwise the type and row type are the same.
     */
    var type: RelDataType?

    /**
     * Returns the row type of this namespace, sans any system columns.
     *
     * @return Row type sans system columns
     */
    val rowTypeSansSystemColumns: RelDataType

    /**
     * Validates this namespace.
     *
     *
     * If the scope has already been validated, does nothing.
     *
     *
     * Please call [SqlValidatorImpl.validateNamespace] rather than
     * calling this method directly.
     *
     * @param targetRowType Desired row type, must not be null, may be the data
     * type 'unknown'.
     */
    fun validate(targetRowType: RelDataType?)

    /**
     * Returns the parse tree node at the root of this namespace.
     *
     * @return parse tree node; null for [TableNamespace]
     */
    @get:Nullable
    val node: SqlNode

    /**
     * Returns the parse tree node that at is at the root of this namespace and
     * includes all decorations. If there are no decorations, returns the same
     * as [.getNode].
     */
    @get:Nullable
    @get:Pure
    val enclosingNode: SqlNode?

    /**
     * Looks up a child namespace of a given name.
     *
     *
     * For example, in the query `select e.name from emps as e`,
     * `e` is an [IdentifierNamespace] which has a child `
     * name` which is a [FieldNamespace].
     *
     * @param name Name of namespace
     * @return Namespace
     */
    @Nullable
    fun lookupChild(name: String?): SqlValidatorNamespace

    /**
     * Returns whether this namespace has a field of a given name.
     *
     * @param name Field name
     * @return Whether field exists
     */
    fun fieldExists(name: String?): Boolean

    /**
     * Returns a list of expressions which are monotonic in this namespace. For
     * example, if the namespace represents a relation ordered by a column
     * called "TIMESTAMP", then the list would contain a
     * [org.apache.calcite.sql.SqlIdentifier] called "TIMESTAMP".
     */
    val monotonicExprs: List<Any?>?

    /**
     * Returns whether and how a given column is sorted.
     */
    fun getMonotonicity(columnName: String?): SqlMonotonicity?

    @Deprecated
    fun  // to be removed before 2.0
            makeNullable()

    /**
     * Returns this namespace, or a wrapped namespace, cast to a particular
     * class.
     *
     * @param clazz Desired type
     * @return This namespace cast to desired type
     * @throws ClassCastException if no such interface is available
     */
    fun <T : Object?> unwrap(clazz: Class<T>?): T

    /**
     * Returns whether this namespace implements a given interface, or wraps a
     * class which does.
     *
     * @param clazz Interface
     * @return Whether namespace implements given interface
     */
    fun isWrapperFor(clazz: Class<*>?): Boolean

    /** If this namespace resolves to another namespace, returns that namespace,
     * following links to the end of the chain.
     *
     *
     * A `WITH`) clause defines table names that resolve to queries
     * (the body of the with-item). An [IdentifierNamespace] typically
     * resolves to a [TableNamespace].
     *
     *
     * You must not call this method before [.validate] has
     * completed.  */
    fun resolve(): SqlValidatorNamespace

    /** Returns whether this namespace is capable of giving results of the desired
     * modality. `true` means streaming, `false` means relational.
     *
     * @param modality Modality
     */
    fun supportsModality(modality: SqlModality?): Boolean
}
