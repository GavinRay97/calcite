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

import org.apache.calcite.config.CalciteConnectionConfig

/**
 * Supplies catalog information for [SqlValidator].
 *
 *
 * This interface only provides a thin API to the underlying repository, and
 * this is intentional. By only presenting the repository information of
 * interest to the validator, we reduce the dependency on exact mechanism to
 * implement the repository. It is also possible to construct mock
 * implementations of this interface for testing purposes.
 */
interface SqlValidatorCatalogReader : Wrapper {
    //~ Methods ----------------------------------------------------------------
    /**
     * Finds a table or schema with the given name, possibly qualified.
     *
     *
     * Uses the case-sensitivity policy of the catalog reader.
     *
     *
     * If not found, returns null. If you want a more descriptive error
     * message or to override the case-sensitivity of the match, use
     * [SqlValidatorScope.resolveTable].
     *
     * @param names Name of table, may be qualified or fully-qualified
     *
     * @return Table with the given name, or null
     */
    @Nullable
    fun getTable(names: List<String?>?): SqlValidatorTable?

    /**
     * Finds a user-defined type with the given name, possibly qualified.
     *
     *
     * NOTE jvs 12-Feb-2005: the reason this method is defined here instead
     * of on RelDataTypeFactory is that it has to take into account
     * context-dependent information such as SQL schema path, whereas a type
     * factory is context-independent.
     *
     * @param typeName Name of type
     * @return named type, or null if not found
     */
    @Nullable
    fun getNamedType(typeName: SqlIdentifier?): RelDataType?

    /**
     * Given fully qualified schema name, returns schema object names as
     * specified. They can be schema, table, function, view.
     * When names array is empty, the contents of root schema should be returned.
     *
     * @param names the array contains fully qualified schema name or empty
     * list for root schema
     * @return the list of all object (schema, table, function,
     * view) names under the above criteria
     */
    fun getAllSchemaObjectNames(names: List<String?>?): List<SqlMoniker?>?

    /**
     * Returns the paths of all schemas to look in for tables.
     *
     * @return paths of current schema and root schema
     */
    val schemaPaths: List<List<String?>?>
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Nullable
    @Deprecated(
        """Use
    {@link #nameMatcher()}.{@link SqlNameMatcher#field(RelDataType, String)} """
    )
    fun field(rowType: RelDataType?, alias: String?): RelDataTypeField?

    /** Returns an implementation of
     * [org.apache.calcite.sql.validate.SqlNameMatcher]
     * that matches the case-sensitivity policy.  */
    fun nameMatcher(): SqlNameMatcher
    // CHECKSTYLE: IGNORE 1

    @Deprecated
    @Deprecated(
        """Use
    {@link #nameMatcher()}.{@link SqlNameMatcher#matches(String, String)} """
    )
    fun  // to be removed before 2.0
            matches(string: String?, name: String?): Boolean

    fun createTypeFromProjection(
        type: RelDataType?,
        columnNameList: List<String?>?
    ): RelDataType?
    // CHECKSTYLE: IGNORE 1

    // to be removed before 2.0
    @get:Deprecated(
        """Use
    {@link #nameMatcher()}.{@link SqlNameMatcher#isCaseSensitive()} """
    )
    @get:Deprecated
    val isCaseSensitive: Boolean

    /** Returns the root namespace for name resolution.  */
    val rootSchema: CalciteSchema

    /** Returns Config settings.  */
    val config: CalciteConnectionConfig?
}
