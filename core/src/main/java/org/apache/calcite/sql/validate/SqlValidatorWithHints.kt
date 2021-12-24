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

import org.apache.calcite.sql.SqlIdentifier

/**
 * Extends [SqlValidator] to allow discovery of useful data such as fully
 * qualified names of SQL objects, alternative valid SQL objects that can be
 * used in the SQL statement (dubbed as hints).
 */
interface SqlValidatorWithHints : SqlValidator {
    //~ Methods ----------------------------------------------------------------
    /**
     * Looks up completion hints for a syntactically correct SQL statement that
     * has been parsed into an expression tree. (Note this should be called
     * after [.validate].
     *
     * @param topNode top of expression tree in which to lookup completion hints
     * @param pos     indicates the position in the sql statement we want to get
     * completion hints for. For example, "select a.ename, b.deptno
     * from sales.emp a join sales.dept b "on a.deptno=b.deptno
     * where empno=1"; setting pos to 'Line 1, Column 17' returns
     * all the possible column names that can be selected from
     * sales.dept table setting pos to 'Line 1, Column 31' returns
     * all the possible table names in 'sales' schema
     * @return an array of [SqlMoniker] (sql identifiers) that can fill in
     * at the indicated position
     */
    fun lookupHints(topNode: SqlNode?, pos: SqlParserPos?): List<SqlMoniker?>?

    /**
     * Looks up the fully qualified name for a [SqlIdentifier] at a given
     * Parser Position in a parsed expression tree Note: call this only after
     * [.validate] has been called.
     *
     * @param topNode top of expression tree in which to lookup the qualified
     * name for the SqlIdentifier
     * @param pos indicates the position of the [SqlIdentifier] in
     * the SQL statement we want to get the qualified
     * name for
     * @return a string of the fully qualified name of the [SqlIdentifier]
     * if the Parser position represents a valid [SqlIdentifier]. Else
     * return null
     */
    @Nullable
    fun lookupQualifiedName(topNode: SqlNode?, pos: SqlParserPos?): SqlMoniker?
}
