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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.sql.SqlFunction

/**
 * Operator table that contains only Oracle-specific functions and operators.
 *
 */
@Deprecated // to be removed before 2.0
@Deprecated(
    """Use
  {@link SqlLibraryOperatorTableFactory#getOperatorTable(SqlLibrary...)}
  instead, passing {@link SqlLibrary#ORACLE} as argument."""
)
object OracleSqlOperatorTable : ReflectiveSqlOperatorTable() {
    //~ Static fields/initializers ---------------------------------------------
    /**
     * The table of contains Oracle-specific operators.
     */
    @Nullable
    private var instance: OracleSqlOperatorTable? = null

    @Deprecated // to be removed before 2.0
    val DECODE: SqlFunction = SqlLibraryOperators.DECODE

    @Deprecated // to be removed before 2.0
    val NVL: SqlFunction = SqlLibraryOperators.NVL

    @Deprecated // to be removed before 2.0
    val LTRIM: SqlFunction = SqlLibraryOperators.LTRIM

    @Deprecated // to be removed before 2.0
    val RTRIM: SqlFunction = SqlLibraryOperators.RTRIM

    @Deprecated // to be removed before 2.0
    val SUBSTR: SqlFunction = SqlLibraryOperators.SUBSTR_ORACLE

    @Deprecated // to be removed before 2.0
    val GREATEST: SqlFunction = SqlLibraryOperators.GREATEST

    @Deprecated // to be removed before 2.0
    val LEAST: SqlFunction = SqlLibraryOperators.LEAST

    @Deprecated // to be removed before 2.0
    val TRANSLATE3: SqlFunction = SqlLibraryOperators.TRANSLATE3

    /**
     * Returns the Oracle operator table, creating it if necessary.
     */
    @Synchronized
    fun instance(): OracleSqlOperatorTable? {
        var instance = instance
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the
            // table until the constructor of the sub-class has completed.
            instance = OracleSqlOperatorTable()
            instance.init()
            OracleSqlOperatorTable.instance = instance
        }
        return instance
    }
}
