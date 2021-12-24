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

import org.apache.calcite.sql.`fun`.SqlLibrary

/**
 * Enumeration of built-in SQL compatibility modes.
 */
enum class SqlConformanceEnum : SqlConformance {
    /** Calcite's default SQL behavior.  */
    DEFAULT,

    /** Conformance value that allows just about everything supported by
     * Calcite.  */
    LENIENT,

    /** Conformance value that allows anything supported by any dialect.
     * Even more liberal than [.LENIENT].  */
    BABEL,

    /** Conformance value that instructs Calcite to use SQL semantics strictly
     * consistent with the SQL:92 standard.  */
    STRICT_92,

    /** Conformance value that instructs Calcite to use SQL semantics strictly
     * consistent with the SQL:99 standard.  */
    STRICT_99,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with the SQL:99 standard, but ignoring its more
     * inconvenient or controversial dicta.  */
    PRAGMATIC_99,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with BigQuery.  */
    BIG_QUERY,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with MySQL version 5.x.  */
    MYSQL_5,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with Oracle version 10.  */
    ORACLE_10,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with Oracle version 12.
     *
     *
     * As [.ORACLE_10] except for [.isApplyAllowed].  */
    ORACLE_12,

    /** Conformance value that instructs Calcite to use SQL semantics strictly
     * consistent with the SQL:2003 standard.  */
    STRICT_2003,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with the SQL:2003 standard, but ignoring its more
     * inconvenient or controversial dicta.  */
    PRAGMATIC_2003,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with Presto.  */
    PRESTO,

    /** Conformance value that instructs Calcite to use SQL semantics
     * consistent with Microsoft SQL Server version 2008.  */
    SQL_SERVER_2008;

    @get:Override
    override val isLiberal: Boolean
        get() = when (this) {
            BABEL -> true
            else -> false
        }

    @Override
    override fun allowCharLiteralAlias(): Boolean {
        return when (this) {
            BABEL, BIG_QUERY, LENIENT, MYSQL_5, SQL_SERVER_2008 -> true
            else -> false
        }
    }

    @get:Override
    override val isGroupByAlias: Boolean
        get() = when (this) {
            BABEL, LENIENT, BIG_QUERY, MYSQL_5 -> true
            else -> false
        }

    @get:Override
    override val isGroupByOrdinal: Boolean
        get() = when (this) {
            BABEL, BIG_QUERY, LENIENT, MYSQL_5, PRESTO -> true
            else -> false
        }

    @get:Override
    override val isHavingAlias: Boolean
        get() = when (this) {
            BABEL, LENIENT, BIG_QUERY, MYSQL_5 -> true
            else -> false
        }

    @get:Override
    override val isSortByOrdinal: Boolean
        get() = when (this) {
            DEFAULT, BABEL, LENIENT, BIG_QUERY, MYSQL_5, ORACLE_10, ORACLE_12, STRICT_92, PRAGMATIC_99, PRAGMATIC_2003, SQL_SERVER_2008, PRESTO -> true
            else -> false
        }

    @get:Override
    override val isSortByAlias: Boolean
        get() = when (this) {
            DEFAULT, BABEL, LENIENT, MYSQL_5, ORACLE_10, ORACLE_12, STRICT_92, SQL_SERVER_2008 -> true
            else -> false
        }

    @get:Override
    override val isSortByAliasObscures: Boolean
        get() = this == STRICT_92

    @get:Override
    override val isFromRequired: Boolean
        get() = when (this) {
            ORACLE_10, ORACLE_12, STRICT_92, STRICT_99, STRICT_2003 -> true
            else -> false
        }

    @Override
    override fun splitQuotedTableName(): Boolean {
        return when (this) {
            BIG_QUERY -> true
            else -> false
        }
    }

    @Override
    override fun allowHyphenInUnquotedTableName(): Boolean {
        return when (this) {
            BIG_QUERY -> true
            else -> false
        }
    }

    @get:Override
    override val isBangEqualAllowed: Boolean
        get() = when (this) {
            LENIENT, BABEL, MYSQL_5, ORACLE_10, ORACLE_12, PRESTO -> true
            else -> false
        }

    @get:Override
    override val isMinusAllowed: Boolean
        get() = when (this) {
            BABEL, LENIENT, ORACLE_10, ORACLE_12 -> true
            else -> false
        }

    @get:Override
    override val isPercentRemainderAllowed: Boolean
        get() = when (this) {
            BABEL, LENIENT, MYSQL_5, PRESTO -> true
            else -> false
        }

    @get:Override
    override val isApplyAllowed: Boolean
        get() = when (this) {
            BABEL, LENIENT, SQL_SERVER_2008, ORACLE_12 -> true
            else -> false
        }

    @get:Override
    override val isInsertSubsetColumnsAllowed: Boolean
        get() = when (this) {
            BABEL, LENIENT, PRAGMATIC_99, PRAGMATIC_2003, BIG_QUERY -> true
            else -> false
        }

    @Override
    override fun allowNiladicParentheses(): Boolean {
        return when (this) {
            BABEL, LENIENT, MYSQL_5, BIG_QUERY -> true
            else -> false
        }
    }

    @Override
    override fun allowExplicitRowValueConstructor(): Boolean {
        return when (this) {
            DEFAULT, LENIENT, PRESTO -> true
            else -> false
        }
    }

    @Override
    override fun allowExtend(): Boolean {
        return when (this) {
            BABEL, LENIENT -> true
            else -> false
        }
    }

    @get:Override
    override val isLimitStartCountAllowed: Boolean
        get() = when (this) {
            BABEL, LENIENT, MYSQL_5 -> true
            else -> false
        }

    @Override
    override fun allowGeometry(): Boolean {
        return when (this) {
            BABEL, LENIENT, MYSQL_5, SQL_SERVER_2008, PRESTO -> true
            else -> false
        }
    }

    @Override
    override fun shouldConvertRaggedUnionTypesToVarying(): Boolean {
        return when (this) {
            PRAGMATIC_99, PRAGMATIC_2003, BIG_QUERY, MYSQL_5, ORACLE_10, ORACLE_12, SQL_SERVER_2008, PRESTO -> true
            else -> false
        }
    }

    @Override
    override fun allowExtendedTrim(): Boolean {
        return when (this) {
            BABEL, LENIENT, MYSQL_5, SQL_SERVER_2008 -> true
            else -> false
        }
    }

    @Override
    override fun allowPluralTimeUnits(): Boolean {
        return when (this) {
            BABEL, LENIENT -> true
            else -> false
        }
    }

    @Override
    override fun allowQualifyingCommonColumn(): Boolean {
        return when (this) {
            ORACLE_10, ORACLE_12, STRICT_92, STRICT_99, STRICT_2003, PRESTO -> false
            else -> true
        }
    }

    @Override
    override fun allowAliasUnnestItems(): Boolean {
        return when (this) {
            PRESTO -> true
            else -> false
        }
    }

    @Override
    override fun semantics(): SqlLibrary {
        return when (this) {
            BIG_QUERY -> SqlLibrary.BIG_QUERY
            MYSQL_5 -> SqlLibrary.MYSQL
            ORACLE_12, ORACLE_10 -> SqlLibrary.ORACLE
            else -> SqlLibrary.STANDARD
        }
    }
}
