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

import org.apache.calcite.avatica.util.Casing

/**
 * The default implementation of a `SqlDialectFactory`.
 */
class SqlDialectFactoryImpl : SqlDialectFactory {
    private val jethroCache: JethroDataSqlDialect.JethroInfoCache = JethroDataSqlDialect.createCache()
    @Override
    fun create(databaseMetaData: DatabaseMetaData): SqlDialect {
        val databaseProductName: String
        val databaseMajorVersion: Int
        val databaseMinorVersion: Int
        val databaseVersion: String
        try {
            databaseProductName = databaseMetaData.getDatabaseProductName()
            databaseMajorVersion = databaseMetaData.getDatabaseMajorVersion()
            databaseMinorVersion = databaseMetaData.getDatabaseMinorVersion()
            databaseVersion = databaseMetaData.getDatabaseProductVersion()
        } catch (e: SQLException) {
            throw RuntimeException("while detecting database product", e)
        }
        val upperProductName: String = databaseProductName.toUpperCase(Locale.ROOT).trim()
        val quoteString = getIdentifierQuoteString(databaseMetaData)
        val nullCollation: NullCollation = getNullCollation(databaseMetaData)
        val unquotedCasing: Casing = getCasing(databaseMetaData, false)
        val quotedCasing: Casing = getCasing(databaseMetaData, true)
        val caseSensitive = isCaseSensitive(databaseMetaData)
        val c: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProductName(databaseProductName)
            .withDatabaseMajorVersion(databaseMajorVersion)
            .withDatabaseMinorVersion(databaseMinorVersion)
            .withDatabaseVersion(databaseVersion)
            .withIdentifierQuoteString(quoteString)
            .withUnquotedCasing(unquotedCasing)
            .withQuotedCasing(quotedCasing)
            .withCaseSensitive(caseSensitive)
            .withNullCollation(nullCollation)
        when (upperProductName) {
            "ACCESS" -> return AccessSqlDialect(c)
            "APACHE DERBY" -> return DerbySqlDialect(c)
            "CLICKHOUSE" -> return ClickHouseSqlDialect(c)
            "DBMS:CLOUDSCAPE" -> return DerbySqlDialect(c)
            "EXASOL" -> return ExasolSqlDialect(c)
            "HIVE" -> return HiveSqlDialect(c)
            "INGRES" -> return IngresSqlDialect(c)
            "INTERBASE" -> return InterbaseSqlDialect(c)
            "JETHRODATA" -> return JethroDataSqlDialect(
                c.withJethroInfo(jethroCache.get(databaseMetaData))
            )
            "LUCIDDB" -> return LucidDbSqlDialect(c)
            "ORACLE" -> return OracleSqlDialect(c)
            "PHOENIX" -> return PhoenixSqlDialect(c)
            "MYSQL (INFOBRIGHT)" -> return InfobrightSqlDialect(c)
            "MYSQL" -> return MysqlSqlDialect(
                c.withDataTypeSystem(MysqlSqlDialect.MYSQL_TYPE_SYSTEM)
            )
            "REDSHIFT" -> return RedshiftSqlDialect(
                c.withDataTypeSystem(RedshiftSqlDialect.TYPE_SYSTEM)
            )
            "SNOWFLAKE" -> return SnowflakeSqlDialect(c)
            "SPARK" -> return SparkSqlDialect(c)
            else -> {}
        }
        // Now the fuzzy matches.
        return if (databaseProductName.startsWith("DB2")) {
            Db2SqlDialect(c)
        } else if (upperProductName.contains("FIREBIRD")) {
            FirebirdSqlDialect(c)
        } else if (databaseProductName.startsWith("Informix")) {
            InformixSqlDialect(c)
        } else if (upperProductName.contains("NETEZZA")) {
            NetezzaSqlDialect(c)
        } else if (upperProductName.contains("PARACCEL")) {
            ParaccelSqlDialect(c)
        } else if (databaseProductName.startsWith("HP Neoview")) {
            NeoviewSqlDialect(c)
        } else if (upperProductName.contains("POSTGRE")) {
            PostgresqlSqlDialect(
                c.withDataTypeSystem(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM)
            )
        } else if (upperProductName.contains("SQL SERVER")) {
            MssqlSqlDialect(c)
        } else if (upperProductName.contains("SYBASE")) {
            SybaseSqlDialect(c)
        } else if (upperProductName.contains("TERADATA")) {
            TeradataSqlDialect(c)
        } else if (upperProductName.contains("HSQL")) {
            HsqldbSqlDialect(c)
        } else if (upperProductName.contains("H2")) {
            H2SqlDialect(c)
        } else if (upperProductName.contains("VERTICA")) {
            VerticaSqlDialect(c)
        } else if (upperProductName.contains("SNOWFLAKE")) {
            SnowflakeSqlDialect(c)
        } else if (upperProductName.contains("SPARK")) {
            SparkSqlDialect(c)
        } else {
            AnsiSqlDialect(c)
        }
    }

    companion object {
        val INSTANCE = SqlDialectFactoryImpl()
        private fun getCasing(databaseMetaData: DatabaseMetaData, quoted: Boolean): Casing {
            return try {
                if (if (quoted) databaseMetaData.storesUpperCaseQuotedIdentifiers() else databaseMetaData.storesUpperCaseIdentifiers()) {
                    Casing.TO_UPPER
                } else if (if (quoted) databaseMetaData.storesLowerCaseQuotedIdentifiers() else databaseMetaData.storesLowerCaseIdentifiers()) {
                    Casing.TO_LOWER
                } else if (if (quoted) databaseMetaData.storesMixedCaseQuotedIdentifiers()
                            || databaseMetaData.supportsMixedCaseQuotedIdentifiers() else databaseMetaData.storesMixedCaseIdentifiers()
                            || databaseMetaData.supportsMixedCaseIdentifiers()
                ) {
                    Casing.UNCHANGED
                } else {
                    Casing.UNCHANGED
                }
            } catch (e: SQLException) {
                throw IllegalArgumentException("cannot deduce casing", e)
            }
        }

        private fun isCaseSensitive(databaseMetaData: DatabaseMetaData): Boolean {
            return try {
                (databaseMetaData.supportsMixedCaseIdentifiers()
                        || databaseMetaData.supportsMixedCaseQuotedIdentifiers())
            } catch (e: SQLException) {
                throw IllegalArgumentException("cannot deduce case-sensitivity", e)
            }
        }

        private fun getNullCollation(databaseMetaData: DatabaseMetaData): NullCollation {
            return try {
                if (databaseMetaData.nullsAreSortedAtEnd()) {
                    NullCollation.LAST
                } else if (databaseMetaData.nullsAreSortedAtStart()) {
                    NullCollation.FIRST
                } else if (databaseMetaData.nullsAreSortedLow()) {
                    NullCollation.LOW
                } else if (databaseMetaData.nullsAreSortedHigh()) {
                    NullCollation.HIGH
                } else if (isBigQuery(databaseMetaData)) {
                    NullCollation.LOW
                } else {
                    throw IllegalArgumentException("cannot deduce null collation")
                }
            } catch (e: SQLException) {
                throw IllegalArgumentException("cannot deduce null collation", e)
            }
        }

        @Throws(SQLException::class)
        private fun isBigQuery(databaseMetaData: DatabaseMetaData): Boolean {
            return databaseMetaData.getDatabaseProductName()
                .equals("Google Big Query")
        }

        private fun getIdentifierQuoteString(databaseMetaData: DatabaseMetaData): String {
            return try {
                databaseMetaData.getIdentifierQuoteString()
            } catch (e: SQLException) {
                throw IllegalArgumentException("cannot deduce identifier quote string", e)
            }
        }

        /** Returns a basic dialect for a given product, or null if none is known.  */
        @Nullable
        fun simple(databaseProduct: SqlDialect.DatabaseProduct?): SqlDialect? {
            return when (databaseProduct) {
                ACCESS -> AccessSqlDialect.DEFAULT
                BIG_QUERY -> BigQuerySqlDialect.DEFAULT
                CALCITE -> CalciteSqlDialect.DEFAULT
                CLICKHOUSE -> ClickHouseSqlDialect.DEFAULT
                DB2 -> Db2SqlDialect.DEFAULT
                DERBY -> DerbySqlDialect.DEFAULT
                EXASOL -> ExasolSqlDialect.DEFAULT
                FIREBIRD -> FirebirdSqlDialect.DEFAULT
                H2 -> H2SqlDialect.DEFAULT
                HIVE -> HiveSqlDialect.DEFAULT
                HSQLDB -> HsqldbSqlDialect.DEFAULT
                INFOBRIGHT -> InfobrightSqlDialect.DEFAULT
                INFORMIX -> InformixSqlDialect.DEFAULT
                INGRES -> IngresSqlDialect.DEFAULT
                INTERBASE -> InterbaseSqlDialect.DEFAULT
                JETHRO -> throw RuntimeException("Jethro does not support simple creation")
                LUCIDDB -> LucidDbSqlDialect.DEFAULT
                MSSQL -> MssqlSqlDialect.DEFAULT
                MYSQL -> MysqlSqlDialect.DEFAULT
                NEOVIEW -> NeoviewSqlDialect.DEFAULT
                NETEZZA -> NetezzaSqlDialect.DEFAULT
                ORACLE -> OracleSqlDialect.DEFAULT
                PARACCEL -> ParaccelSqlDialect.DEFAULT
                PHOENIX -> PhoenixSqlDialect.DEFAULT
                POSTGRESQL -> PostgresqlSqlDialect.DEFAULT
                PRESTO -> PrestoSqlDialect.DEFAULT
                REDSHIFT -> RedshiftSqlDialect.DEFAULT
                SYBASE -> SybaseSqlDialect.DEFAULT
                TERADATA -> TeradataSqlDialect.DEFAULT
                VERTICA -> VerticaSqlDialect.DEFAULT
                SPARK -> SparkSqlDialect.DEFAULT
                SQLSTREAM, UNKNOWN -> null
                else -> null
            }
        }
    }
}
