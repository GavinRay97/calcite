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
package org.apache.calcite.sql.dialect

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.type.SqlTypeName
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSetMultimap
import com.google.common.collect.LinkedHashMultimap
import com.google.common.collect.Multimap
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.Statement
import java.util.HashMap
import java.util.List
import java.util.Locale
import java.util.Map
import java.util.Objects
import java.util.Set

/**
 * A `SqlDialect` implementation for the JethroData database.
 */
class JethroDataSqlDialect(context: Context) : SqlDialect(context) {
    private val info: JethroInfo

    /** Creates a JethroDataSqlDialect.  */
    init {
        info = context.jethroInfo()
    }

    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    @Nullable
    fun emulateNullDirection(
        node: SqlNode,
        nullsFirst: Boolean, desc: Boolean
    ): SqlNode {
        return node
    }

    @Override
    fun supportsAggregateFunction(kind: SqlKind?): Boolean {
        when (kind) {
            COUNT, SUM, AVG, MIN, MAX, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP -> return true
            else -> {}
        }
        return false
    }

    @Override
    fun supportsFunction(
        operator: SqlOperator,
        type: RelDataType?, paramTypes: List<RelDataType>
    ): Boolean {
        when (operator.getKind()) {
            IS_NOT_NULL, IS_NULL, AND, OR, NOT, BETWEEN, CASE, CAST -> return true
            else -> {}
        }
        val functions: Set<JethroSupportedFunction> = info.supportedFunctions.get(operator.getName())
        if (functions != null) {
            for (f in functions) {
                if (f.argumentsMatch(paramTypes)) {
                    return true
                }
            }
        }
        LOGGER.debug(
            "Unsupported function in jethro: " + operator + " with params "
                    + paramTypes
        )
        return false
    }

    @SuppressWarnings("deprecation")
    @Override
    fun supportsOffsetFetch(): Boolean {
        return false
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    /** Information about a function supported by Jethro.  */
    class JethroSupportedFunction(name: String?, operands: String) {
        private val operandTypes: List<SqlTypeName>

        init {
            Objects.requireNonNull(name, "name") // not currently used
            val b: ImmutableList.Builder<SqlTypeName> = ImmutableList.builder()
            for (strType in operands.split(":")) {
                b.add(parse(strType))
            }
            operandTypes = b.build()
        }

        fun argumentsMatch(paramTypes: List<RelDataType>): Boolean {
            if (paramTypes.size() !== operandTypes.size()) {
                return false
            }
            for (i in 0 until paramTypes.size()) {
                if (paramTypes[i].getSqlTypeName() !== operandTypes[i]) {
                    return false
                }
            }
            return true
        }

        companion object {
            private fun parse(strType: String): SqlTypeName {
                return when (strType.toLowerCase(Locale.ROOT)) {
                    "bigint", "long" -> SqlTypeName.BIGINT
                    "integer", "int" -> SqlTypeName.INTEGER
                    "double" -> SqlTypeName.DOUBLE
                    "float" -> SqlTypeName.FLOAT
                    "string" -> SqlTypeName.VARCHAR
                    "timestamp" -> SqlTypeName.TIMESTAMP
                    else -> SqlTypeName.ANY
                }
            }
        }
    }

    /** Stores information about capabilities of Jethro databases.  */
    interface JethroInfoCache {
        operator fun get(databaseMetaData: DatabaseMetaData?): JethroInfo?
    }

    /** Implementation of `JethroInfoCache`.  */
    private class JethroInfoCacheImpl : JethroInfoCache {
        val map: Map<String, JethroInfo> = HashMap()
        @Override
        override fun get(metaData: DatabaseMetaData): JethroInfo? {
            try {
                assert("JethroData".equals(metaData.getDatabaseProductName()))
                val productVersion: String = metaData.getDatabaseProductVersion()
                synchronized(this@JethroInfoCacheImpl) {
                    var info = map[productVersion]
                    if (info == null) {
                        val c: Connection = metaData.getConnection()
                        info = makeInfo(c)
                        map.put(productVersion, info)
                    }
                    return info
                }
            } catch (e: Exception) {
                LOGGER.error("Failed to create JethroDataDialect", e)
                throw RuntimeException("Failed to create JethroDataDialect", e)
            }
        }

        companion object {
            private fun makeInfo(jethroConnection: Connection): JethroInfo {
                try {
                    jethroConnection.createStatement().use { jethroStatement ->
                        jethroStatement.executeQuery("show functions extended").use { functionsTupleSet ->
                            val supportedFunctions: Multimap<String?, JethroSupportedFunction?> =
                                LinkedHashMultimap.create()
                            while (functionsTupleSet.next()) {
                                val functionName: String = Objects.requireNonNull(
                                    functionsTupleSet.getString(1),
                                    "functionName"
                                )
                                val operandsType: String = Objects.requireNonNull(
                                    functionsTupleSet.getString(3)
                                ) { "operands for $functionName" }
                                supportedFunctions.put(
                                    functionName,
                                    JethroSupportedFunction(functionName, operandsType)
                                )
                            }
                            return JethroInfo(supportedFunctions)
                        }
                    }
                } catch (e: Exception) {
                    val msg = "Jethro server failed to execute 'show functions extended'"
                    LOGGER.error(msg, e)
                    throw RuntimeException("$msg; make sure your Jethro server is up to date", e)
                }
            }
        }
    }

    /** Information about the capabilities of a Jethro database.  */
    class JethroInfo(supportedFunctions: Multimap<String?, JethroSupportedFunction?>?) {
        private val supportedFunctions: ImmutableSetMultimap<String, JethroSupportedFunction>

        init {
            this.supportedFunctions = ImmutableSetMultimap.copyOf(supportedFunctions)
        }

        companion object {
            val EMPTY = JethroInfo(
                ImmutableSetMultimap.of()
            )
        }
    }

    companion object {
        fun createCache(): JethroInfoCache {
            return JethroInfoCacheImpl()
        }
    }
}
