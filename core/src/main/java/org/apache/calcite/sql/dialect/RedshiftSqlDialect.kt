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

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.rel.type.RelDataTypeSystemImpl
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName

/**
 * A `SqlDialect` implementation for the Redshift database.
 */
class RedshiftSqlDialect
/** Creates a RedshiftSqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter?, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode?
    ) {
        unparseFetchUsingLimit(writer, offset, fetch)
    }

    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    @Nullable
    fun getCastSpec(type: RelDataType): SqlNode {
        val castSpec: String
        castSpec = when (type.getSqlTypeName()) {
            TINYINT ->       // Redshift has no tinyint (1 byte), so instead cast to smallint or int2 (2 bytes).
                // smallint does not work when enclosed in quotes (i.e.) as "smallint".
                // int2 however works within quotes (i.e.) as "int2".
                // Hence using int2.
                "int2"
            DOUBLE ->       // Redshift has a double type but it is named differently. It is named as double precision or
                // float8.
                // double precision does not work when enclosed in quotes (i.e.) as "double precision".
                // float8 however works within quotes (i.e.) as "float8".
                // Hence using float8.
                "float8"
            else -> return super.getCastSpec(type)
        }
        return SqlDataTypeSpec(
            SqlUserDefinedTypeNameSpec(castSpec, SqlParserPos.ZERO),
            SqlParserPos.ZERO
        )
    }

    companion object {
        val TYPE_SYSTEM: RelDataTypeSystem = object : RelDataTypeSystemImpl() {
            @Override
            fun getMaxPrecision(typeName: SqlTypeName?): Int {
                return when (typeName) {
                    VARCHAR -> 65535
                    CHAR -> 4096
                    else -> super.getMaxPrecision(typeName)
                }
            }

            @get:Override
            val maxNumericPrecision: Int
                get() = 38

            @get:Override
            val maxNumericScale: Int
                get() = 37
        }
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.REDSHIFT)
            .withIdentifierQuoteString("\"")
            .withQuotedCasing(Casing.TO_LOWER)
            .withUnquotedCasing(Casing.TO_LOWER)
            .withCaseSensitive(false)
            .withDataTypeSystem(TYPE_SYSTEM)
        val DEFAULT: SqlDialect = RedshiftSqlDialect(DEFAULT_CONTEXT)
    }
}
