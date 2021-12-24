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
import org.apache.calcite.config.NullCollation
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.util.RelToSqlConverterUtil
import com.google.common.base.Preconditions

/**
 * A `SqlDialect` implementation for the Presto database.
 */
class PrestoSqlDialect
/**
 * Creates a PrestoSqlDialect.
 */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun requiresAliasForFromItems(): Boolean {
        return true
    }

    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode?
    ) {
        unparseUsingLimit(writer, offset, fetch)
    }

    @Override
    @Nullable
    fun emulateNullDirection(
        node: SqlNode?,
        nullsFirst: Boolean, desc: Boolean
    ): SqlNode {
        return emulateNullDirectionWithIsNull(node, nullsFirst, desc)
    }

    @Override
    fun supportsAggregateFunction(kind: SqlKind?): Boolean {
        when (kind) {
            AVG, COUNT, CUBE, SUM, MIN, MAX, ROLLUP -> return true
            else -> {}
        }
        return false
    }

    @Override
    fun supportsGroupByWithCube(): Boolean {
        return true
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    @Override
    fun supportsGroupByWithRollup(): Boolean {
        return true
    }

    @get:Override
    val calendarPolicy: CalendarPolicy
        get() = CalendarPolicy.SHIFT

    @Override
    @Nullable
    fun getCastSpec(type: RelDataType?): SqlNode {
        return super.getCastSpec(type)
    }

    @Override
    fun unparseCall(
        writer: SqlWriter?, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        if (call.getOperator() === SqlStdOperatorTable.SUBSTRING) {
            RelToSqlConverterUtil.specialOperatorByName("SUBSTR")
                .unparse(writer, call, 0, 0)
        } else {
            // Current impl is same with Postgresql.
            PostgresqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec)
        }
    }

    @Override
    fun unparseSqlIntervalQualifier(
        writer: SqlWriter?,
        qualifier: SqlIntervalQualifier?, typeSystem: RelDataTypeSystem?
    ) {
        // Current impl is same with MySQL.
        MysqlSqlDialect.DEFAULT.unparseSqlIntervalQualifier(writer, qualifier, typeSystem)
    }

    companion object {
        val DEFAULT_CONTEXT: Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(DatabaseProduct.PRESTO)
            .withIdentifierQuoteString("\"")
            .withUnquotedCasing(Casing.UNCHANGED)
            .withNullCollation(NullCollation.LOW)
        val DEFAULT: SqlDialect = PrestoSqlDialect(DEFAULT_CONTEXT)

        /** Unparses offset/fetch using "OFFSET offset LIMIT fetch " syntax.  */
        private fun unparseUsingLimit(
            writer: SqlWriter, @Nullable offset: SqlNode?,
            @Nullable fetch: SqlNode?
        ) {
            Preconditions.checkArgument(fetch != null || offset != null)
            unparseOffset(writer, offset)
            unparseLimit(writer, fetch)
        }
    }
}
