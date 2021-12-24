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
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.rel.type.RelDataTypeSystemImpl
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlFloorFunction
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import java.util.List

/**
 * A `SqlDialect` implementation for the PostgreSQL database.
 */
class PostgresqlSqlDialect
/** Creates a PostgresqlSqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    @Nullable
    fun getCastSpec(type: RelDataType): SqlNode {
        val castSpec: String
        castSpec = when (type.getSqlTypeName()) {
            TINYINT ->       // Postgres has no tinyint (1 byte), so instead cast to smallint (2 bytes)
                "smallint"
            DOUBLE ->       // Postgres has a double type but it is named differently
                "double precision"
            else -> return super.getCastSpec(type)
        }
        return SqlDataTypeSpec(
            SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
            SqlParserPos.ZERO
        )
    }

    @Override
    fun supportsFunction(
        operator: SqlOperator,
        type: RelDataType?, paramTypes: List<RelDataType?>?
    ): Boolean {
        return when (operator.kind) {
            LIKE ->       // introduces support for ILIKE as well
                true
            else -> super.supportsFunction(operator, type, paramTypes)
        }
    }

    @Override
    fun requiresAliasForFromItems(): Boolean {
        return true
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    @Override
    fun unparseCall(
        writer: SqlWriter?, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        when (call.getKind()) {
            FLOOR -> {
                if (call.operandCount() !== 2) {
                    super.unparseCall(writer, call, leftPrec, rightPrec)
                    return
                }
                val timeUnitNode: SqlLiteral = call.operand(1)
                val timeUnit: TimeUnitRange = timeUnitNode.getValueAs(TimeUnitRange::class.java)
                val call2: SqlCall = SqlFloorFunction.replaceTimeUnitOperand(
                    call, timeUnit.name(),
                    timeUnitNode.getParserPosition()
                )
                SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false)
            }
            else -> super.unparseCall(writer, call, leftPrec, rightPrec)
        }
    }

    companion object {
        /** PostgreSQL type system.  */
        val POSTGRESQL_TYPE_SYSTEM: RelDataTypeSystem = object : RelDataTypeSystemImpl() {
            @Override
            fun getMaxPrecision(typeName: SqlTypeName?): Int {
                return when (typeName) {
                    VARCHAR ->             // From htup_details.h in postgresql:
                        // MaxAttrSize is a somewhat arbitrary upper limit on the declared size of
                        // data fields of char(n) and similar types.  It need not have anything
                        // directly to do with the *actual* upper limit of varlena values, which
                        // is currently 1Gb (see TOAST structures in postgres.h).  I've set it
                        // at 10Mb which seems like a reasonable number --- tgl 8/6/00. */
                        10 * 1024 * 1024
                    else -> super.getMaxPrecision(typeName)
                }
            }
        }
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
            .withIdentifierQuoteString("\"")
            .withUnquotedCasing(Casing.TO_LOWER)
            .withDataTypeSystem(POSTGRESQL_TYPE_SYSTEM)
        val DEFAULT: SqlDialect = PostgresqlSqlDialect(DEFAULT_CONTEXT)
    }
}
