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

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.config.NullCollation
import org.apache.calcite.sql.JoinType
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlFunctionCategory
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlFloorFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.util.RelToSqlConverterUtil.unparseHiveTrim

/**
 * A `SqlDialect` implementation for the APACHE SPARK database.
 */
class SparkSqlDialect
/**
 * Creates a SparkSqlDialect.
 */
    (context: SqlDialect.Context?) : SqlDialect(context) {
    @Override
    protected fun allowsAs(): Boolean {
        return false
    }

    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun emulateJoinTypeForCrossJoin(): JoinType {
        return JoinType.CROSS
    }

    @Override
    fun supportsGroupByWithRollup(): Boolean {
        return true
    }

    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
    }

    @Override
    fun supportsGroupByWithCube(): Boolean {
        return true
    }

    @Override
    fun unparseOffsetFetch(
        writer: SqlWriter?, @Nullable offset: SqlNode?,
        @Nullable fetch: SqlNode?
    ) {
        unparseFetchUsingLimit(writer, offset, fetch)
    }

    @Override
    fun unparseCall(
        writer: SqlWriter?, call: SqlCall,
        leftPrec: Int, rightPrec: Int
    ) {
        if (call.getOperator() === SqlStdOperatorTable.SUBSTRING) {
            SqlUtil.unparseFunctionSyntax(SPARKSQL_SUBSTRING, writer, call, false)
        } else {
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
                TRIM -> unparseHiveTrim(writer, call, leftPrec, rightPrec)
                else -> super.unparseCall(writer, call, leftPrec, rightPrec)
            }
        }
    }

    companion object {
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.SPARK)
            .withNullCollation(NullCollation.LOW)
        val DEFAULT: SqlDialect = SparkSqlDialect(DEFAULT_CONTEXT)
        private val SPARKSQL_SUBSTRING: SqlFunction = SqlFunction(
            "SUBSTRING", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
            SqlFunctionCategory.STRING
        )
    }
}
