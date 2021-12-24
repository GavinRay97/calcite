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
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlOperator
import java.util.List

/**
 * A `SqlDialect` implementation for the Vertica database.
 */
class VerticaSqlDialect
/** Creates a VerticaSqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsNestedAggregations(): Boolean {
        return false
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

    companion object {
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.VERTICA)
            .withIdentifierQuoteString("\"")
            .withUnquotedCasing(Casing.UNCHANGED)
        val DEFAULT: SqlDialect = VerticaSqlDialect(DEFAULT_CONTEXT)
    }
}
