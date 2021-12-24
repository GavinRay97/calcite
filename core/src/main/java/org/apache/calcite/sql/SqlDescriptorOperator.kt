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

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.type.SqlOperandCountRanges
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Static.RESOURCE

/**
 *
 * DESCRIPTOR appears as an argument in a function. DESCRIPTOR accepts a list of
 * identifiers that represent a list of names. The interpretation of names is left
 * to the function.
 *
 *
 * A typical syntax is DESCRIPTOR(col_name, ...).
 *
 *
 * An example is a table-valued function that takes names of columns to filter on.
 */
class SqlDescriptorOperator : SqlOperator(
    "DESCRIPTOR",
    SqlKind.DESCRIPTOR,
    100,
    100,
    { opBinding -> opBinding.typeFactory.createSqlType(SqlTypeName.COLUMN_LIST) },
    null,
    null
) {
    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?, call: SqlCall?
    ): RelDataType {
        return validator.getTypeFactory().createSqlType(SqlTypeName.COLUMN_LIST)
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        for (operand in callBinding.getCall().getOperandList()) {
            if (operand !is SqlIdentifier
                || (operand as SqlIdentifier).isSimple()
            ) {
                if (throwOnFailure) {
                    throw SqlUtil.newContextException(
                        operand.getParserPosition(),
                        RESOURCE.aliasMustBeSimpleIdentifier()
                    )
                }
            }
        }
        return true
    }

    val operandCountRange: SqlOperandCountRange
        @Override get() = SqlOperandCountRanges.from(1)
    val syntax: SqlSyntax
        @Override get() = SqlSyntax.FUNCTION
}
