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

import org.apache.calcite.rel.type.RelDataType

/** Interval expression.
 *
 *
 * Syntax:
 *
 * <blockquote><pre>INTERVAL numericExpression timeUnit
 *
 * timeUnit: YEAR | MONTH | DAY | HOUR | MINUTE | SECOND</pre></blockquote>
 *
 *
 * Compare with interval literal, whose syntax is
 * `INTERVAL characterLiteral timeUnit [ TO timeUnit ]`.
 */
class SqlIntervalOperator internal constructor() : SqlInternalOperator(
    "INTERVAL", SqlKind.INTERVAL, 0, true, RETURN_TYPE,
    InferTypes.ANY_NULLABLE, OperandTypes.NUMERIC_INTERVAL
) {
    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        writer.keyword("INTERVAL")
        val expression: SqlNode = call.operand(0)
        val intervalQualifier: SqlIntervalQualifier = call.operand(1)
        expression.unparseWithParentheses(
            writer, leftPrec, rightPrec,
            !(expression is SqlLiteral
                    || expression is SqlIdentifier
                    || expression.getKind() === SqlKind.MINUS_PREFIX || writer.isAlwaysUseParentheses())
        )
        assert(intervalQualifier.timeUnitRange.endUnit == null)
        intervalQualifier.unparse(writer, 0, 0)
    }

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        return when (operandsCount) {
            2 -> "{0} {1} {2}" // e.g. "INTERVAL <INTEGER> <INTERVAL HOUR>"
            else -> throw AssertionError()
        }
    }

    companion object {
        private val RETURN_TYPE: SqlReturnTypeInference =
            (SqlReturnTypeInference { opBinding: SqlOperatorBinding -> returnType(opBinding) } as SqlReturnTypeInference)
                .andThen(SqlTypeTransforms.TO_NULLABLE)

        private fun returnType(opBinding: SqlOperatorBinding): RelDataType {
            val intervalQualifier: SqlIntervalQualifier =
                getOperandLiteralValueOrThrow(opBinding, 1, SqlIntervalQualifier::class.java)
            return opBinding.getTypeFactory().createSqlIntervalType(intervalQualifier)
        }
    }
}
