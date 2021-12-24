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

/**
 * Base class for time functions such as "LOCALTIME", "LOCALTIME(n)".
 */
class SqlAbstractTimeFunction(name: String?, typeName: SqlTypeName) : SqlFunction(
    name, SqlKind.OTHER_FUNCTION, null, null, OTC_CUSTOM,
    SqlFunctionCategory.TIMEDATE
) {
    //~ Instance fields --------------------------------------------------------
    private val typeName: SqlTypeName

    //~ Constructors -----------------------------------------------------------
    init {
        this.typeName = typeName
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.FUNCTION_ID

    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType {
        // REVIEW jvs 20-Feb-2005: Need to take care of time zones.
        var precision = 0
        if (opBinding.getOperandCount() === 1) {
            val type: RelDataType = opBinding.getOperandType(0)
            if (SqlTypeUtil.isNumeric(type)) {
                precision = getOperandLiteralValueOrThrow(opBinding, 0, Integer::class.java)
            }
        }
        assert(precision >= 0)
        if (precision > SqlTypeName.MAX_DATETIME_PRECISION) {
            throw opBinding.newError(
                RESOURCE.argumentMustBeValidPrecision(
                    opBinding.getOperator().getName(), 0,
                    SqlTypeName.MAX_DATETIME_PRECISION
                )
            )
        }
        return opBinding.getTypeFactory().createSqlType(typeName, precision)
    }

    // All of the time functions are increasing. Not strictly increasing.
    @Override
    fun getMonotonicity(call: SqlOperatorBinding?): SqlMonotonicity {
        return SqlMonotonicity.INCREASING
    }

    // Plans referencing context variables should never be cached
    @get:Override
    val isDynamicFunction: Boolean
        get() = true

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val OTC_CUSTOM: SqlOperandTypeChecker = OperandTypes.or(
            OperandTypes.POSITIVE_INTEGER_LITERAL, OperandTypes.NILADIC
        )
    }
}
