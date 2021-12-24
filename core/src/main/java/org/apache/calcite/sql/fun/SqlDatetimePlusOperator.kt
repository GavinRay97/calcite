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

import org.apache.calcite.avatica.util.TimeUnit

/**
 * Operator that adds an INTERVAL to a DATETIME.
 */
class SqlDatetimePlusOperator  //~ Constructors -----------------------------------------------------------
internal constructor() : SqlSpecialOperator(
    "+", SqlKind.PLUS, 40, true, ReturnTypes.ARG2_NULLABLE,
    InferTypes.FIRST_KNOWN, OperandTypes.MINUS_DATE_OPERATOR
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun inferReturnType(opBinding: SqlOperatorBinding): RelDataType {
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val leftType: RelDataType = opBinding.getOperandType(0)
        val unitType: IntervalSqlType = opBinding.getOperandType(1) as IntervalSqlType
        val timeUnit: TimeUnit = unitType.getIntervalQualifier().getStartUnit()
        return SqlTimestampAddFunction.deduceType(
            typeFactory, timeUnit,
            unitType, leftType
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall?,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.getDialect().unparseSqlDatetimeArithmetic(
            writer, call, SqlKind.PLUS, leftPrec, rightPrec
        )
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding?): SqlMonotonicity {
        return SqlStdOperatorTable.PLUS.getMonotonicity(call)
    }
}
