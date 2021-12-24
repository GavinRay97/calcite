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

import org.apache.calcite.sql.SqlCall

/**
 * A special operator for the subtraction of two DATETIMEs. The format of
 * DATETIME subtraction is:
 *
 * <blockquote>`"(" <datetime> "-" <datetime> ")"
 * <interval qualifier>`</blockquote>
 *
 *
 * This operator is special since it needs to hold the
 * additional interval qualifier specification, when in [SqlCall] form.
 * In [org.apache.calcite.rex.RexNode] form, it has only two parameters,
 * and the return type describes the desired type of interval.
 */
class SqlDatetimeSubtractionOperator  //~ Constructors -----------------------------------------------------------
    : SqlSpecialOperator(
    "-",
    SqlKind.MINUS,
    40,
    true,
    ReturnTypes.ARG2_NULLABLE,
    InferTypes.FIRST_KNOWN, OperandTypes.MINUS_DATE_OPERATOR
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall?,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.getDialect().unparseSqlDatetimeArithmetic(
            writer, call, SqlKind.MINUS, leftPrec, rightPrec
        )
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding?): SqlMonotonicity {
        return SqlStdOperatorTable.MINUS.getMonotonicity(call)
    }
}
