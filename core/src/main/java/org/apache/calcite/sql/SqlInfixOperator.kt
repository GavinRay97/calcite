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

import org.apache.calcite.linq4j.Ord

/**
 * A generalization of a binary operator to involve several (two or more)
 * arguments, and keywords between each pair of arguments.
 *
 *
 * For example, the `BETWEEN` operator is ternary, and has syntax
 * `*exp1* BETWEEN *exp2* AND *exp3*`.
 */
class SqlInfixOperator protected constructor(
    names: Array<String?>,
    kind: SqlKind?,
    precedence: Int,
    @Nullable returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?
) : SqlSpecialOperator(
    names[0],
    kind,
    precedence,
    true,
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker
) {
    //~ Instance fields --------------------------------------------------------
    private val names: Array<String?>

    //~ Constructors -----------------------------------------------------------
    init {
        assert(names.size > 1)
        this.names = names
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() === names.size + 1)
        val needWhitespace: Boolean = needsSpace()
        for (operand in Ord.zip(call.getOperandList())) {
            if (operand.i > 0) {
                writer.setNeedWhitespace(needWhitespace)
                writer.keyword(names[operand.i - 1])
                writer.setNeedWhitespace(needWhitespace)
            }
            operand.e.unparse(writer, leftPrec, getLeftPrec())
        }
    }
}
