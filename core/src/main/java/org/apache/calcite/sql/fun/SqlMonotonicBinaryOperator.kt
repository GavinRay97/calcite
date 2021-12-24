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

import org.apache.calcite.sql.SqlBinaryOperator

/**
 * Base class for binary operators such as addition, subtraction, and
 * multiplication which are monotonic for the patterns `m op c` and
 * `c op m` where m is any monotonic expression and c is a constant.
 */
class SqlMonotonicBinaryOperator  //~ Constructors -----------------------------------------------------------
    (
    name: String?,
    kind: SqlKind?,
    prec: Int,
    isLeftAssoc: Boolean,
    returnTypeInference: SqlReturnTypeInference?,
    operandTypeInference: SqlOperandTypeInference?,
    operandTypeChecker: SqlOperandTypeChecker?
) : SqlBinaryOperator(
    name,
    kind,
    prec,
    isLeftAssoc,
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        val mono0: SqlMonotonicity = call.getOperandMonotonicity(0)
        val mono1: SqlMonotonicity = call.getOperandMonotonicity(1)

        // constant <op> constant --> constant
        if (mono1 === SqlMonotonicity.CONSTANT
            && mono0 === SqlMonotonicity.CONSTANT
        ) {
            return SqlMonotonicity.CONSTANT
        }

        // monotonic <op> constant
        if (mono1 === SqlMonotonicity.CONSTANT) {
            // mono0 + constant --> mono0
            // mono0 - constant --> mono0
            if (getName().equals("-")
                || getName().equals("+")
            ) {
                return mono0
            }
            assert(getName().equals("*"))
            val value: BigDecimal = call.getOperandLiteralValue(1, BigDecimal::class.java)
            return when (if (value == null) 1 else value.signum()) {
                -1 ->         // mono0 * negative constant --> reverse mono0
                    mono0.reverse()
                0 ->         // mono0 * 0 --> constant (zero)
                    SqlMonotonicity.CONSTANT
                else ->         // mono0 * positive constant --> mono0
                    mono0
            }
        }

        // constant <op> mono
        if (mono0 === SqlMonotonicity.CONSTANT) {
            if (getName().equals("-")) {
                // constant - mono1 --> reverse mono1
                return mono1.reverse()
            }
            if (getName().equals("+")) {
                // constant + mono1 --> mono1
                return mono1
            }
            assert(getName().equals("*"))
            if (!call.isOperandNull(0, true)) {
                val value: BigDecimal = call.getOperandLiteralValue(0, BigDecimal::class.java)
                return when (if (value == null) 1 else value.signum()) {
                    -1 ->           // negative constant * mono1 --> reverse mono1
                        mono1.reverse()
                    0 ->           // 0 * mono1 --> constant (zero)
                        SqlMonotonicity.CONSTANT
                    else ->           // positive constant * mono1 --> mono1
                        mono1
                }
            }
        }

        // strictly asc + strictly asc --> strictly asc
        //   e.g. 2 * orderid + 3 * orderid
        //     is strictly increasing if orderid is strictly increasing
        // asc + asc --> asc
        //   e.g. 2 * orderid + 3 * orderid
        //     is increasing if orderid is increasing
        // asc + desc --> not monotonic
        //   e.g. 2 * orderid + (-3 * orderid) is not monotonic
        if (getName().equals("+")) {
            return if (mono0 === mono1) {
                mono0
            } else if (mono0.unstrict() === mono1.unstrict()) {
                mono0.unstrict()
            } else {
                SqlMonotonicity.NOT_MONOTONIC
            }
        }
        if (getName().equals("-")) {
            return if (mono0 === mono1.reverse()) {
                mono0
            } else if (mono0.unstrict() === mono1.reverse().unstrict()) {
                mono0.unstrict()
            } else {
                SqlMonotonicity.NOT_MONOTONIC
            }
        }
        return if (getName().equals("*")) {
            SqlMonotonicity.NOT_MONOTONIC
        } else super.getMonotonicity(call)
    }
}
