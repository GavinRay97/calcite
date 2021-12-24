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
package org.apache.calcite.sql.type

import org.apache.calcite.sql.SqlCallBinding

/**
 * Strategy interface to check for allowed operand types of an operator call.
 *
 *
 * This interface is an example of the
 * [strategy pattern][org.apache.calcite.util.Glossary.STRATEGY_PATTERN].
 *
 * @see OperandTypes
 */
interface SqlOperandTypeChecker {
    //~ Methods ----------------------------------------------------------------
    /**
     * Checks the types of all operands to an operator call.
     *
     * @param callBinding    description of the call to be checked
     * @param throwOnFailure whether to throw an exception if check fails
     * (otherwise returns false in that case)
     * @return whether check succeeded
     */
    fun checkOperandTypes(
        callBinding: SqlCallBinding?,
        throwOnFailure: Boolean
    ): Boolean

    /** Returns the range of operand counts allowed in a call.  */
    val operandCountRange: SqlOperandCountRange?

    /**
     * Returns a string describing the allowed formal signatures of a call, e.g.
     * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     *
     * @param op     the operator being checked
     * @param opName name to use for the operator in case of aliasing
     * @return generated string
     */
    fun getAllowedSignatures(op: SqlOperator?, opName: String?): String?

    /** Returns the strategy for making the arguments have consistency types.  */
    val consistency: Consistency?

    /** Returns whether the `i`th operand is optional.  */
    fun isOptional(i: Int): Boolean

    /** Returns whether the list of parameters is fixed-length. In standard SQL,
     * user-defined functions are fixed-length.
     *
     *
     * If true, the validator should expand calls, supplying a `DEFAULT`
     * value for each parameter for which an argument is not supplied.  */
    val isFixedParameters: Boolean
        get() = false

    /** Converts this type checker to a type inference; returns null if not
     * possible.  */
    @Nullable
    fun typeInference(): SqlOperandTypeInference? {
        return null
    }

    /** Strategy used to make arguments consistent.  */
    enum class Consistency {
        /** Do not try to make arguments consistent.  */
        NONE,

        /** Make arguments of consistent type using comparison semantics.
         * Character values are implicitly converted to numeric, date-time, interval
         * or boolean.  */
        COMPARE,

        /** Convert all arguments to the least restrictive type.  */
        LEAST_RESTRICTIVE
    }
}
