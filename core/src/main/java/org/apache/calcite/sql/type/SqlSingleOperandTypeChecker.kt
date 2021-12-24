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
 * SqlSingleOperandTypeChecker is an extension of [SqlOperandTypeChecker]
 * for implementations which are capable of checking the type of a single
 * operand in isolation. This isn't meaningful for all type-checking rules (e.g.
 * SameOperandTypeChecker requires two operands to have matching types, so
 * checking one in isolation is meaningless).
 */
interface SqlSingleOperandTypeChecker : SqlOperandTypeChecker {
    //~ Methods ----------------------------------------------------------------
    /**
     * Checks the type of a single operand against a particular ordinal position
     * within a formal operator signature. Note that the actual ordinal position
     * of the operand being checked may be *different* from the position
     * of the formal operand.
     *
     *
     * For example, when validating the actual call
     *
     * <blockquote>
     * <pre>C(X, Y, Z)</pre>
    </blockquote> *
     *
     *
     * the strategy for validating the operand Z might involve checking its
     * type against the formal signature OP(W). In this case,
     * `iFormalOperand` would be zero, even though the position of Z
     * within call C is two.
     *
     *
     * Caution that we could not(shouldn't) implement implicit type coercion for this checker,
     * implicit type coercion has side effect(modify the AST), if this single operand checker is
     * subsumed in a composite rule(OR or AND), we can not make any side effect if we
     * can not make sure that all the single operands type check are passed(with type coercion).
     * But there is an exception: only if the call has just one operand, for this case,
     * use [SqlOperandTypeChecker.checkOperandTypes] instead.
     *
     * @param callBinding    description of the call being checked; this is only
     * provided for context when throwing an exception; the
     * implementation should *NOT* examine the
     * operands of the call as part of the check
     * @param operand        the actual operand to be checked
     * @param iFormalOperand the 0-based formal operand ordinal
     * @param throwOnFailure whether to throw an exception if check fails
     * (otherwise returns false in that case)
     * @return whether check succeeded
     */
    fun checkSingleOperandType(
        callBinding: SqlCallBinding?,
        operand: SqlNode?,
        iFormalOperand: Int,
        throwOnFailure: Boolean
    ): Boolean
}
