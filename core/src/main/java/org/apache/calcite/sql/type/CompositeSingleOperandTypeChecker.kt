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
 * Allows multiple
 * [org.apache.calcite.sql.type.SqlSingleOperandTypeChecker] rules to be
 * combined into one rule.
 */
class CompositeSingleOperandTypeChecker  //~ Constructors -----------------------------------------------------------
/**
 * Package private. Use [org.apache.calcite.sql.type.OperandTypes.and],
 * [org.apache.calcite.sql.type.OperandTypes.or].
 */
internal constructor(
    composition: CompositeOperandTypeChecker.Composition,
    allowedRules: ImmutableList<out SqlSingleOperandTypeChecker?>,
    @Nullable allowedSignatures: String?
) : CompositeOperandTypeChecker(composition, allowedRules, allowedSignatures, null), SqlSingleOperandTypeChecker {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    @get:SuppressWarnings("unchecked")
    override val rules: ImmutableList<out SqlSingleOperandTypeChecker?>
        get() = allowedRules as ImmutableList<out SqlSingleOperandTypeChecker?>

    @Override
    override fun checkSingleOperandType(
        callBinding: SqlCallBinding,
        node: SqlNode?,
        iFormalOperand: Int,
        throwOnFailure: Boolean
    ): Boolean {
        assert(allowedRules.size() >= 1)
        val rules: ImmutableList<out SqlSingleOperandTypeChecker?> = rules
        if (composition === Composition.SEQUENCE) {
            return rules.get(iFormalOperand).checkSingleOperandType(
                callBinding, node, 0, throwOnFailure
            )
        }
        var typeErrorCount = 0
        val throwOnAndFailure = (composition === Composition.AND
                && throwOnFailure)
        for (rule in rules) {
            if (!rule.checkSingleOperandType(
                    callBinding,
                    node,
                    iFormalOperand,
                    throwOnAndFailure
                )
            ) {
                typeErrorCount++
            }
        }
        val ret: Boolean
        when (composition) {
            AND -> ret = typeErrorCount == 0
            OR -> ret = typeErrorCount < allowedRules.size()
            else -> throw Util.unexpected(composition)
        }
        if (!ret && throwOnFailure) {
            // In the case of a composite OR, we want to throw an error
            // describing in more detail what the problem was, hence doing the
            // loop again.
            for (rule in rules) {
                rule.checkSingleOperandType(
                    callBinding,
                    node,
                    iFormalOperand,
                    true
                )
            }
            throw callBinding.newValidationSignatureError()
        }
        return ret
    }
}
