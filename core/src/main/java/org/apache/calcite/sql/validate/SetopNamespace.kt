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
package org.apache.calcite.sql.validate

import org.apache.calcite.rel.type.RelDataType

/**
 * Namespace based upon a set operation (UNION, INTERSECT, EXCEPT).
 */
class SetopNamespace(
    validator: SqlValidatorImpl?,
    call: SqlCall?,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    private val call: SqlCall?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `SetopNamespace`.
     *
     * @param validator     Validator
     * @param call          Call to set operator
     * @param enclosingNode Enclosing node
     */
    init {
        this.call = call
    }

    //~ Methods ----------------------------------------------------------------
    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() = call

    @Override
    fun getMonotonicity(columnName: String?): SqlMonotonicity {
        var monotonicity: SqlMonotonicity? = null
        val index: Int = getRowType().getFieldNames().indexOf(columnName)
        if (index < 0) {
            return SqlMonotonicity.NOT_MONOTONIC
        }
        for (operand in call.getOperandList()) {
            val namespace: SqlValidatorNamespace = requireNonNull(
                validator.getNamespace(operand)
            ) { "namespace for $operand" }
            monotonicity = combine(
                monotonicity,
                namespace.getMonotonicity(
                    namespace.getRowType().getFieldNames().get(index)
                )
            )
        }
        return Util.first(monotonicity, SqlMonotonicity.NOT_MONOTONIC)
    }

    @Override
    fun validateImpl(targetRowType: RelDataType?): RelDataType {
        return when (call.getKind()) {
            UNION, INTERSECT, EXCEPT -> {
                val scope: SqlValidatorScope = requireNonNull(
                    validator.scopes.get(call)
                ) { "scope for $call" }
                for (operand in call.getOperandList()) {
                    if (!operand.isA(SqlKind.QUERY)) {
                        throw validator.newValidationError(
                            operand,
                            RESOURCE.needQueryOp(operand.toString())
                        )
                    }
                    validator.validateQuery(operand, scope, targetRowType)
                }
                call.getOperator().deriveType(
                    validator,
                    scope,
                    call
                )
            }
            else -> throw AssertionError("Not a query: " + call.getKind())
        }
    }

    companion object {
        private fun combine(
            @Nullable m0: SqlMonotonicity?,
            m1: SqlMonotonicity?
        ): SqlMonotonicity? {
            if (m0 == null) {
                return m1
            }
            if (m1 == null) {
                return m0
            }
            if (m0 === m1) {
                return m0
            }
            if (m0.unstrict() === m1) {
                return m1
            }
            return if (m1.unstrict() === m0) {
                m0
            } else SqlMonotonicity.NOT_MONOTONIC
        }
    }
}
