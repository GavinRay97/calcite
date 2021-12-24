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
 * Namespace for UNNEST.
 */
internal class UnnestNamespace(
    validator: SqlValidatorImpl?,
    unnest: SqlCall?,
    scope: SqlValidatorScope?,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    private val unnest: SqlCall?
    private val scope: SqlValidatorScope?

    //~ Constructors -----------------------------------------------------------
    init {
        assert(scope != null)
        assert(unnest.getOperator() is SqlUnnestOperator)
        this.unnest = unnest
        this.scope = scope
    }// When operand of SqlIdentifier type does not have struct, fake a table

    // for UnnestNamespace
    //~ Methods ----------------------------------------------------------------
    @get:Nullable
    @get:Override
    val table: org.apache.calcite.sql.validate.SqlValidatorTable?
        get() {
            val toUnnest: SqlNode = unnest.operand(0)
            if (toUnnest is SqlIdentifier) {
                // When operand of SqlIdentifier type does not have struct, fake a table
                // for UnnestNamespace
                val id: SqlIdentifier = toUnnest as SqlIdentifier
                val qualified: SqlQualified = scope!!.fullyQualify(id)
                return if (qualified.namespace == null) {
                    null
                } else qualified.namespace.getTable()
            }
            return null
        }

    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        // Validate the call and its arguments, and infer the return type.
        validator.validateCall(unnest, scope)
        val type: RelDataType = unnest.getOperator().validateOperands(validator, scope, unnest)
        return toStruct(type, unnest)
    }

    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() = unnest
}
