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
 * Concrete implementation of [SqlAggFunction].
 *
 *
 * The class is final, and instances are immutable.
 *
 *
 * Instances are created only by [SqlBasicAggFunction.create] and are
 * "modified" by "wither" methods such as [.withDistinct] to create a new
 * instance with one property changed. Since the class is final, you can modify
 * behavior only by providing strategy objects, not by overriding methods in a
 * sub-class.
 */
class SqlBasicAggFunction private constructor(
    name: String, @Nullable sqlIdentifier: SqlIdentifier?,
    kind: SqlKind, returnTypeInference: SqlReturnTypeInference,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    operandTypeChecker: SqlOperandTypeChecker, funcType: SqlFunctionCategory,
    requiresOrder: Boolean, requiresOver: Boolean,
    requiresGroupOrder: Optionality, distinctOptionality: Optionality,
    syntax: SqlSyntax, allowsNullTreatment: Boolean, allowsSeparator: Boolean,
    percentile: Boolean
) : SqlAggFunction(
    name, sqlIdentifier, kind,
    requireNonNull(returnTypeInference, "returnTypeInference"), operandTypeInference,
    requireNonNull(operandTypeChecker, "operandTypeChecker"),
    requireNonNull(funcType, "funcType"), requiresOrder, requiresOver,
    requiresGroupOrder
) {
    private val distinctOptionality: Optionality
    private val syntax: SqlSyntax
    private val allowsNullTreatment: Boolean
    private val allowsSeparator: Boolean

    @get:Override
    val isPercentile: Boolean

    //~ Constructors -----------------------------------------------------------
    init {
        this.distinctOptionality = requireNonNull(distinctOptionality, "distinctOptionality")
        this.syntax = requireNonNull(syntax, "syntax")
        this.allowsNullTreatment = allowsNullTreatment
        this.allowsSeparator = allowsSeparator
        isPercentile = percentile
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun deriveType(
        validator: SqlValidator?,
        scope: SqlValidatorScope?, call: SqlCall
    ): RelDataType {
        var strippedCall: SqlCall = call
        if (syntax === SqlSyntax.ORDERED_FUNCTION) {
            if (allowsSeparator) {
                strippedCall = ReturnTypes.stripSeparator(strippedCall)
            }
            strippedCall = ReturnTypes.stripOrderBy(strippedCall)
        }
        val derivedType: RelDataType = super.deriveType(validator, scope, strippedCall)

        // Assigning back the operands that might have been casted by validator
        for (i in 0 until strippedCall.getOperandList().size()) {
            call.setOperand(i, strippedCall.getOperandList().get(i))
        }
        return derivedType
    }

    @Override
    fun getDistinctOptionality(): Optionality {
        return distinctOptionality
    }

    // constructor ensures it is non-null
    @get:Override
    val returnTypeInference: SqlReturnTypeInference
        get() =// constructor ensures it is non-null
            requireNonNull(super.getReturnTypeInference(), "returnTypeInference")

    // constructor ensures it is non-null
    @get:Override
    val operandTypeChecker: SqlOperandTypeChecker
        get() =// constructor ensures it is non-null
            requireNonNull(super.getOperandTypeChecker(), "operandTypeChecker")

    /** Sets [.getDistinctOptionality].  */
    fun withDistinct(distinctOptionality: Optionality): SqlBasicAggFunction {
        return SqlBasicAggFunction(
            getName(), getSqlIdentifier(), kind,
            returnTypeInference, getOperandTypeInference(),
            operandTypeChecker, getFunctionType(), requiresOrder(),
            requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
            allowsNullTreatment, allowsSeparator, isPercentile
        )
    }

    /** Sets [.getFunctionType].  */
    fun withFunctionType(category: SqlFunctionCategory): SqlBasicAggFunction {
        return SqlBasicAggFunction(
            getName(), getSqlIdentifier(), kind,
            returnTypeInference, getOperandTypeInference(),
            operandTypeChecker, category, requiresOrder(),
            requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
            allowsNullTreatment, allowsSeparator, isPercentile
        )
    }

    @Override
    fun getSyntax(): SqlSyntax {
        return syntax
    }

    /** Sets [.getSyntax].  */
    fun withSyntax(syntax: SqlSyntax): SqlBasicAggFunction {
        return SqlBasicAggFunction(
            getName(), getSqlIdentifier(), kind,
            returnTypeInference, getOperandTypeInference(),
            operandTypeChecker, getFunctionType(), requiresOrder(),
            requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
            allowsNullTreatment, allowsSeparator, isPercentile
        )
    }

    @Override
    fun allowsNullTreatment(): Boolean {
        return allowsNullTreatment
    }

    /** Sets [.allowsNullTreatment].  */
    fun withAllowsNullTreatment(allowsNullTreatment: Boolean): SqlBasicAggFunction {
        return SqlBasicAggFunction(
            getName(), getSqlIdentifier(), kind,
            returnTypeInference, getOperandTypeInference(),
            operandTypeChecker, getFunctionType(), requiresOrder(),
            requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
            allowsNullTreatment, allowsSeparator, isPercentile
        )
    }

    /** Returns whether this aggregate function allows '`SEPARATOR string`'
     * among its arguments.  */
    fun allowsSeparator(): Boolean {
        return allowsSeparator
    }

    /** Sets [.allowsSeparator].  */
    fun withAllowsSeparator(allowsSeparator: Boolean): SqlBasicAggFunction {
        return SqlBasicAggFunction(
            getName(), getSqlIdentifier(), kind,
            returnTypeInference, getOperandTypeInference(),
            operandTypeChecker, getFunctionType(), requiresOrder(),
            requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
            allowsNullTreatment, allowsSeparator, isPercentile
        )
    }

    /** Sets [.isPercentile].  */
    fun withPercentile(percentile: Boolean): SqlBasicAggFunction {
        return SqlBasicAggFunction(
            getName(), getSqlIdentifier(), kind,
            returnTypeInference, getOperandTypeInference(),
            operandTypeChecker, getFunctionType(), requiresOrder(),
            requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
            allowsNullTreatment, allowsSeparator, percentile
        )
    }

    /** Sets [.requiresGroupOrder].  */
    fun withGroupOrder(groupOrder: Optionality): SqlBasicAggFunction {
        return SqlBasicAggFunction(
            getName(), getSqlIdentifier(), kind,
            returnTypeInference, getOperandTypeInference(),
            operandTypeChecker, getFunctionType(), requiresOrder(),
            requiresOver(), groupOrder, distinctOptionality, syntax,
            allowsNullTreatment, allowsSeparator, isPercentile
        )
    }

    companion object {
        /** Creates a SqlBasicAggFunction whose name is the same as its kind.  */
        fun create(
            kind: SqlKind,
            returnTypeInference: SqlReturnTypeInference,
            operandTypeChecker: SqlOperandTypeChecker
        ): SqlBasicAggFunction {
            return create(kind.name(), kind, returnTypeInference, operandTypeChecker)
        }

        /** Creates a SqlBasicAggFunction.  */
        fun create(
            name: String, kind: SqlKind,
            returnTypeInference: SqlReturnTypeInference,
            operandTypeChecker: SqlOperandTypeChecker
        ): SqlBasicAggFunction {
            return SqlBasicAggFunction(
                name, null, kind, returnTypeInference, null,
                operandTypeChecker, SqlFunctionCategory.NUMERIC, false, false,
                Optionality.FORBIDDEN, Optionality.OPTIONAL, SqlSyntax.FUNCTION, false,
                false, false
            )
        }
    }
}
