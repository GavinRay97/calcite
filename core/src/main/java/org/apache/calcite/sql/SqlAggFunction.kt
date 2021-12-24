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

import org.apache.calcite.linq4j.function.Experimental
import org.apache.calcite.plan.Context
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.sql.`fun`.SqlBasicAggFunction
import org.apache.calcite.sql.type.SqlOperandTypeChecker
import org.apache.calcite.sql.type.SqlOperandTypeInference
import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Optionality
import java.util.List
import java.util.Objects

/**
 * Abstract base class for the definition of an aggregate function: an operator
 * which aggregates sets of values into a result.
 *
 * @see SqlBasicAggFunction
 */
abstract class SqlAggFunction protected constructor(
    name: String?,
    @Nullable sqlIdentifier: SqlIdentifier?,
    kind: SqlKind?,
    returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?,
    funcType: SqlFunctionCategory?,
    private val requiresOrder: Boolean,
    private val requiresOver: Boolean,
    requiresGroupOrder: Optionality?
) : SqlFunction(
    name, sqlIdentifier, kind, returnTypeInference, operandTypeInference,
    operandTypeChecker, funcType
), Context {
    private val requiresGroupOrder: Optionality
    //~ Constructors -----------------------------------------------------------
    /** Creates a built-in SqlAggFunction.  */
    @Deprecated // to be removed before 2.0
    protected constructor(
        name: String?,
        kind: SqlKind?,
        returnTypeInference: SqlReturnTypeInference?,
        @Nullable operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        funcType: SqlFunctionCategory?
    ) : this(
        name, null, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, funcType, false, false,
        Optionality.FORBIDDEN
    ) {
        // We leave sqlIdentifier as null to indicate that this is a builtin.
    }

    /** Creates a user-defined SqlAggFunction.  */
    @Deprecated // to be removed before 2.0
    protected constructor(
        name: String?,
        @Nullable sqlIdentifier: SqlIdentifier?,
        kind: SqlKind?,
        returnTypeInference: SqlReturnTypeInference?,
        @Nullable operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        funcType: SqlFunctionCategory?
    ) : this(
        name, sqlIdentifier, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, funcType, false, false,
        Optionality.FORBIDDEN
    ) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        name: String?,
        @Nullable sqlIdentifier: SqlIdentifier?,
        kind: SqlKind?,
        returnTypeInference: SqlReturnTypeInference?,
        @Nullable operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        funcType: SqlFunctionCategory?,
        requiresOrder: Boolean,
        requiresOver: Boolean
    ) : this(
        name, sqlIdentifier, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, funcType, requiresOrder, requiresOver,
        Optionality.FORBIDDEN
    ) {
    }

    /** Creates a built-in or user-defined SqlAggFunction or window function.
     *
     *
     * A user-defined function will have a value for `sqlIdentifier`; for
     * a built-in function it will be null.  */
    init {
        this.requiresGroupOrder = Objects.requireNonNull(requiresGroupOrder, "requiresGroupOrder")
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
        return if (clazz.isInstance(this)) clazz.cast(this) else null
    }

    @get:Override
    val isAggregator: Boolean
        get() = true

    @get:Override
    val isQuantifierAllowed: Boolean
        get() = true

    @Override
    fun validateCall(
        call: SqlCall?,
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        super.validateCall(call, validator, scope, operandScope)
        validator.validateAggregateParams(call, null, null, null, scope)
    }

    @Override
    fun requiresOrder(): Boolean {
        return requiresOrder
    }

    /** Returns whether this aggregate function must, may, or must not contain a
     * `WITHIN GROUP (ORDER ...)` clause.
     *
     *
     * Cases:
     *
     *  * If [Optionality.MANDATORY],
     * then `AGG(x) WITHIN GROUP (ORDER BY 1)` is valid,
     * and `AGG(x)` is invalid.
     *
     *  * If [Optionality.OPTIONAL],
     * then `AGG(x) WITHIN GROUP (ORDER BY 1)`
     * and `AGG(x)` are both valid.
     *
     *  * If [Optionality.IGNORED],
     * then `AGG(x)` is valid,
     * and `AGG(x) WITHIN GROUP (ORDER BY 1)` is valid but is
     * treated the same as `AGG(x)`.
     *
     *  * If [Optionality.FORBIDDEN],
     * then `AGG(x) WITHIN GROUP (ORDER BY 1)` is invalid,
     * and `AGG(x)` is valid.
     *
     */
    fun requiresGroupOrder(): Optionality {
        return requiresGroupOrder
    }

    @Override
    fun requiresOver(): Boolean {
        return requiresOver
    }

    /** Returns whether this aggregate function allows the `DISTINCT`
     * keyword.
     *
     *
     * The default implementation returns [Optionality.OPTIONAL],
     * which is appropriate for most aggregate functions, including `SUM`
     * and `COUNT`.
     *
     *
     * Some aggregate functions, for example `MIN`, produce the same
     * result with or without `DISTINCT`, and therefore return
     * [Optionality.IGNORED] to indicate this. For such functions,
     * Calcite will probably remove `DISTINCT` while optimizing the query.
     */
    val distinctOptionality: Optionality
        get() = Optionality.OPTIONAL

    @Deprecated // to be removed before 2.0
    fun getParameterTypes(typeFactory: RelDataTypeFactory?): List<RelDataType> {
        throw UnsupportedOperationException("remove before calcite-2.0")
    }

    @Deprecated // to be removed before 2.0
    fun getReturnType(typeFactory: RelDataTypeFactory?): RelDataType {
        throw UnsupportedOperationException("remove before calcite-2.0")
    }

    /** Whether this aggregate function allows a `FILTER (WHERE ...)`
     * clause.  */
    fun allowsFilter(): Boolean {
        return true
    }

    /** Returns whether this aggregate function allows specifying null treatment
     * (`RESPECT NULLS` or `IGNORE NULLS`).  */
    fun allowsNullTreatment(): Boolean {
        return false
    }

    /**
     * Gets rollup aggregation function.
     */
    @get:Nullable
    val rollup: SqlAggFunction?
        get() = null

    /** Returns whether this aggregate function is a PERCENTILE function.
     * Such functions require a `WITHIN GROUP` clause that has precisely
     * one sort key.
     *
     *
     * NOTE: This API is experimental and subject to change without notice.  */
    @get:Experimental
    val isPercentile: Boolean
        get() = false
}
