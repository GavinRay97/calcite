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

import org.apache.calcite.sql.type.ReturnTypes

/**
 * SQL function that computes keys by which rows can be partitioned and
 * aggregated.
 *
 *
 * Grouped window functions always occur in the GROUP BY clause. They often
 * have auxiliary functions that access information about the group. For
 * example, `HOP` is a group function, and its auxiliary functions are
 * `HOP_START` and `HOP_END`. Here they are used in a streaming
 * query:
 *
 * <blockquote><pre>
 * SELECT STREAM HOP_START(rowtime, INTERVAL '1' HOUR),
 * HOP_END(rowtime, INTERVAL '1' HOUR),
 * MIN(unitPrice)
 * FROM Orders
 * GROUP BY HOP(rowtime, INTERVAL '1' HOUR), productId
</pre></blockquote> *
 */
class SqlGroupedWindowFunction(
    name: String?, kind: SqlKind?,
    /** The grouped function, if this an auxiliary function; null otherwise.  */
    @field:Nullable @param:Nullable val groupFunction: SqlGroupedWindowFunction?,
    returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?, category: SqlFunctionCategory?
) : SqlFunction(
    name, kind, returnTypeInference, operandTypeInference,
    operandTypeChecker, category
) {
    /** Creates a SqlGroupedWindowFunction.
     *
     * @param name Function name
     * @param kind Kind
     * @param groupFunction Group function, if this is an auxiliary;
     * null, if this is a group function
     * @param returnTypeInference  Strategy to use for return type inference
     * @param operandTypeInference Strategy to use for parameter type inference
     * @param operandTypeChecker   Strategy to use for parameter type checking
     * @param category             Categorization for function
     */
    init {
        Preconditions.checkArgument(
            groupFunction == null
                    || groupFunction.groupFunction == null
        )
    }

    @Deprecated // to be removed before 2.0
    constructor(
        name: String?, kind: SqlKind?,
        @Nullable groupFunction: SqlGroupedWindowFunction?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?
    ) : this(
        name, kind, groupFunction, ReturnTypes.ARG0, null, operandTypeChecker,
        SqlFunctionCategory.SYSTEM
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        kind: SqlKind,
        @Nullable groupFunction: SqlGroupedWindowFunction?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?
    ) : this(
        kind.name(), kind, groupFunction, ReturnTypes.ARG0, null,
        operandTypeChecker, SqlFunctionCategory.SYSTEM
    ) {
    }

    /** Creates an auxiliary function from this grouped window function.
     *
     * @param kind Kind; also determines function name
     */
    fun auxiliary(kind: SqlKind): SqlGroupedWindowFunction {
        return auxiliary(kind.name(), kind)
    }

    /** Creates an auxiliary function from this grouped window function.
     *
     * @param name Function name
     * @param kind Kind
     */
    fun auxiliary(name: String?, kind: SqlKind?): SqlGroupedWindowFunction {
        return SqlGroupedWindowFunction(
            name, kind, this, ReturnTypes.ARG0,
            null, getOperandTypeChecker(), SqlFunctionCategory.SYSTEM
        )
    }

    /** Returns a list of this grouped window function's auxiliary functions.  */
    val auxiliaryFunctions: List<SqlGroupedWindowFunction>
        get() = ImmutableList.of()

    // Auxiliary functions are not group functions
    val isGroup: Boolean
        @Override get() =// Auxiliary functions are not group functions
            groupFunction == null
    val isGroupAuxiliary: Boolean
        @Override get() = groupFunction != null

    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        // Monotonic iff its first argument is, but not strict.
        //
        // Note: This strategy happens to works for all current group functions
        // (HOP, TUMBLE, SESSION). When there are exceptions to this rule, we'll
        // make the method abstract.
        return call.getOperandMonotonicity(0).unstrict()
    }
}
