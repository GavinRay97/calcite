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

import org.apache.calcite.adapter.enumerable.EnumUtils

/**
 * `SqlOperatorBinding` represents the binding of an
 * [SqlOperator] to actual operands, along with any additional information
 * required to validate those operands if needed.
 */
abstract class SqlOperatorBinding protected constructor(
    typeFactory: RelDataTypeFactory,
    sqlOperator: SqlOperator
) {
    //~ Instance fields --------------------------------------------------------
    protected val typeFactory: RelDataTypeFactory
    private val sqlOperator: SqlOperator
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlOperatorBinding.
     *
     * @param typeFactory Type factory
     * @param sqlOperator Operator which is subject of this call
     */
    init {
        this.typeFactory = typeFactory
        this.sqlOperator = sqlOperator
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * If the operator call occurs in an aggregate query, returns the number of
     * columns in the GROUP BY clause. For example, for "SELECT count(*) FROM emp
     * GROUP BY deptno, gender", returns 2.
     *
     *
     * Returns 0 if the query is implicitly "GROUP BY ()" because of an
     * aggregate expression. For example, "SELECT sum(sal) FROM emp".
     *
     *
     * Returns -1 if the query is not an aggregate query.
     */
    val groupCount: Int
        get() = -1

    /**
     * Returns whether the operator is an aggregate function with a filter.
     */
    fun hasFilter(): Boolean {
        return false
    }

    /** Returns the bound operator.  */
    val operator: org.apache.calcite.sql.SqlOperator
        get() = sqlOperator

    /** Returns the factory for type creation.  */
    fun getTypeFactory(): RelDataTypeFactory {
        return typeFactory
    }

    /**
     * Gets the string value of a string literal operand.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @return string value
     */
    @Deprecated // to be removed before 2.0
    @Nullable
    fun getStringLiteralOperand(ordinal: Int): String {
        throw UnsupportedOperationException()
    }

    /**
     * Gets the integer value of a numeric literal operand.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @return integer value
     */
    @Deprecated // to be removed before 2.0
    fun getIntLiteralOperand(ordinal: Int): Int {
        throw UnsupportedOperationException()
    }

    /**
     * Gets the value of a literal operand.
     *
     *
     * Cases:
     *
     *  * If the operand is not a literal, the value is null.
     *
     *  * If the operand is a string literal,
     * the value will be of type [org.apache.calcite.util.NlsString].
     *
     *  * If the operand is a numeric literal,
     * the value will be of type [java.math.BigDecimal].
     *
     *  * If the operand is an interval qualifier,
     * the value will be of type [SqlIntervalQualifier]
     *
     *  * Otherwise the type is undefined, and the value may be null.
     *
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @param clazz Desired valued type
     *
     * @return value of operand
     */
    fun <T : Object?> getOperandLiteralValue(ordinal: Int, clazz: Class<T>?): @Nullable T? {
        throw UnsupportedOperationException()
    }

    /**
     * Gets the value of a literal operand as a Calcite type.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @param type Desired valued type
     *
     * @return value of operand
     */
    @Nullable
    fun getOperandLiteralValue(ordinal: Int, type: RelDataType): Object? {
        if (type !is RelDataTypeFactoryImpl.JavaType) {
            return null
        }
        val clazz: Class<*> = (type as RelDataTypeFactoryImpl.JavaType).getJavaClass()
        val o: Object = getOperandLiteralValue(ordinal, Object::class.java) ?: return null
        if (clazz.isInstance(o)) {
            return clazz.cast(o)
        }
        val o2: Object = if (o is NlsString) (o as NlsString).getValue() else o
        return EnumUtils.evaluate(o2, clazz)
    }

    @Deprecated // to be removed before 2.0
    @Nullable
    fun getOperandLiteralValue(ordinal: Int): Comparable? {
        return getOperandLiteralValue(ordinal, Comparable::class.java)
    }

    /**
     * Determines whether a bound operand is NULL.
     *
     *
     * This is only relevant for SQL validation.
     *
     * @param ordinal   zero-based ordinal of operand of interest
     * @param allowCast whether to regard CAST(constant) as a constant
     * @return whether operand is null; false for everything except SQL
     * validation
     */
    fun isOperandNull(ordinal: Int, allowCast: Boolean): Boolean {
        throw UnsupportedOperationException()
    }

    /**
     * Determines whether an operand is a literal.
     *
     * @param ordinal   zero-based ordinal of operand of interest
     * @param allowCast whether to regard CAST(literal) as a literal
     * @return whether operand is literal
     */
    fun isOperandLiteral(ordinal: Int, allowCast: Boolean): Boolean {
        throw UnsupportedOperationException()
    }

    /** Returns the number of bound operands.  */
    abstract val operandCount: Int

    /**
     * Gets the type of a bound operand.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @return bound operand type
     */
    abstract fun getOperandType(ordinal: Int): RelDataType

    /**
     * Gets the monotonicity of a bound operand.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @return monotonicity of operand
     */
    fun getOperandMonotonicity(ordinal: Int): SqlMonotonicity {
        return SqlMonotonicity.NOT_MONOTONIC
    }

    /**
     * Collects the types of the bound operands into a list.
     *
     * @return collected list
     */
    fun collectOperandTypes(): List<RelDataType> {
        return object : AbstractList<RelDataType?>() {
            @Override
            operator fun get(index: Int): RelDataType {
                return getOperandType(index)
            }

            @Override
            fun size(): Int {
                return operandCount
            }
        }
    }

    /**
     * Returns the rowtype of the `ordinal`th operand, which is a
     * cursor.
     *
     *
     * This is only implemented for [SqlCallBinding].
     *
     * @param ordinal Ordinal of the operand
     * @return Rowtype of the query underlying the cursor
     */
    @Nullable
    fun getCursorOperand(ordinal: Int): RelDataType {
        throw UnsupportedOperationException()
    }

    /**
     * Retrieves information about a column list parameter.
     *
     * @param ordinal    ordinal position of the column list parameter
     * @param paramName  name of the column list parameter
     * @param columnList returns a list of the column names that are referenced
     * in the column list parameter
     * @return the name of the parent cursor referenced by the column list
     * parameter if it is a column list parameter; otherwise, null is returned
     */
    @Nullable
    fun getColumnListParamInfo(
        ordinal: Int,
        paramName: String?,
        columnList: List<String?>?
    ): String {
        throw UnsupportedOperationException()
    }

    /**
     * Wraps a validation error with context appropriate to this operator call.
     *
     * @param e Validation error, not null
     * @return Error wrapped, if possible, with positional information
     */
    abstract fun newError(
        e: Resources.ExInst<SqlValidatorException?>?
    ): CalciteException?
}
