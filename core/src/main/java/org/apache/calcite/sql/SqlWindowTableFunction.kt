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

import org.apache.calcite.linq4j.Ord

/**
 * Base class for a table-valued function that computes windows. Examples
 * include `TUMBLE`, `HOP` and `SESSION`.
 */
class SqlWindowTableFunction
/** Creates a window table function with a given name.  */
    (name: String?, operandMetadata: SqlOperandMetadata?) : SqlFunction(
    name, SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
    operandMetadata, SqlFunctionCategory.SYSTEM
), SqlTableFunction {
    @get:Nullable
    @get:Override
    val operandTypeChecker: SqlOperandMetadata
        get() = super.getOperandTypeChecker() as SqlOperandMetadata

    @get:Override
    override val rowTypeInference: SqlReturnTypeInference
        get() = ARG0_TABLE_FUNCTION_WINDOWING

    /**
     * {@inheritDoc}
     *
     *
     * Overrides because the first parameter of
     * table-value function windowing is an explicit TABLE parameter,
     * which is not scalar.
     */
    @Override
    fun argumentMustBeScalar(ordinal: Int): Boolean {
        return ordinal != 0
    }

    /** Partial implementation of operand type checker.  */
    protected abstract class AbstractOperandMetadata internal constructor(
        paramNames: List<String?>,
        mandatoryParamCount: Int
    ) : SqlOperandMetadata {
        val paramNames: List<String>
        val mandatoryParamCount: Int

        init {
            this.paramNames = ImmutableList.copyOf(paramNames)
            this.mandatoryParamCount = mandatoryParamCount
            Preconditions.checkArgument(
                mandatoryParamCount >= 0
                        && mandatoryParamCount <= paramNames.size()
            )
        }

        @get:Override
        val operandCountRange: SqlOperandCountRange
            get() = SqlOperandCountRanges.between(
                mandatoryParamCount,
                paramNames.size()
            )

        @Override
        fun paramTypes(typeFactory: RelDataTypeFactory): List<RelDataType> {
            return Collections.nCopies(
                paramNames.size(),
                typeFactory.createSqlType(SqlTypeName.ANY)
            )
        }

        @Override
        fun paramNames(): List<String> {
            return paramNames
        }

        @get:Override
        val consistency: Consistency
            get() = Consistency.NONE

        @Override
        fun isOptional(i: Int): Boolean {
            return (i > operandCountRange.getMin()
                    && i <= operandCountRange.getMax())
        }

        fun throwValidationSignatureErrorOrReturnFalse(
            callBinding: SqlCallBinding,
            throwOnFailure: Boolean
        ): Boolean {
            return if (throwOnFailure) {
                throw callBinding.newValidationSignatureError()
            } else {
                false
            }
        }

        /**
         * Checks whether the heading operands are in the form
         * `(ROW, DESCRIPTOR, DESCRIPTOR ..., other params)`,
         * returning whether successful, and throwing if any columns are not found.
         *
         * @param callBinding The call binding
         * @param descriptorCount The number of descriptors following the first
         * operand (e.g. the table)
         *
         * @return true if validation passes; throws if any columns are not found
         */
        fun checkTableAndDescriptorOperands(
            callBinding: SqlCallBinding,
            descriptorCount: Int
        ): Boolean {
            val operand0: SqlNode = callBinding.operand(0)
            val validator: SqlValidator = callBinding.getValidator()
            val type: RelDataType = validator.getValidatedNodeType(operand0)
            if (type.getSqlTypeName() !== SqlTypeName.ROW) {
                return false
            }
            for (i in 1 until descriptorCount + 1) {
                val operand: SqlNode = callBinding.operand(i)
                if (operand.getKind() !== SqlKind.DESCRIPTOR) {
                    return false
                }
                validateColumnNames(
                    validator, type.getFieldNames(),
                    (operand as SqlCall).getOperandList()
                )
            }
            return true
        }

        /**
         * Checks whether the type that the operand of time col descriptor refers to is valid.
         *
         * @param callBinding The call binding
         * @param pos The position of the descriptor at the operands of the call
         * @return true if validation passes, false otherwise
         */
        fun checkTimeColumnDescriptorOperand(callBinding: SqlCallBinding, pos: Int): Boolean {
            val validator: SqlValidator = callBinding.getValidator()
            val operand0: SqlNode = callBinding.operand(0)
            val type: RelDataType = validator.getValidatedNodeType(operand0)
            val operands: List<SqlNode> = (callBinding.operand(pos) as SqlCall).getOperandList()
            val identifier: SqlIdentifier = operands[0] as SqlIdentifier
            val columnName: String = identifier.getSimple()
            val matcher: SqlNameMatcher = validator.getCatalogReader().nameMatcher()
            for (field in type.getFieldList()) {
                if (matcher.matches(field.getName(), columnName)) {
                    return SqlTypeUtil.isTimestamp(field.getType())
                }
            }
            return false
        }

        /**
         * Checks whether the operands starting from position `startPos` are
         * all of type `INTERVAL`, returning whether successful.
         *
         * @param callBinding The call binding
         * @param startPos    The start position to validate (starting index is 0)
         *
         * @return true if validation passes
         */
        fun checkIntervalOperands(callBinding: SqlCallBinding, startPos: Int): Boolean {
            val validator: SqlValidator = callBinding.getValidator()
            for (i in startPos until callBinding.getOperandCount()) {
                val type: RelDataType = validator.getValidatedNodeType(callBinding.operand(i))
                if (!SqlTypeUtil.isInterval(type)) {
                    return false
                }
            }
            return true
        }

        fun validateColumnNames(
            validator: SqlValidator,
            fieldNames: List<String?>?, columnNames: List<SqlNode>
        ) {
            val matcher: SqlNameMatcher = validator.getCatalogReader().nameMatcher()
            Ord.forEach(SqlIdentifier.simpleNames(columnNames)) { name, i ->
                if (matcher.indexOf(fieldNames, name) < 0) {
                    val columnName: SqlIdentifier = columnNames[i] as SqlIdentifier
                    throw SqlUtil.newContextException(
                        columnName.getParserPosition(),
                        RESOURCE.unknownIdentifier(name)
                    )
                }
            }
        }
    }

    companion object {
        /** The data source which the table function computes with.  */
        protected const val PARAM_DATA = "DATA"

        /** The time attribute column. Also known as the event time.  */
        protected const val PARAM_TIMECOL = "TIMECOL"

        /** The window duration INTERVAL.  */
        protected const val PARAM_SIZE = "SIZE"

        /** The optional align offset for each window.  */
        protected const val PARAM_OFFSET = "OFFSET"

        /** The session key(s), only used for SESSION window.  */
        protected const val PARAM_KEY = "KEY"

        /** The slide interval, only used for HOP window.  */
        protected const val PARAM_SLIDE = "SLIDE"

        /**
         * Type-inference strategy whereby the row type of a table function call is a
         * ROW, which is combined from the row type of operand #0 (which is a TABLE)
         * and two additional fields. The fields are as follows:
         *
         *
         *  1. `window_start`: TIMESTAMP type to indicate a window's start
         *  1. `window_end`: TIMESTAMP type to indicate a window's end
         *
         */
        val ARG0_TABLE_FUNCTION_WINDOWING: SqlReturnTypeInference =
            SqlReturnTypeInference { opBinding: SqlOperatorBinding -> inferRowType(opBinding) }

        /** Helper for [.ARG0_TABLE_FUNCTION_WINDOWING].  */
        private fun inferRowType(opBinding: SqlOperatorBinding): RelDataType {
            val inputRowType: RelDataType = opBinding.getOperandType(0)
            val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
            return typeFactory.builder()
                .kind(inputRowType.getStructKind())
                .addAll(inputRowType.getFieldList())
                .add("window_start", SqlTypeName.TIMESTAMP, 3)
                .add("window_end", SqlTypeName.TIMESTAMP, 3)
                .build()
        }
    }
}
