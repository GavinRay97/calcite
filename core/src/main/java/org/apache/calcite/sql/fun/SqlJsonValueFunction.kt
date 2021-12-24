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
 * The `JSON_VALUE` function.
 */
class SqlJsonValueFunction(name: String?) : SqlFunction(
    name, SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(
        { opBinding -> explicitTypeSpec(opBinding).orElse(getDefaultType(opBinding)) },
        SqlTypeTransforms.FORCE_NULLABLE
    ),
    null,
    OperandTypes.family(
        ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER)
    ) { ordinal -> ordinal > 1 },
    SqlFunctionCategory.SYSTEM
) {
    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.between(2, 10)

    @Override
    fun getAllowedSignatures(opNameToUse: String?): String {
        return ("JSON_VALUE(json_doc, path [RETURNING type] "
                + "[{NULL | ERROR | DEFAULT value} ON EMPTY] "
                + "[{NULL | ERROR | DEFAULT value} ON ERROR])")
    }

    @Override
    fun unparse(writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.sep(",", true)
        for (i in 1 until call.operandCount()) {
            call.operand(i).unparse(writer, leftPrec, rightPrec)
        }
        writer.endFunCall(frame)
    }

    companion object {
        /** Returns VARCHAR(2000) as default.  */
        private fun getDefaultType(opBinding: SqlOperatorBinding): RelDataType {
            val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
            return typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000)
        }

        /**
         * Returns new operand list with type specification removed.
         */
        fun removeTypeSpecOperands(call: SqlCall): List<SqlNode> {
            @Nullable val operands: Array<SqlNode?> = call.getOperandList().toArray(arrayOfNulls<SqlNode>(0))
            if (hasExplicitTypeSpec(operands)) {
                operands[2] = null
                operands[3] = null
            }
            return Arrays.stream(operands)
                .filter(Objects::nonNull)
                .collect(Collectors.toList())
        }

        /** Returns the optional explicit returning type specification.  */
        private fun explicitTypeSpec(opBinding: SqlOperatorBinding): Optional<RelDataType> {
            return if (opBinding.getOperandCount() > 2 && opBinding.isOperandLiteral(2, false)
                && opBinding.getOperandLiteralValue(2, Object::class.java) is SqlJsonValueReturning
            ) {
                Optional.of(opBinding.getOperandType(3))
            } else Optional.empty()
        }

        /** Returns whether there is an explicit return type specification.  */
        fun hasExplicitTypeSpec(@Nullable operands: Array<SqlNode?>): Boolean {
            return (operands.size > 2
                    && isReturningTypeSymbol(operands[2]))
        }

        private fun isReturningTypeSymbol(@Nullable node: SqlNode?): Boolean {
            return (node is SqlLiteral
                    && (node as SqlLiteral?).getValue() is SqlJsonValueReturning)
        }
    }
}
