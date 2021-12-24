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

import org.apache.calcite.rel.type.RelDataType

/**
 * The `UNNEST` operator.
 */
class SqlUnnestOperator     //~ Constructors -----------------------------------------------------------
    (
    /** Whether `WITH ORDINALITY` was specified.
     *
     *
     * If so, the returned records include a column `ORDINALITY`.  */
    val withOrdinality: Boolean
) : SqlFunctionalOperator(
    "UNNEST",
    SqlKind.UNNEST,
    200,
    true,
    null,
    null,
    OperandTypes.repeat(
        SqlOperandCountRanges.from(1),
        OperandTypes.SCALAR_OR_RECORD_COLLECTION_OR_MAP
    )
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun inferReturnType(opBinding: SqlOperatorBinding): RelDataType {
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val builder: RelDataTypeFactory.Builder = typeFactory.builder()
        for (operand in Util.range(opBinding.getOperandCount())) {
            var type: RelDataType = opBinding.getOperandType(operand)
            if (type.getSqlTypeName() === SqlTypeName.ANY) {
                // Unnest Operator in schema less systems returns one column as the output
                // $unnest is a place holder to specify that one column with type ANY is output.
                return builder
                    .add(
                        "\$unnest",
                        SqlTypeName.ANY
                    )
                    .nullable(true)
                    .build()
            }
            if (type.isStruct()) {
                type = type.getFieldList().get(0).getType()
            }
            assert(
                type is ArraySqlType || type is MultisetSqlType
                        || type is MapSqlType
            )
            if (type is MapSqlType) {
                val mapType: MapSqlType = type as MapSqlType
                builder.add(MAP_KEY_COLUMN_NAME, mapType.getKeyType())
                builder.add(MAP_VALUE_COLUMN_NAME, mapType.getValueType())
            } else {
                val componentType: RelDataType = requireNonNull(type.getComponentType(), "componentType")
                if (!allowAliasUnnestItems(opBinding) && componentType.isStruct()) {
                    builder.addAll(componentType.getFieldList())
                } else {
                    builder.add(
                        SqlUtil.deriveAliasFromOrdinal(operand),
                        componentType
                    )
                }
            }
        }
        if (withOrdinality) {
            builder.add(ORDINALITY_COLUMN_NAME, SqlTypeName.INTEGER)
        }
        return builder.build()
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        if (call.operandCount() === 1
            && call.getOperandList().get(0).getKind() === SqlKind.SELECT
        ) {
            // avoid double ( ) on unnesting a sub-query
            writer.keyword(getName())
            call.operand(0).unparse(writer, 0, 0)
        } else {
            super.unparse(writer, call, leftPrec, rightPrec)
        }
        if (withOrdinality) {
            writer.keyword("WITH ORDINALITY")
        }
    }

    @Override
    fun argumentMustBeScalar(ordinal: Int): Boolean {
        return false
    }

    companion object {
        const val ORDINALITY_COLUMN_NAME = "ORDINALITY"
        const val MAP_KEY_COLUMN_NAME = "KEY"
        const val MAP_VALUE_COLUMN_NAME = "VALUE"
        private fun allowAliasUnnestItems(operatorBinding: SqlOperatorBinding): Boolean {
            return (operatorBinding is SqlCallBinding
                    && (operatorBinding as SqlCallBinding)
                .getValidator()
                .config()
                .sqlConformance()
                .allowAliasUnnestItems())
        }
    }
}
