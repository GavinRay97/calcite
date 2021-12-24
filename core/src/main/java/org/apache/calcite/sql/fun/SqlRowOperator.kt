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
 * SqlRowOperator represents the special ROW constructor.
 *
 *
 * TODO: describe usage for row-value construction and row-type construction
 * (SQL supports both).
 */
class SqlRowOperator  //~ Constructors -----------------------------------------------------------
    (name: String?) : SqlSpecialOperator(
    name,
    SqlKind.ROW, MDX_PRECEDENCE,
    false,
    null,
    InferTypes.RETURN_TYPE,
    OperandTypes.VARIADIC
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType {
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        return opBinding.getTypeFactory().createStructType(
            object : AbstractList<Map.Entry<String?, RelDataType?>?>() {
                @Override
                operator fun get(index: Int): Map.Entry<String, RelDataType> {
                    return Pair.of(
                        SqlUtil.deriveAliasFromOrdinal(index),
                        opBinding.getOperandType(index)
                    )
                }

                @Override
                fun size(): Int {
                    return opBinding.getOperandCount()
                }
            })
    }

    @Override
    fun unparse(
        writer: SqlWriter?,
        call: SqlCall?,
        leftPrec: Int,
        rightPrec: Int
    ) {
        SqlUtil.unparseFunctionSyntax(this, writer, call, false)
    }

    // override SqlOperator
    @Override
    fun requiresDecimalExpansion(): Boolean {
        return false
    }
}
