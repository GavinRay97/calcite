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

/** Operator that tests whether its left operand is included in the range of
 * values covered by search arguments.  */
internal object SqlSearchOperator : SqlInternalOperator() {
    /** Sets whether a call to SEARCH should allow nulls.
     *
     *
     * For example, if the type of `x` is NOT NULL, then
     * `SEARCH(x, Sarg[10])` will never return UNKNOWN.
     * It is evident from the expansion, "x = 10", but holds for all Sarg
     * values.
     *
     *
     * If [Sarg.nullAs] is TRUE or FALSE, SEARCH will never return
     * UNKNOWN. For example, `SEARCH(x, Sarg[10; NULL AS UNKNOWN])` expands
     * to `x = 10 OR x IS NOT NULL`, which returns `TRUE` if
     * `x` is NULL, `TRUE` if `x` is 10, and `FALSE`
     * for all other values.
     */
    private fun makeNullable(
        binding: SqlOperatorBinding,
        type: RelDataType
    ): RelDataType {
        val nullable = (binding.getOperandType(0).isNullable()
                && getOperandLiteralValueOrThrow(binding, 1, Sarg::class.java).nullAs
                === RexUnknownAs.UNKNOWN)
        return binding.getTypeFactory().createTypeWithNullability(type, nullable)
    }
}
