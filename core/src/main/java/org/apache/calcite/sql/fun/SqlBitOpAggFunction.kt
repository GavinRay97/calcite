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

import org.apache.calcite.sql.SqlAggFunction

/**
 * Definition of the `BIT_AND` and `BIT_OR` aggregate functions,
 * returning the bitwise AND/OR of all non-null input values, or null if none.
 *
 *
 * INTEGER and BINARY types are supported:
 * tinyint, smallint, int, bigint, binary, varbinary
 */
class SqlBitOpAggFunction(kind: SqlKind) : SqlAggFunction(
    kind.name(),
    null,
    kind,
    ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
    null,
    OperandTypes.or(OperandTypes.INTEGER, OperandTypes.BINARY),
    SqlFunctionCategory.NUMERIC,
    false,
    false,
    Optionality.FORBIDDEN
) {
    //~ Constructors -----------------------------------------------------------
    /** Creates a SqlBitOpAggFunction.  */
    init {
        Preconditions.checkArgument(kind === SqlKind.BIT_AND || kind === SqlKind.BIT_OR || kind === SqlKind.BIT_XOR)
    }

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
        return if (clazz === SqlSplittableAggFunction::class.java) {
            clazz.cast(SqlSplittableAggFunction.SelfSplitter.INSTANCE)
        } else super.unwrap(clazz)
    }

    @get:Override
    val distinctOptionality: Optionality
        get() {
            val optionality: Optionality
            optionality = when (kind) {
                BIT_AND, BIT_OR -> Optionality.IGNORED
                else -> Optionality.OPTIONAL
            }
            return optionality
        }
}
