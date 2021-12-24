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
 * Type Constructor function.
 *
 *
 * Created by the parser, then it is rewritten to proper SqlFunction by
 * the validator to a function defined in a Calcite schema.
 */
class SqlTypeConstructorFunction(
    identifier: SqlIdentifier?,
    type: RelDataType
) : SqlFunction(
    identifier,
    null,
    null,
    ExplicitOperandTypeChecker(type),
    null,
    SqlFunctionCategory.SYSTEM
) {
    private val type: RelDataType

    /**
     * Creates a constructor function for types.
     *
     * @param identifier possibly qualified identifier for function
     * @param type type of data
     */
    init {
        this.type = type
    }

    @Override
    fun inferReturnType(opBinding: SqlOperatorBinding?): RelDataType {
        return type
    }
}
