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
package org.apache.calcite.sql.validate

import org.apache.calcite.rel.type.RelDataType

/**
 * A scope which contains nothing besides a few parameters. Like
 * [EmptyScope] (which is its base class), it has no parent scope.
 *
 * @see ParameterNamespace
 */
class ParameterScope(
    validator: SqlValidatorImpl?,
    nameToTypeMap: Map<String, RelDataType>
) : EmptyScope(validator) {
    //~ Instance fields --------------------------------------------------------
    /**
     * Map from the simple names of the parameters to types of the parameters
     * ([RelDataType]).
     */
    private val nameToTypeMap: Map<String, RelDataType>

    //~ Constructors -----------------------------------------------------------
    init {
        this.nameToTypeMap = nameToTypeMap
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun fullyQualify(identifier: SqlIdentifier?): SqlQualified {
        return SqlQualified.create(this, 1, null, identifier)
    }

    @Override
    fun getOperandScope(call: SqlCall?): SqlValidatorScope {
        return this
    }

    @Override
    @Nullable
    fun resolveColumn(name: String, ctx: SqlNode?): RelDataType? {
        return nameToTypeMap[name]
    }
}
