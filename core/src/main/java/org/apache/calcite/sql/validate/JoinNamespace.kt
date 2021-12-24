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
 * Namespace representing the row type produced by joining two relations.
 */
internal class JoinNamespace(validator: SqlValidatorImpl?, join: SqlJoin) : AbstractNamespace(validator, null) {
    //~ Instance fields --------------------------------------------------------
    private val join: SqlJoin

    //~ Constructors -----------------------------------------------------------
    init {
        this.join = join
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        var leftType: RelDataType = validator.getNamespaceOrThrow(join.getLeft()).getRowType()
        var rightType: RelDataType = validator.getNamespaceOrThrow(join.getRight()).getRowType()
        val typeFactory: RelDataTypeFactory = validator.getTypeFactory()
        when (join.getJoinType()) {
            LEFT -> rightType = typeFactory.createTypeWithNullability(rightType, true)
            RIGHT -> leftType = typeFactory.createTypeWithNullability(leftType, true)
            FULL -> {
                leftType = typeFactory.createTypeWithNullability(leftType, true)
                rightType = typeFactory.createTypeWithNullability(rightType, true)
            }
            else -> {}
        }
        return typeFactory.createJoinType(leftType, rightType)
    }

    @get:Nullable
    @get:Override
    val node: SqlNode
        get() = join
}
