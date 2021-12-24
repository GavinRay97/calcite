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
package org.apache.calcite.rex

import org.apache.calcite.rel.type.RelDataType

/**
 * Shuttle which creates a deep copy of a Rex expression.
 *
 *
 * This is useful when copying objects from one type factory or builder to
 * another.
 *
 * @see RexBuilder.copy
 */
internal class RexCopier(builder: RexBuilder) : RexShuttle() {
    //~ Instance fields --------------------------------------------------------
    private val builder: RexBuilder
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RexCopier.
     *
     * @param builder Builder
     */
    init {
        this.builder = builder
    }

    //~ Methods ----------------------------------------------------------------
    private fun copy(type: RelDataType): RelDataType {
        return builder.getTypeFactory().copyType(type)
    }

    @Override
    fun visitOver(over: RexOver): RexNode {
        val update: BooleanArray? = null
        return RexOver(
            copy(over.getType()), over.getAggOperator(),
            visitList(over.getOperands(), update), visitWindow(over.getWindow()),
            over.isDistinct(), over.ignoreNulls()
        )
    }

    @Override
    fun visitCall(call: RexCall): RexNode {
        val update: BooleanArray? = null
        return builder.makeCall(
            copy(call.getType()),
            call.getOperator(),
            visitList(call.getOperands(), update)
        )
    }

    @Override
    fun visitCorrelVariable(variable: RexCorrelVariable): RexNode {
        return builder.makeCorrel(copy(variable.getType()), variable.id)
    }

    @Override
    fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
        return builder.makeFieldAccess(
            fieldAccess.getReferenceExpr().accept(this),
            fieldAccess.getField().getIndex()
        )
    }

    @Override
    fun visitInputRef(inputRef: RexInputRef): RexNode {
        return builder.makeInputRef(copy(inputRef.getType()), inputRef.getIndex())
    }

    @Override
    fun visitLocalRef(localRef: RexLocalRef): RexNode {
        return RexLocalRef(localRef.getIndex(), copy(localRef.getType()))
    }

    @Override
    fun visitLiteral(literal: RexLiteral): RexNode {
        // Get the value as is
        return RexLiteral(
            RexLiteral.value(literal), copy(literal.getType()),
            literal.getTypeName()
        )
    }

    @Override
    fun visitDynamicParam(dynamicParam: RexDynamicParam): RexNode {
        return builder.makeDynamicParam(
            copy(dynamicParam.getType()),
            dynamicParam.getIndex()
        )
    }

    @Override
    fun visitRangeRef(rangeRef: RexRangeRef): RexNode {
        return builder.makeRangeReference(
            copy(rangeRef.getType()),
            rangeRef.getOffset(), false
        )
    }
}
