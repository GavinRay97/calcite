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
 * An operator describing a window function specification.
 *
 *
 * Operands are as follows:
 *
 *
 *  * 0: name of window function ([org.apache.calcite.sql.SqlCall])
 *
 *  * 1: window name ([org.apache.calcite.sql.SqlLiteral]) or
 * window in-line specification ([SqlWindow])
 *
 *
 */
class SqlOverOperator  //~ Constructors -----------------------------------------------------------
    : SqlBinaryOperator(
    "OVER",
    SqlKind.OVER,
    20,
    true,
    ReturnTypes.ARG0_FORCE_NULLABLE,
    null,
    OperandTypes.ANY_IGNORE
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun validateCall(
        call: SqlCall,
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        assert(call.getOperator() === this)
        assert(call.operandCount() === 2)
        var aggCall: SqlCall = call.operand(0)
        when (aggCall.getKind()) {
            RESPECT_NULLS, IGNORE_NULLS -> {
                validator.validateCall(aggCall, scope)
                aggCall = aggCall.operand(0)
            }
            else -> {}
        }
        if (!aggCall.getOperator().isAggregator()) {
            throw validator.newValidationError(aggCall, RESOURCE.overNonAggregate())
        }
        val window: SqlNode = call.operand(1)
        validator.validateWindow(window, scope, aggCall)
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType {
        // Validate type of the inner aggregate call
        validateOperands(validator, scope, call)

        // Assume the first operand is an aggregate call and derive its type.
        // When we are sure the window is not empty, pass that information to the
        // aggregate's operator return type inference as groupCount=1
        // Otherwise pass groupCount=0 so the agg operator understands the window
        // can be empty
        val agg: SqlNode = call.operand(0)
        if (agg !is SqlCall) {
            throw IllegalStateException(
                "Argument to SqlOverOperator"
                        + " should be SqlCall, got " + agg.getClass() + ": " + agg
            )
        }
        val window: SqlNode = call.operand(1)
        val w: SqlWindow = validator.resolveWindow(window, scope)
        val groupCount = if (w.isAlwaysNonEmpty()) 1 else 0
        val aggCall: SqlCall = agg as SqlCall
        val opBinding: SqlCallBinding = object : SqlCallBinding(validator, scope, aggCall) {
            @get:Override
            val groupCount: Int
                get() = groupCount
        }
        val ret: RelDataType = aggCall.getOperator().inferReturnType(opBinding)

        // Copied from validateOperands
        (validator as SqlValidatorImpl).setValidatedNodeType(call, ret)
        (validator as SqlValidatorImpl).setValidatedNodeType(agg, ret)
        return ret
    }

    /**
     * Accepts a [SqlVisitor], and tells it to visit each child.
     *
     * @param visitor Visitor
     */
    @Override
    fun <R> acceptCall(
        visitor: SqlVisitor<R>?,
        call: SqlCall,
        onlyExpressions: Boolean,
        argHandler: SqlBasicVisitor.ArgHandler<R>
    ) {
        if (onlyExpressions) {
            for (operand in Ord.zip(call.getOperandList())) {
                // if the second param is an Identifier then it's supposed to
                // be a name from a window clause and isn't part of the
                // group by check
                if (operand == null) {
                    continue
                }
                if (operand.i === 1 && operand.e is SqlIdentifier) {
                    continue
                }
                argHandler.visitChild(visitor, call, operand.i, operand.e)
            }
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler)
        }
    }
}
