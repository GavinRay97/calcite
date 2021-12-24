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
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeUtil
import java.util.ArrayList
import java.util.HashSet
import java.util.Set

/**
 * Takes a tree of [RexNode] objects and transforms it into another in one
 * sense equivalent tree. Nodes in tree will be modified and hence tree will not
 * remain unchanged.
 *
 *
 * NOTE: You must validate the tree of RexNodes before using this class.
 */
class RexTransformer(
    root: RexNode,
    rexBuilder: RexBuilder
) {
    //~ Instance fields --------------------------------------------------------
    private var root: RexNode
    private val rexBuilder: RexBuilder
    private var isParentsCount: Int
    private val transformableOperators: Set<SqlOperator> = HashSet()

    //~ Constructors -----------------------------------------------------------
    init {
        this.root = root
        this.rexBuilder = rexBuilder
        isParentsCount = 0
        transformableOperators.add(SqlStdOperatorTable.AND)
        /** NOTE the OR operator is NOT missing.
         * see [org.apache.calcite.test.RexTransformerTest]  */
        transformableOperators.add(SqlStdOperatorTable.EQUALS)
        transformableOperators.add(SqlStdOperatorTable.NOT_EQUALS)
        transformableOperators.add(SqlStdOperatorTable.GREATER_THAN)
        transformableOperators.add(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
        )
        transformableOperators.add(SqlStdOperatorTable.LESS_THAN)
        transformableOperators.add(SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
    }

    private fun isTransformable(node: RexNode): Boolean {
        if (0 == isParentsCount) {
            return false
        }
        if (node is RexCall) {
            val call: RexCall = node as RexCall
            return (!transformableOperators.contains(
                call.getOperator()
            )
                    && isNullable(node))
        }
        return isNullable(node)
    }

    fun transformNullSemantics(): RexNode {
        root = transformNullSemantics(root)
        return root
    }

    private fun transformNullSemantics(node: RexNode): RexNode {
        assert(isParentsCount >= 0) { "Cannot be negative" }
        if (!isBoolean(node)) {
            return node
        }
        var directlyUnderIs: Boolean? = null
        if (node.isA(SqlKind.IS_TRUE)) {
            directlyUnderIs = Boolean.TRUE
            isParentsCount++
        } else if (node.isA(SqlKind.IS_FALSE)) {
            directlyUnderIs = Boolean.FALSE
            isParentsCount++
        }

        // Special case when we have a Literal, Parameter or Identifier directly
        // as an operand to IS TRUE or IS FALSE.
        if (null != directlyUnderIs) {
            val call: RexCall = node as RexCall
            assert(isParentsCount > 0) { "Stack should not be empty" }
            assert(1 == call.operands.size())
            val operand: RexNode = call.operands.get(0)
            if (operand is RexLiteral
                || operand is RexInputRef
                || operand is RexDynamicParam
            ) {
                return if (isNullable(node)) {
                    val notNullNode: RexNode = rexBuilder.makeCall(
                        SqlStdOperatorTable.IS_NOT_NULL,
                        operand
                    )
                    val boolNode: RexNode = rexBuilder.makeLiteral(
                        directlyUnderIs.booleanValue()
                    )
                    val eqNode: RexNode = rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        operand,
                        boolNode
                    )
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        notNullNode,
                        eqNode
                    )
                } else {
                    val boolNode: RexNode = rexBuilder.makeLiteral(
                        directlyUnderIs.booleanValue()
                    )
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        node,
                        boolNode
                    )
                }
            }

            // else continue as normal
        }
        if (node is RexCall) {
            val call: RexCall = node as RexCall

            // Transform children (if any) before transforming node itself.
            val operands: ArrayList<RexNode> = ArrayList()
            for (operand in call.operands) {
                operands.add(transformNullSemantics(operand))
            }
            if (null != directlyUnderIs) {
                isParentsCount--
                directlyUnderIs = null
                return operands.get(0)
            }
            if (transformableOperators.contains(call.getOperator())) {
                assert(2 == operands.size())
                val isNotNullOne: RexNode?
                isNotNullOne = if (isTransformable(operands.get(0))) {
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.IS_NOT_NULL,
                        operands.get(0)
                    )
                } else {
                    null
                }
                val isNotNullTwo: RexNode?
                isNotNullTwo = if (isTransformable(operands.get(1))) {
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.IS_NOT_NULL,
                        operands.get(1)
                    )
                } else {
                    null
                }
                var intoFinalAnd: RexNode? = null
                if (null != isNotNullOne && null != isNotNullTwo) {
                    intoFinalAnd = rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        isNotNullOne,
                        isNotNullTwo
                    )
                } else if (null != isNotNullOne) {
                    intoFinalAnd = isNotNullOne
                } else if (null != isNotNullTwo) {
                    intoFinalAnd = isNotNullTwo
                }
                if (null != intoFinalAnd) {
                    return rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        intoFinalAnd,
                        call.clone(call.getType(), operands)
                    )
                }

                // if come here no need to do anything
            }
            if (!operands.equals(call.operands)) {
                return call.clone(call.getType(), operands)
            }
        }
        return node
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        private fun isBoolean(node: RexNode): Boolean {
            val type: RelDataType = node.getType()
            return SqlTypeUtil.inBooleanFamily(type)
        }

        private fun isNullable(node: RexNode): Boolean {
            return node.getType().isNullable()
        }
    }
}
