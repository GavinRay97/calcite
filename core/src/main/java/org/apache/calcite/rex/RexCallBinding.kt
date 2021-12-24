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

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.runtime.CalciteException
import org.apache.calcite.runtime.Resources
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlOperatorBinding
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlValidatorException
import com.google.common.collect.ImmutableList
import java.util.List

/**
 * `RexCallBinding` implements [SqlOperatorBinding] by
 * referring to an underlying collection of [RexNode] operands.
 */
class RexCallBinding(
    typeFactory: RelDataTypeFactory?,
    sqlOperator: SqlOperator?,
    operands: List<RexNode?>?,
    inputCollations: List<RelCollation?>?
) : SqlOperatorBinding(typeFactory, sqlOperator) {
    //~ Instance fields --------------------------------------------------------
    private val operands: List<RexNode>
    private val inputCollations: List<RelCollation>

    //~ Constructors -----------------------------------------------------------
    init {
        this.operands = ImmutableList.copyOf(operands)
        this.inputCollations = ImmutableList.copyOf(inputCollations)
    }

    //~ Methods ----------------------------------------------------------------
    @SuppressWarnings("deprecation")
    @Override
    @Nullable
    fun getStringLiteralOperand(ordinal: Int): String {
        return RexLiteral.stringValue(operands[ordinal])
    }

    @SuppressWarnings("deprecation")
    @Override
    fun getIntLiteralOperand(ordinal: Int): Int {
        return RexLiteral.intValue(operands[ordinal])
    }

    @Override
    fun <T> getOperandLiteralValue(ordinal: Int, clazz: Class<T>): @Nullable T? {
        val node: RexNode = operands[ordinal]
        return if (node is RexLiteral) {
            (node as RexLiteral).getValueAs(clazz)
        } else clazz.cast(RexLiteral.value(node))
    }

    @Override
    fun getOperandMonotonicity(ordinal: Int): SqlMonotonicity {
        val operand: RexNode = operands[ordinal]
        if (operand is RexInputRef) {
            for (ic in inputCollations) {
                if (ic.getFieldCollations().isEmpty()) {
                    continue
                }
                for (rfc in ic.getFieldCollations()) {
                    if (rfc.getFieldIndex() === (operand as RexInputRef).getIndex()) {
                        return rfc.direction.monotonicity()
                        // TODO: Is it possible to have more than one RelFieldCollation for a RexInputRef?
                    }
                }
            }
        } else if (operand is RexCall) {
            val binding = create(typeFactory, operand as RexCall, inputCollations)
            return (operand as RexCall).getOperator().getMonotonicity(binding)
        }
        return SqlMonotonicity.NOT_MONOTONIC
    }

    @Override
    fun isOperandNull(ordinal: Int, allowCast: Boolean): Boolean {
        return RexUtil.isNullLiteral(operands[ordinal], allowCast)
    }

    @Override
    fun isOperandLiteral(ordinal: Int, allowCast: Boolean): Boolean {
        return RexUtil.isLiteral(operands[ordinal], allowCast)
    }

    fun operands(): List<RexNode> {
        return operands
    }

    // implement SqlOperatorBinding
    @get:Override
    val operandCount: Int
        get() = operands.size()

    // implement SqlOperatorBinding
    @Override
    fun getOperandType(ordinal: Int): RelDataType {
        return operands[ordinal].getType()
    }

    @Override
    fun newError(
        e: Resources.ExInst<SqlValidatorException?>?
    ): CalciteException {
        return SqlUtil.newContextException(SqlParserPos.ZERO, e)
    }

    /** To be compatible with `SqlCall`, CAST needs to pretend that it
     * has two arguments, the second of which is the target type.  */
    private class RexCastCallBinding internal constructor(
        typeFactory: RelDataTypeFactory?,
        sqlOperator: SqlOperator?, operands: List<RexNode?>?,
        type: RelDataType,
        inputCollations: List<RelCollation?>?
    ) : RexCallBinding(typeFactory, sqlOperator, operands, inputCollations) {
        private val type: RelDataType

        init {
            this.type = type
        }

        @Override
        override fun getOperandType(ordinal: Int): RelDataType {
            return if (ordinal == 1) {
                type
            } else super.getOperandType(ordinal)
        }
    }

    companion object {
        /** Creates a binding of the appropriate type.  */
        fun create(
            typeFactory: RelDataTypeFactory?,
            call: RexCall,
            inputCollations: List<RelCollation?>?
        ): RexCallBinding {
            return create(typeFactory, call, null, inputCollations)
        }

        /** Creates a binding of the appropriate type, optionally with a program.  */
        fun create(
            typeFactory: RelDataTypeFactory?,
            call: RexCall, @Nullable program: RexProgram?,
            inputCollations: List<RelCollation?>?
        ): RexCallBinding {
            val operands: List<RexNode> =
                if (program != null) program.expandList(call.getOperands()) else call.getOperands()
            return when (call.getKind()) {
                CAST -> RexCastCallBinding(
                    typeFactory, call.getOperator(),
                    operands, call.getType(), inputCollations
                )
                else -> RexCallBinding(
                    typeFactory, call.getOperator(),
                    operands, inputCollations
                )
            }
        }
    }
}
