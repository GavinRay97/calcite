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

import org.apache.calcite.linq4j.Ord

/**
 * Internal operator, by which the parser represents a continued string literal.
 *
 *
 * The string fragments are [SqlLiteral] objects, all of the same type,
 * collected as the operands of an [SqlCall] using this operator. After
 * validation, the fragments will be concatenated into a single literal.
 *
 *
 * For a chain of [org.apache.calcite.sql.SqlCharStringLiteral]
 * objects, a [SqlCollation] object is attached only to the head of the
 * chain.
 */
class SqlLiteralChainOperator  //~ Constructors -----------------------------------------------------------
internal constructor() : SqlSpecialOperator(
    "\$LiteralChain",
    SqlKind.LITERAL_CHAIN,
    80,
    true,  // precedence tighter than the * and || operators
    ReturnTypes.ARG0,
    InferTypes.FIRST_KNOWN,
    OperandTypes.VARIADIC
) {
    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        if (!argTypesValid(callBinding)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError()
            }
            return false
        }
        return true
    }

    // Result type is the same as all the args, but its size is the
    // total size.
    // REVIEW mb 8/8/04: Possibly this can be achieved by combining
    // the strategy useFirstArgType with a new transformer.
    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType {
        // Here we know all the operands have the same type,
        // which has a size (precision), but not a scale.
        val ret: RelDataType = opBinding.getOperandType(0)
        val typeName: SqlTypeName = ret.getSqlTypeName()
        assert(typeName.allowsPrecNoScale()) {
            ("LiteralChain has impossible operand type "
                    + typeName)
        }
        var size = 0
        for (type in opBinding.collectOperandTypes()) {
            size += type.getPrecision()
            assert(type.getSqlTypeName() === typeName)
        }
        return opBinding.getTypeFactory().createSqlType(typeName, size)
    }

    @Override
    fun getAllowedSignatures(opName: String): String {
        return "$opName(...)"
    }

    @Override
    fun validateCall(
        call: SqlCall,
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        // per the SQL std, each string fragment must be on a different line
        val operandList: List<SqlNode> = call.getOperandList()
        for (i in 1 until operandList.size()) {
            val prevPos: SqlParserPos = operandList[i - 1].getParserPosition()
            val operand: SqlNode = operandList[i]
            val pos: SqlParserPos = operand.getParserPosition()
            if (pos.getLineNum() <= prevPos.getLineNum()) {
                throw validator.newValidationError(
                    operand,
                    RESOURCE.stringFragsOnSameLine()
                )
            }
        }
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startList("", "")
        var collation: SqlCollation? = null
        for (operand in Ord.zip(call.getOperandList())) {
            val rand: SqlLiteral = operand.e as SqlLiteral
            if (operand.i > 0) {
                // SQL:2003 says there must be a newline between string
                // fragments.
                writer.newlineAndIndent()
            }
            if (rand is SqlCharStringLiteral) {
                val nls: NlsString = rand.getValueAs(NlsString::class.java)
                if (operand.i === 0) {
                    collation = nls.getCollation()

                    // print with prefix
                    writer.literal(nls.asSql(true, false, writer.getDialect()))
                } else {
                    // print without prefix
                    writer.literal(nls.asSql(false, false, writer.getDialect()))
                }
            } else if (operand.i === 0) {
                // print with prefix
                rand.unparse(writer, leftPrec, rightPrec)
            } else {
                // print without prefix
                if (rand.getTypeName() === SqlTypeName.BINARY) {
                    val bs: BitString = rand.getValueAs(BitString::class.java)
                    writer.literal("'" + bs.toHexString().toString() + "'")
                } else {
                    writer.literal("'" + rand.toValue().toString() + "'")
                }
            }
        }
        if (collation != null) {
            collation.unparse(writer)
        }
        writer.endList(frame)
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        // all operands must be the same type
        private fun argTypesValid(callBinding: SqlCallBinding): Boolean {
            if (callBinding.getOperandCount() < 2) {
                return true // nothing to compare
            }
            var firstType: RelDataType? = null
            for (operand in Ord.zip(callBinding.operands())) {
                val type: RelDataType = SqlTypeUtil.deriveType(callBinding, operand.e)
                if (operand.i === 0) {
                    firstType = type
                } else if (!SqlTypeUtil.sameNamedType(castNonNull(firstType), type)) {
                    return false
                }
            }
            return true
        }

        /**
         * Concatenates the operands of a call to this operator.
         */
        fun concatenateOperands(call: SqlCall): SqlLiteral {
            val operandList: List<SqlNode> = call.getOperandList()
            assert(operandList.size() > 0)
            assert(operandList[0] is SqlLiteral) { operandList[0].getClass() }
            return SqlUtil.concatenateLiterals(
                Util.cast(operandList, SqlLiteral::class.java)
            )
        }
    }
}
