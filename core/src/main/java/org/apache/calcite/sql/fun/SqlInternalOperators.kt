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

import org.apache.calcite.rex.RexCall

/**
 * Contains internal operators.
 *
 *
 * These operators are always created directly, not by looking up a function
 * or operator by name or syntax, and therefore this class does not implement
 * interface [SqlOperatorTable].
 */
object SqlInternalOperators {
    /** Similar to [SqlStdOperatorTable.ROW], but does not print "ROW".
     *
     *
     * For arguments [1, TRUE], ROW would print "`ROW (1, TRUE)`",
     * but this operator prints "`(1, TRUE)`".  */
    val ANONYMOUS_ROW: SqlRowOperator = object : SqlRowOperator("\$ANONYMOUS_ROW") {
        @Override
        override fun unparse(
            writer: SqlWriter, call: SqlCall,
            leftPrec: Int, rightPrec: Int
        ) {
            @SuppressWarnings("assignment.type.incompatible") val operandList: List<SqlNode> = call.getOperandList()
            writer.list(
                SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
                SqlNodeList.of(call.getParserPosition(), operandList)
            )
        }
    }

    /** Similar to [.ANONYMOUS_ROW], but does not print "ROW" or
     * parentheses.
     *
     *
     * For arguments [1, TRUE], prints "`1, TRUE`".  It is used in
     * contexts where parentheses have been printed (because we thought we were
     * about to print "`(ROW (1, TRUE))`") and we wish we had not.  */
    val ANONYMOUS_ROW_NO_PARENTHESES: SqlRowOperator = object : SqlRowOperator("\$ANONYMOUS_ROW_NO_PARENTHESES") {
        @Override
        override fun unparse(
            writer: SqlWriter, call: SqlCall,
            leftPrec: Int, rightPrec: Int
        ) {
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL)
            for (operand in call.getOperandList()) {
                writer.sep(",")
                operand.unparse(writer, leftPrec, rightPrec)
            }
            writer.endList(frame)
        }
    }

    /** "$THROW_UNLESS(condition, message)" throws an error with the given message
     * if condition is not TRUE, otherwise returns TRUE.  */
    val THROW_UNLESS: SqlInternalOperator = SqlInternalOperator("\$THROW_UNLESS", SqlKind.OTHER)

    /** An IN operator for Druid.
     *
     *
     * Unlike the regular
     * [SqlStdOperatorTable.IN] operator it may
     * be used in [RexCall]. It does not require that
     * its operands have consistent types.  */
    val DRUID_IN: SqlInOperator = SqlInOperator(SqlKind.DRUID_IN)

    /** A NOT IN operator for Druid, analogous to [.DRUID_IN].  */
    val DRUID_NOT_IN: SqlInOperator = SqlInOperator(SqlKind.DRUID_NOT_IN)

    /** A BETWEEN operator for Druid, analogous to [.DRUID_IN].  */
    val DRUID_BETWEEN: SqlBetweenOperator = object : SqlBetweenOperator(SqlBetweenOperator.Flag.SYMMETRIC, false) {
        @get:Override
        val kind: SqlKind
            get() = SqlKind.this

        @Override
        override fun validRexOperands(count: Int, litmus: Litmus): Boolean {
            return litmus.succeed()
        }
    }

    /** Separator expression inside GROUP_CONCAT, e.g. '`SEPARATOR ','`'.  */
    val SEPARATOR: SqlOperator = SqlInternalOperator(
        "SEPARATOR", SqlKind.SEPARATOR, 20, false,
        ReturnTypes.ARG0, InferTypes.RETURN_TYPE, OperandTypes.ANY
    )
}
