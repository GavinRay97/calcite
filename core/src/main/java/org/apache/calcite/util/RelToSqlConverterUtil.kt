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
package org.apache.calcite.util

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlCharStringLiteral
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.`fun`.SqlTrimFunction
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.`fun`.SqlLibraryOperators.REGEXP_REPLACE
import java.util.Objects.requireNonNull

/**
 * Utilities used by multiple dialect for RelToSql conversion.
 */
object RelToSqlConverterUtil {
    /**
     * For usage of TRIM, LTRIM and RTRIM in Hive, see
     * [Hive UDF usage](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF).
     */
    fun unparseHiveTrim(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val valueToTrim: SqlLiteral = call.operand(1)
        val value: String = requireNonNull(
            valueToTrim.toValue()
        ) { "call.operand(1).toValue() for call $call" }
        if (value.matches("\\s+")) {
            unparseTrimWithSpace(writer, call, leftPrec, rightPrec)
        } else {
            // SELECT TRIM(both 'A' from "ABC") -> SELECT REGEXP_REPLACE("ABC", '^(A)*', '')
            val trimFlag: SqlLiteral = call.operand(0)
            val regexNode: SqlCharStringLiteral = createRegexPatternLiteral(call.operand(1), trimFlag)
            val blankLiteral: SqlCharStringLiteral = SqlLiteral.createCharString("", call.getParserPosition())
            val trimOperands: Array<SqlNode> = arrayOf<SqlNode>(call.operand(2), regexNode, blankLiteral)
            val regexReplaceCall: SqlCall = REGEXP_REPLACE.createCall(SqlParserPos.ZERO, trimOperands)
            regexReplaceCall.unparse(writer, leftPrec, rightPrec)
        }
    }

    /**
     * Unparses TRIM function with value as space.
     *
     *
     * For example :
     *
     * <blockquote><pre>
     * SELECT TRIM(both ' ' from "ABC")  SELECT TRIM(ABC)
    </pre></blockquote> *
     *
     * @param writer writer
     * @param call the call
     */
    private fun unparseTrimWithSpace(
        writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int
    ) {
        val operatorName: String
        val trimFlag: SqlLiteral = call.operand(0)
        operatorName = when (trimFlag.getValueAs(SqlTrimFunction.Flag::class.java)) {
            LEADING -> "LTRIM"
            TRAILING -> "RTRIM"
            else -> call.getOperator().getName()
        }
        val trimFrame: SqlWriter.Frame = writer.startFunCall(operatorName)
        call.operand(2).unparse(writer, leftPrec, rightPrec)
        writer.endFunCall(trimFrame)
    }

    /**
     * Creates regex pattern based on the TRIM flag.
     *
     * @param call     SqlCall contains the values that need to be trimmed
     * @param trimFlag the trimFlag, either BOTH, LEADING or TRAILING
     * @return the regex pattern of the character to be trimmed
     */
    fun createRegexPatternLiteral(call: SqlNode, trimFlag: SqlLiteral): SqlCharStringLiteral {
        val regexPattern: String = requireNonNull(
            (call as SqlCharStringLiteral).toValue()
        ) { "null value for SqlNode $call" }
        val escaped = escapeSpecialChar(regexPattern)
        val builder = StringBuilder()
        when (trimFlag.getValueAs(SqlTrimFunction.Flag::class.java)) {
            LEADING -> builder.append("^(").append(escaped).append(")*")
            TRAILING -> builder.append("(").append(escaped).append(")*$")
            else -> builder.append("^(")
                .append(escaped)
                .append(")*|(")
                .append(escaped)
                .append(")*$")
        }
        return SqlLiteral.createCharString(
            builder.toString(),
            call.getParserPosition()
        )
    }

    /**
     * Escapes the special character.
     *
     * @param inputString the string
     * @return escape character if any special character is present in the string
     */
    private fun escapeSpecialChar(inputString: String): String {
        var inputString = inputString
        val specialCharacters = arrayOf(
            "\\", "^", "$", "{", "}", "[", "]", "(", ")", ".",
            "*", "+", "?", "|", "<", ">", "-", "&", "%", "@"
        )
        for (specialCharacter in specialCharacters) {
            if (inputString.contains(specialCharacter)) {
                inputString = inputString.replace(specialCharacter, "\\" + specialCharacter)
            }
        }
        return inputString
    }

    /** Returns a [SqlSpecialOperator] with given operator name, mainly used for
     * unparse override.  */
    fun specialOperatorByName(opName: String?): SqlSpecialOperator {
        return object : SqlSpecialOperator(opName, SqlKind.OTHER_FUNCTION) {
            @Override
            fun unparse(
                writer: SqlWriter,
                call: SqlCall,
                leftPrec: Int,
                rightPrec: Int
            ) {
                writer.print(getName())
                val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")")
                for (operand in call.getOperandList()) {
                    writer.sep(",")
                    operand.unparse(writer, 0, 0)
                }
                writer.endList(frame)
            }
        }
    }
}
