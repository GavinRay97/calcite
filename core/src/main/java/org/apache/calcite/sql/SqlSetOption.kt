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

import org.apache.calcite.sql.parser.SqlParserPos

/**
 * SQL parse tree node to represent `SET` and `RESET` statements,
 * optionally preceded by `ALTER SYSTEM` or `ALTER SESSION`.
 *
 *
 * Syntax:
 *
 * <blockquote>`
 * ALTER scope SET `option.name` = value;<br></br>
 * ALTER scope RESET `option`.`name`;<br></br>
 * ALTER scope RESET ALL;<br></br>
 * <br></br>
 * SET `option.name` = value;<br></br>
 * RESET `option`.`name`;<br></br>
 * RESET ALL;
`</blockquote> *
 *
 *
 * If [.scope] is null, assume a default scope. (The default scope
 * is defined by the project using Calcite, but is typically SESSION.)
 *
 *
 * If [.value] is null, assume RESET;
 * if [.value] is not null, assume SET.
 *
 *
 * Examples:
 *
 *
 *  * `ALTER SYSTEM SET `my`.`param1` = 1`
 *  * `SET `my.param2` = 1`
 *  * `SET `my.param3` = ON`
 *  * `ALTER SYSTEM RESET `my`.`param1``
 *  * `RESET `my.param2``
 *  * `ALTER SESSION RESET ALL`
 *
 */
class SqlSetOption(
    pos: SqlParserPos?, @Nullable scope: String, name: SqlIdentifier?,
    @Nullable value: SqlNode?
) : SqlAlter(pos, scope) {
    /** Name of the option as an [org.apache.calcite.sql.SqlIdentifier]
     * with one or more parts. */
    var name: SqlIdentifier?

    /** Value of the option. May be a [org.apache.calcite.sql.SqlLiteral] or
     * a [org.apache.calcite.sql.SqlIdentifier] with one
     * part. Reserved words (currently just 'ON') are converted to
     * identifiers by the parser.  */
    @Nullable
    var value: SqlNode?

    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     * @param scope Scope (generally "SYSTEM" or "SESSION"), may be null.
     * @param name Name of option, as an identifier, must not be null.
     * @param value Value of option, as an identifier or literal, may be null.
     * If null, assume RESET command, else assume SET command.
     */
    init {
        scope = scope
        this.name = name
        this.value = value
        assert(name != null)
    }

    @get:Override
    val kind: SqlKind
        get() = SqlKind.SET_OPTION

    @get:Override
    val operator: org.apache.calcite.sql.SqlOperator
        get() = OPERATOR

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() {
            val operandList: List<SqlNode> = ArrayList()
            if (scope == null) {
                operandList.add(null)
            } else {
                operandList.add(SqlIdentifier(scope, SqlParserPos.ZERO))
            }
            operandList.add(name)
            operandList.add(value)
            return ImmutableNullableList.copyOf(operandList)
        }

    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode?) {
        when (i) {
            0 -> if (operand != null) {
                scope = (operand as SqlIdentifier).getSimple()
            } else {
                scope = null
            }
            1 -> name = requireNonNull(operand,  /**/"name") as SqlIdentifier?
            2 -> value = operand
            else -> throw AssertionError(i)
        }
    }

    @Override
    protected fun unparseAlterOperation(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        if (value != null) {
            writer.keyword("SET")
        } else {
            writer.keyword("RESET")
        }
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE)
        name.unparse(writer, leftPrec, rightPrec)
        if (value != null) {
            writer.sep("=")
            value.unparse(writer, leftPrec, rightPrec)
        }
        writer.endList(frame)
    }

    @Override
    fun validate(
        validator: SqlValidator,
        scope: SqlValidatorScope?
    ) {
        if (value != null) {
            validator.validate(value)
        }
    }

    fun getName(): SqlIdentifier? {
        return name
    }

    fun setName(name: SqlIdentifier?) {
        this.name = name
    }

    @Nullable
    fun getValue(): SqlNode? {
        return value
    }

    fun setValue(value: SqlNode?) {
        this.value = value
    }

    companion object {
        val OPERATOR: SqlSpecialOperator = object : SqlSpecialOperator("SET_OPTION", SqlKind.SET_OPTION) {
            @SuppressWarnings("argument.type.incompatible")
            @Override
            override fun createCall(
                @Nullable functionQualifier: SqlLiteral?,
                pos: SqlParserPos?, @Nullable vararg operands: SqlNode?
            ): SqlCall {
                val scopeNode: SqlNode? = operands[0]
                return SqlSetOption(
                    pos,
                    (if (scopeNode == null) null else scopeNode.toString())!!,
                    operands[1] as SqlIdentifier?, operands[2]
                )
            }
        }
    }
}
