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
 * A `SqlExplain` is a node of a parse tree which represents an
 * EXPLAIN PLAN statement.
 */
class SqlExplain(
    pos: SqlParserPos?,
    explicandum: SqlNode,
    detailLevel: SqlLiteral,
    depth: SqlLiteral,
    format: SqlLiteral,
    dynamicParameterCount: Int
) : SqlCall(pos) {
    //~ Enums ------------------------------------------------------------------
    /**
     * The level of abstraction with which to display the plan.
     */
    enum class Depth : Symbolizable {
        TYPE, LOGICAL, PHYSICAL
    }

    //~ Instance fields --------------------------------------------------------
    var explicandum: SqlNode
    var detailLevel: SqlLiteral
    var depth: SqlLiteral
    var format: SqlLiteral

    /**
     * Returns the number of dynamic parameters in the statement.
     */
    @get:Pure
    val dynamicParamCount: Int

    //~ Constructors -----------------------------------------------------------
    init {
        this.explicandum = explicandum
        this.detailLevel = detailLevel
        this.depth = depth
        this.format = format
        dynamicParamCount = dynamicParameterCount
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.EXPLAIN

    @get:Override
    val operator: SqlOperator
        get() = SqlExplain.Companion.OPERATOR

    @get:Override
    val operandList: List<Any>
        get() = ImmutableNullableList.of(explicandum, detailLevel, depth, format)

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> explicandum = operand
            1 -> detailLevel = operand as SqlLiteral
            2 -> depth = operand as SqlLiteral
            3 -> format = operand as SqlLiteral
            else -> throw AssertionError(i)
        }
    }

    /**
     * Returns the underlying SQL statement to be explained.
     */
    @Pure
    fun getExplicandum(): SqlNode {
        return explicandum
    }

    /**
     * Return the detail level to be generated.
     */
    @Pure
    fun getDetailLevel(): SqlExplainLevel {
        return detailLevel.getValueAs(SqlExplainLevel::class.java)
    }

    /**
     * Returns the level of abstraction at which this plan should be displayed.
     */
    @Pure
    fun getDepth(): Depth {
        return depth.getValueAs(Depth::class.java)
    }

    /**
     * Returns whether physical plan implementation should be returned.
     */
    @Pure
    fun withImplementation(): Boolean {
        return getDepth() == Depth.PHYSICAL
    }

    /**
     * Returns whether type should be returned.
     */
    @Pure
    fun withType(): Boolean {
        return getDepth() == Depth.TYPE
    }

    /**
     * Returns the desired output format.
     */
    @Pure
    fun getFormat(): SqlExplainFormat {
        return format.getValueAs(SqlExplainFormat::class.java)
    }// to be removed before 2.0

    /**
     * Returns whether result is to be in XML format.
     *
     */
    @get:Deprecated("Use {@link #getFormat()}")
    @get:Deprecated
    val isXml: Boolean
    // to be removed before 2.0 get() {
    return getFormat() === SqlExplainFormat.XML
}

/**
 * Returns whether result is to be in JSON format.
 */
val isJson: Boolean
    get() = getFormat() === SqlExplainFormat.JSON

@Override
fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
    writer.keyword("EXPLAIN PLAN")
    when (getDetailLevel()) {
        NO_ATTRIBUTES -> writer.keyword("EXCLUDING ATTRIBUTES")
        EXPPLAN_ATTRIBUTES -> writer.keyword("INCLUDING ATTRIBUTES")
        ALL_ATTRIBUTES -> writer.keyword("INCLUDING ALL ATTRIBUTES")
        else -> {}
    }
    when (getDepth()) {
        Depth.TYPE -> writer.keyword("WITH TYPE")
        Depth.LOGICAL -> writer.keyword("WITHOUT IMPLEMENTATION")
        Depth.PHYSICAL -> writer.keyword("WITH IMPLEMENTATION")
        else -> throw UnsupportedOperationException()
    }
    when (getFormat()) {
        XML -> writer.keyword("AS XML")
        JSON -> writer.keyword("AS JSON")
        DOT -> writer.keyword("AS DOT")
        else -> {}
    }
    writer.keyword("FOR")
    writer.newlineAndIndent()
    explicandum.unparse(
        writer, operator.getLeftPrec(), operator.getRightPrec()
    )
}

companion object {
    val OPERATOR: SqlSpecialOperator = object : SqlSpecialOperator("EXPLAIN", SqlKind.EXPLAIN) {
        @SuppressWarnings("argument.type.incompatible")
        @Override
        fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?, @Nullable vararg operands: SqlNode
        ): SqlCall {
            return SqlExplain(
                pos, operands[0], operands[1] as SqlLiteral,
                operands[2] as SqlLiteral, operands[3] as SqlLiteral, 0
            )
        }
    }
}
}
