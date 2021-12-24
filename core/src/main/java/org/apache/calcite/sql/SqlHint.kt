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
 * A `SqlHint` is a node of a parse tree which represents
 * a sql hint expression.
 *
 *
 * Basic hint grammar is: hint_name[(option1, option2 ...)].
 * The hint_name should be a simple identifier, the options part is optional.
 * Every option can be of four formats:
 *
 *
 *  * simple identifier
 *  * literal
 *  * key value pair whose key is a simple identifier and value is a string literal
 *  * key value pair whose key and value are both string literal
 *
 *
 *
 * The option format can not be mixed in, they should either be all simple identifiers
 * or all literals or all key value pairs.
 *
 *
 * We support 2 kinds of hints in the parser:
 *
 *  * Query hint, right after the select keyword, i.e.:
 * <pre>
 * select &#47;&#42;&#43; hint1, hint2, ... &#42;&#47; ...
</pre> *
 *
 *  * Table hint: right after the referenced table name, i.e.:
 * <pre>
 * select f0, f1, f2 from t1 &#47;&#42;&#43; hint1, hint2, ... &#42;&#47; ...
</pre> *
 *
 *
 */
class SqlHint(
    pos: SqlParserPos?,
    name: SqlIdentifier,
    options: SqlNodeList,
    optionFormat: HintOptionFormat
) : SqlCall(pos) {
    //~ Instance fields --------------------------------------------------------
    private val name: SqlIdentifier
    private val options: SqlNodeList
    /** Returns the hint option format.  */
    val optionFormat: HintOptionFormat

    //~ Constructors -----------------------------------------------------------
    init {
        this.name = name
        this.optionFormat = optionFormat
        this.options = options
    }

    //~ Methods ----------------------------------------------------------------
    val operator: SqlOperator
        @Override get() = OPERATOR
    val operandList: List<Any>
        @Override get() = ImmutableList.of(name, options, optionFormat.symbol(SqlParserPos.ZERO))

    /**
     * Returns the sql hint name.
     */
    fun getName(): String {
        return name.getSimple()
    }

    /**
     * Returns a string list if the hint option is a list of
     * simple SQL identifier, or a list of literals,
     * else returns an empty list.
     */
    val optionList: List<String>
        get() = if (optionFormat == HintOptionFormat.ID_LIST) {
            ImmutableList.copyOf(SqlIdentifier.simpleNames(options))
        } else if (optionFormat == HintOptionFormat.LITERAL_LIST) {
            options.stream()
                .map { node ->
                    val literal: SqlLiteral = node as SqlLiteral
                    requireNonNull(
                        literal.toValue()
                    ) { "null hint literal in $options" }
                }
                .collect(Util.toImmutableList())
        } else {
            ImmutableList.of()
        }

    /**
     * Returns a key value string map if the hint option is a list of
     * pair, each pair contains a simple SQL identifier and a string literal;
     * else returns an empty map.
     */
    val optionKVPairs: Map<String, String>
        get() = if (optionFormat == HintOptionFormat.KV_LIST) {
            val attrs: Map<String, String> = HashMap()
            var i = 0
            while (i < options.size() - 1) {
                val k: SqlNode = options.get(i)
                val v: SqlNode = options.get(i + 1)
                attrs.put(
                    getOptionKeyAsString(k), (v as SqlLiteral).getValueAs(
                        String::class.java
                    )
                )
                i += 2
            }
            ImmutableMap.copyOf(attrs)
        } else {
            ImmutableMap.of()
        }

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        name.unparse(writer, leftPrec, rightPrec)
        if (options.size() > 0) {
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")")
            var i = 0
            while (i < options.size()) {
                val option: SqlNode = options.get(i)
                val nextOption: SqlNode? = if (i < options.size() - 1) options.get(i + 1) else null
                writer.sep(",", false)
                option.unparse(writer, leftPrec, rightPrec)
                if (optionFormat == HintOptionFormat.KV_LIST && nextOption != null) {
                    writer.keyword("=")
                    nextOption.unparse(writer, leftPrec, rightPrec)
                    i += 1
                }
                i++
            }
            writer.endList(frame)
        }
    }

    /** Enumeration that represents hint option format.  */
    enum class HintOptionFormat : Symbolizable {
        /**
         * The hint has no options.
         */
        EMPTY,

        /**
         * The hint options are as literal list.
         */
        LITERAL_LIST,

        /**
         * The hint options are as simple identifier list.
         */
        ID_LIST,

        /**
         * The hint options are list of key-value pairs.
         * For each pair,
         * the key is a simple identifier or string literal,
         * the value is a string literal.
         */
        KV_LIST
    }

    companion object {
        private val OPERATOR: SqlOperator = object : SqlSpecialOperator("HINT", SqlKind.HINT) {
            @Override
            fun createCall(
                @Nullable functionQualifier: SqlLiteral?,
                pos: SqlParserPos?,
                @Nullable vararg operands: SqlNode?
            ): SqlCall {
                return SqlHint(
                    pos,
                    requireNonNull(operands[0], "name") as SqlIdentifier,
                    requireNonNull(operands[1], "options") as SqlNodeList,
                    (requireNonNull(operands[2], "optionFormat") as SqlLiteral)
                        .getValueAs(HintOptionFormat::class.java)
                )
            }
        }

        //~ Tools ------------------------------------------------------------------
        private fun getOptionKeyAsString(node: SqlNode): String {
            assert(node is SqlIdentifier || SqlUtil.isLiteral(node))
            return if (node is SqlIdentifier) {
                (node as SqlIdentifier).getSimple()
            } else (node as SqlLiteral).getValueAs(String::class.java)
        }
    }
}
