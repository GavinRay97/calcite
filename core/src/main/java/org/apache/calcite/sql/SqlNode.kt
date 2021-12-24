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

import org.apache.calcite.sql.dialect.AnsiSqlDialect

/**
 * A `SqlNode` is a SQL parse tree.
 *
 *
 * It may be an
 * [operator][SqlOperator], [literal][SqlLiteral],
 * [identifier][SqlIdentifier], and so forth.
 */
abstract class SqlNode internal constructor(pos: SqlParserPos?) : Cloneable, Cloneable {
    //~ Instance fields --------------------------------------------------------
    val pos: SqlParserPos
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    init {
        this.pos = Objects.requireNonNull(pos, "pos")
    }
    //~ Methods ----------------------------------------------------------------
    // CHECKSTYLE: IGNORE 1

    @Deprecated
    @SuppressWarnings(["MethodDoesntCallSuperMethod", "AmbiguousMethodReference"])
    @Override
    @Deprecated(
        """Please use {@link #clone(SqlNode)}; this method brings
    along too much baggage from early versions of Java """
    )
    fun clone(): Object {
        return clone(parserPosition)
    }

    /**
     * Clones a SqlNode with a different position.
     */
    abstract fun clone(pos: SqlParserPos?): SqlNode

    /**
     * Returns the type of node this is, or
     * [org.apache.calcite.sql.SqlKind.OTHER] if it's nothing special.
     *
     * @return a [SqlKind] value, never null
     * @see .isA
     */
    val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.OTHER

    /**
     * Returns whether this node is a member of an aggregate category.
     *
     *
     * For example, `node.isA(SqlKind.QUERY)` returns `true`
     * if the node is a SELECT, INSERT, UPDATE etc.
     *
     *
     * This method is shorthand: `node.isA(category)` is always
     * equivalent to `node.getKind().belongsTo(category)`.
     *
     * @param category Category
     * @return Whether this node belongs to the given category.
     */
    fun isA(category: Set<SqlKind?>): Boolean {
        return kind.belongsTo(category)
    }

    @Override
    override fun toString(): String {
        return toSqlString({ c ->
            c.withDialect(AnsiSqlDialect.DEFAULT)
                .withAlwaysUseParentheses(false)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0)
        }).getSql()
    }

    /**
     * Returns the SQL text of the tree of which this `SqlNode` is
     * the root.
     *
     *
     * Typical return values are:
     *
     *
     *  * 'It''s a bird!'
     *  * NULL
     *  * 12.3
     *  * DATE '1969-04-29'
     *
     *
     * @param transform   Transform that sets desired writer configuration
     */
    fun toSqlString(transform: UnaryOperator<SqlWriterConfig?>): SqlString {
        val config: SqlWriterConfig = transform.apply(SqlPrettyWriter.config())
        val writer = SqlPrettyWriter(config)
        unparse(writer, 0, 0)
        return writer.toSqlString()
    }

    /**
     * Returns the SQL text of the tree of which this `SqlNode` is
     * the root.
     *
     *
     * Typical return values are:
     *
     *
     *  * 'It''s a bird!'
     *  * NULL
     *  * 12.3
     *  * DATE '1969-04-29'
     *
     *
     * @param dialect     Dialect (null for ANSI SQL)
     * @param forceParens Whether to wrap all expressions in parentheses;
     * useful for parse test, but false by default
     */
    fun toSqlString(@Nullable dialect: SqlDialect?, forceParens: Boolean): SqlString {
        return toSqlString({ c ->
            c.withDialect(Util.first(dialect, AnsiSqlDialect.DEFAULT))
                .withAlwaysUseParentheses(forceParens)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0)
        })
    }

    fun toSqlString(@Nullable dialect: SqlDialect?): SqlString {
        return toSqlString(dialect, false)
    }

    /**
     * Writes a SQL representation of this node to a writer.
     *
     *
     * The `leftPrec` and `rightPrec` parameters give
     * us enough context to decide whether we need to enclose the expression in
     * parentheses. For example, we need parentheses around "2 + 3" if preceded
     * by "5 *". This is because the precedence of the "*" operator is greater
     * than the precedence of the "+" operator.
     *
     *
     * The algorithm handles left- and right-associative operators by giving
     * them slightly different left- and right-precedence.
     *
     *
     * If [SqlWriter.isAlwaysUseParentheses] is true, we use
     * parentheses even when they are not required by the precedence rules.
     *
     *
     * For the details of this algorithm, see [SqlCall.unparse].
     *
     * @param writer    Target writer
     * @param leftPrec  The precedence of the [SqlNode] immediately
     * preceding this node in a depth-first scan of the parse
     * tree
     * @param rightPrec The precedence of the [SqlNode] immediately
     */
    abstract fun unparse(
        writer: SqlWriter?,
        leftPrec: Int,
        rightPrec: Int
    )

    fun unparseWithParentheses(
        writer: SqlWriter, leftPrec: Int,
        rightPrec: Int, parentheses: Boolean
    ) {
        if (parentheses) {
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            unparse(writer, 0, 0)
            writer.endList(frame)
        } else {
            unparse(writer, leftPrec, rightPrec)
        }
    }

    val parserPosition: SqlParserPos
        get() = pos

    /**
     * Validates this node.
     *
     *
     * The typical implementation of this method will make a callback to the
     * validator appropriate to the node type and context. The validator has
     * methods such as [SqlValidator.validateLiteral] for these purposes.
     *
     * @param scope Validator
     */
    abstract fun validate(
        validator: SqlValidator?,
        scope: SqlValidatorScope?
    )

    /**
     * Lists all the valid alternatives for this node if the parse position of
     * the node matches that of pos. Only implemented now for SqlCall and
     * SqlOperator.
     *
     * @param validator Validator
     * @param scope     Validation scope
     * @param pos       SqlParserPos indicating the cursor position at which
     * completion hints are requested for
     * @param hintList  list of valid options
     */
    fun findValidOptions(
        validator: SqlValidator?,
        scope: SqlValidatorScope?,
        pos: SqlParserPos?,
        hintList: Collection<SqlMoniker?>?
    ) {
        // no valid options
    }

    /**
     * Validates this node in an expression context.
     *
     *
     * Usually, this method does much the same as [.validate], but a
     * [SqlIdentifier] can occur in expression and non-expression
     * contexts.
     */
    fun validateExpr(
        validator: SqlValidator,
        scope: SqlValidatorScope?
    ) {
        validate(validator, scope)
        Util.discard(validator.deriveType(scope, this))
    }

    /**
     * Accepts a generic visitor.
     *
     *
     * Implementations of this method in subtypes simply call the appropriate
     * `visit` method on the
     * [visitor object][org.apache.calcite.sql.util.SqlVisitor].
     *
     *
     * The type parameter `R` must be consistent with the type
     * parameter of the visitor.
     */
    abstract fun <R> accept(visitor: SqlVisitor<R>?): R

    /**
     * Returns whether this node is structurally equivalent to another node.
     * Some examples:
     *
     *
     *  * 1 + 2 is structurally equivalent to 1 + 2
     *  * 1 + 2 + 3 is structurally equivalent to (1 + 2) + 3, but not to 1 +
     * (2 + 3), because the '+' operator is left-associative
     *
     */
    abstract fun equalsDeep(@Nullable node: SqlNode?, litmus: Litmus?): Boolean

    @Deprecated // to be removed before 2.0
    fun equalsDeep(@Nullable node: SqlNode?, fail: Boolean): Boolean {
        return equalsDeep(node, if (fail) Litmus.THROW else Litmus.IGNORE)
    }

    /**
     * Returns whether expression is always ascending, descending or constant.
     * This property is useful because it allows to safely aggregate infinite
     * streams of values.
     *
     *
     * The default implementation returns
     * [SqlMonotonicity.NOT_MONOTONIC].
     *
     * @param scope Scope
     */
    fun getMonotonicity(@Nullable scope: SqlValidatorScope?): SqlMonotonicity {
        return SqlMonotonicity.NOT_MONOTONIC
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        @Nullable
        val EMPTY_ARRAY = arrayOfNulls<SqlNode>(0)

        /** Creates a copy of a SqlNode.  */
        @SuppressWarnings("AmbiguousMethodReference")
        fun <E : SqlNode?> clone(e: E): E {
            return e!!.clone(e.pos) as E
        }

        @Deprecated // to be removed before 2.0
        fun cloneArray(nodes: Array<SqlNode?>): Array<SqlNode> {
            val clones: Array<SqlNode> = nodes.clone()
            for (i in clones.indices) {
                val node = clones[i]
                if (node != null) {
                    clones[i] = clone(node)
                }
            }
            return clones
        }

        /**
         * Returns whether two nodes are equal (using
         * [.equalsDeep]) or are both null.
         *
         * @param node1 First expression
         * @param node2 Second expression
         * @param litmus What to do if an error is detected (expressions are
         * not equal)
         */
        fun equalDeep(
            @Nullable node1: SqlNode?,
            @Nullable node2: SqlNode?,
            litmus: Litmus?
        ): Boolean {
            return if (node1 == null) {
                node2 == null
            } else if (node2 == null) {
                false
            } else {
                node1.equalsDeep(node2, litmus)
            }
        }

        /** Returns whether two lists of operands are equal.  */
        fun equalDeep(
            operands0: List<SqlNode?>,
            operands1: List<SqlNode?>, litmus: Litmus
        ): Boolean {
            if (operands0.size() !== operands1.size()) {
                return litmus.fail(null)
            }
            for (i in 0 until operands0.size()) {
                if (!equalDeep(operands0[i], operands1[i], litmus)) {
                    return litmus.fail(null)
                }
            }
            return litmus.succeed()
        }

        /**
         * Returns a `Collector` that accumulates the input elements into a
         * [SqlNodeList], with zero position.
         *
         * @param <T> Type of the input elements
         *
         * @return a `Collector` that collects all the input elements into a
         * [SqlNodeList], in encounter order
        </T> */
        fun <T : SqlNode?> toList(): Collector<T, ArrayList<SqlNode>, SqlNodeList> {
            return toList(SqlParserPos.ZERO)
        }

        /**
         * Returns a `Collector` that accumulates the input elements into a
         * [SqlNodeList].
         *
         * @param <T> Type of the input elements
         *
         * @return a `Collector` that collects all the input elements into a
         * [SqlNodeList], in encounter order
        </T> */
        fun <T : SqlNode?> toList(pos: SqlParserPos?): Collector<T, ArrayList<SqlNode>, SqlNodeList> {
            return Collector.< T, ArrayList<SqlNode>, SqlNodeList>of<T?, ArrayList<org.apache.calcite.sql.SqlNode?>?, SqlNodeList?>({ ArrayList() }, ArrayList::add, Util::combine,
            { list: ArrayList<SqlNode?>? -> SqlNodeList.of(pos, list) })
        }
    }
}
