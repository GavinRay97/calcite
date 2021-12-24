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
 * A `SqlNodeList` is a list of [SqlNode]s. It is also a
 * [SqlNode], so may appear in a parse tree.
 *
 * @see SqlNode.toList
 */
class SqlNodeList private constructor(pos: SqlParserPos, list: List<SqlNode>) : SqlNode(pos), List<SqlNode?>,
    RandomAccess {
    //~ Instance fields --------------------------------------------------------
    // Sometimes null values are present in the list, however, it is assumed that callers would
    // perform all the required null-checks.
    private val list: List<SqlNode>
    //~ Constructors -----------------------------------------------------------
    /** Creates a SqlNodeList with a given backing list.
     *
     *
     * Because SqlNodeList implements [RandomAccess], the backing list
     * should allow O(1) access to elements.  */
    init {
        this.list = Objects.requireNonNull(list, "list")
    }

    /**
     * Creates a SqlNodeList that is initially empty.
     */
    constructor(pos: SqlParserPos?) : this(pos, ArrayList()) {}

    /**
     * Creates a `SqlNodeList` containing the nodes in `
     * list`. The list is copied, but the nodes in it are not.
     */
    constructor(
        collection: Collection<SqlNode?>?,
        pos: SqlParserPos?
    ) : this(pos, ArrayList<SqlNode>(collection)) {
    }

    //~ Methods ----------------------------------------------------------------
    // List, Collection and Iterable methods
    @Override
    override fun hashCode(): Int {
        return list.hashCode()
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        return this === o || o is SqlNodeList && list.equals((o as SqlNodeList).list) || o is List && list.equals(o)
    }

    @Override
    override fun isEmpty(): Boolean {
        return list.isEmpty()
    }

    @Override
    fun size(): Int {
        return list.size()
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    override fun iterator(): Iterator<SqlNode> {
        return list.iterator()
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    override fun listIterator(): ListIterator<SqlNode> {
        return list.listIterator()
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    override fun listIterator(index: Int): ListIterator<SqlNode> {
        return list.listIterator(index)
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    override fun subList(fromIndex: Int, toIndex: Int): List<SqlNode> {
        return list.subList(fromIndex, toIndex)
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    override  /*Nullable*/   fun get(n: Int): SqlNode {
        return list[n]
    }

    @Override
    operator fun set(n: Int, @Nullable node: SqlNode?): SqlNode {
        return castNonNull(list.set(n, node))
    }

    @Override
    override fun contains(@Nullable o: Object): Boolean {
        return list.contains(o)
    }

    @Override
    fun containsAll(c: Collection<*>?): Boolean {
        return list.containsAll(c)
    }

    @Override
    override fun indexOf(@Nullable o: Object): Int {
        return list.indexOf(o)
    }

    @Override
    override fun lastIndexOf(@Nullable o: Object): Int {
        return list.lastIndexOf(o)
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    fun toArray(): Array<Object> {
        // Per JDK specification, must return an Object[] not SqlNode[]; see e.g.
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6260652
        return list.toArray()
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    fun <T> toArray(a: @Nullable Array<T>?): Array<T> {
        return list.toArray(a)
    }

    @Override
    fun add(@Nullable node: SqlNode?): Boolean {
        return list.add(node)
    }

    @Override
    fun add(index: Int, @Nullable element: SqlNode?) {
        list.add(index, element)
    }

    @Override
    fun addAll(c: Collection<SqlNode?>?): Boolean {
        return list.addAll(c)
    }

    @Override
    fun addAll(index: Int, c: Collection<SqlNode?>?): Boolean {
        return list.addAll(index, c)
    }

    @Override
    fun clear() {
        list.clear()
    }

    @Override
    fun remove(@Nullable o: Object?): Boolean {
        return list.remove(o)
    }

    @Override
    fun remove(index: Int): SqlNode {
        return castNonNull(list.remove(index))
    }

    @Override
    fun removeAll(c: Collection<*>?): Boolean {
        return list.removeAll(c)
    }

    @Override
    fun retainAll(c: Collection<*>?): Boolean {
        return list.retainAll(c)
    }

    // SqlNodeList-specific methods
    fun getList(): List<SqlNode> {
        return list
    }

    @Override
    override fun clone(pos: SqlParserPos?): SqlNodeList {
        return SqlNodeList(list, pos)
    }

    @Override
    override fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frameType: SqlWriter.FrameTypeEnum =
            if (leftPrec > 0 || rightPrec > 0) SqlWriter.FrameTypeEnum.PARENTHESES else SqlWriter.FrameTypeEnum.SIMPLE
        writer.list(frameType, SqlWriter.COMMA, this)
    }

    @Deprecated
    fun  // to be removed before 2.0
            commaList(writer: SqlWriter) {
        unparse(writer, 0, 0)
    }

    @Deprecated
    fun  // to be removed before 2.0
            andOrList(writer: SqlWriter, sepOp: SqlBinaryOperator?) {
        writer.list(SqlWriter.FrameTypeEnum.WHERE_LIST, sepOp, this)
    }

    @Override
    override fun validate(validator: SqlValidator?, scope: SqlValidatorScope?) {
        for (child in list) {
            if (child == null) {
                continue
            }
            child.validate(validator, scope)
        }
    }

    @Override
    override fun <R> accept(visitor: SqlVisitor<R>): R {
        return visitor.visit(this)
    }

    @Override
    override fun equalsDeep(@Nullable node: SqlNode?, litmus: Litmus): Boolean {
        if (node !is SqlNodeList) {
            return litmus.fail("{} != {}", this, node)
        }
        val that = node
        if (size() != that.size()) {
            return litmus.fail("{} != {}", this, node)
        }
        for (i in 0 until list.size()) {
            val thisChild: SqlNode = list[i]
            val thatChild: SqlNode = that.list[i]
            if (thisChild == null) {
                return if (thatChild == null) {
                    continue
                } else {
                    litmus.fail(null)
                }
            }
            if (!thisChild.equalsDeep(thatChild, litmus)) {
                return litmus.fail(null)
            }
        }
        return litmus.succeed()
    }

    @Override
    override fun validateExpr(validator: SqlValidator, scope: SqlValidatorScope?) {
        // While a SqlNodeList is not always a valid expression, this
        // implementation makes that assumption. It just validates the members
        // of the list.
        //
        // One example where this is valid is the IN operator. The expression
        //
        //    empno IN (10, 20)
        //
        // results in a call with operands
        //
        //    {  SqlIdentifier({"empno"}),
        //       SqlNodeList(SqlLiteral(10), SqlLiteral(20))  }
        for (node in list) {
            if (node == null) {
                continue
            }
            node.validateExpr(validator, scope)
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /**
         * An immutable, empty SqlNodeList.
         */
        val EMPTY: SqlNodeList = SqlNodeList(ImmutableList.of(), SqlParserPos.ZERO)

        /**
         * A SqlNodeList that has a single element that is an empty list.
         */
        val SINGLETON_EMPTY: SqlNodeList = SqlNodeList(ImmutableList.of(EMPTY), SqlParserPos.ZERO)

        /**
         * A SqlNodeList that has a single element that is a star identifier.
         */
        val SINGLETON_STAR: SqlNodeList = SqlNodeList(ImmutableList.of(SqlIdentifier.STAR), SqlParserPos.ZERO)

        /**
         * Creates a SqlNodeList with a given backing list.
         * Does not copy the list.
         */
        fun of(pos: SqlParserPos?, list: List<SqlNode?>?): SqlNodeList {
            return SqlNodeList(pos, list)
        }

        fun isEmptyList(node: SqlNode?): Boolean {
            return (node is SqlNodeList
                    && node.isEmpty())
        }

        fun of(node1: SqlNode?): SqlNodeList {
            val list: List<SqlNode> = ArrayList(1)
            list.add(node1)
            return SqlNodeList(SqlParserPos.ZERO, list)
        }

        fun of(node1: SqlNode?, node2: SqlNode?): SqlNodeList {
            val list: List<SqlNode> = ArrayList(2)
            list.add(node1)
            list.add(node2)
            return SqlNodeList(SqlParserPos.ZERO, list)
        }

        fun of(node1: SqlNode?, node2: SqlNode?, @Nullable vararg nodes: SqlNode?): SqlNodeList {
            val list: List<SqlNode> = ArrayList(nodes.size + 2)
            list.add(node1)
            list.add(node2)
            Collections.addAll(list, nodes)
            return SqlNodeList(SqlParserPos.ZERO, list)
        }
    }
}
