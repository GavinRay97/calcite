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
package org.apache.calcite.sql.validate

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.util.Util
import java.util.ArrayList
import java.util.Iterator
import java.util.List

/** Visitor that looks for an aggregate function inside a tree of
 * [SqlNode] objects and throws [Util.FoundOne] when it finds
 * one.  */
internal class AggFinder
/**
 * Creates an AggFinder.
 *
 * @param opTab Operator table
 * @param over Whether to find windowed function calls `agg(x) OVER
 * windowSpec`
 * @param aggregate Whether to find non-windowed aggregate calls
 * @param group Whether to find group functions (e.g. `TUMBLE`)
 * @param delegate Finder to which to delegate when processing the arguments
 * @param nameMatcher Whether to match the agg function case-sensitively
 */
    (
    opTab: SqlOperatorTable?, over: Boolean, aggregate: Boolean,
    group: Boolean, @Nullable delegate: AggFinder?, nameMatcher: SqlNameMatcher?
) : AggVisitor(opTab, over, aggregate, group, delegate, nameMatcher) {
    //~ Methods ----------------------------------------------------------------
    /**
     * Finds an aggregate.
     *
     * @param node Parse tree to search
     * @return First aggregate function in parse tree, or null if not found
     */
    @Nullable
    fun findAgg(node: SqlNode): SqlCall? {
        return try {
            node.accept(this)
            null
        } catch (e: Util.FoundOne) {
            Util.swallow(e, null)
            e.getNode() as SqlCall
        }
    }

    // SqlNodeList extends SqlNode and implements List<SqlNode>, so this method
    // disambiguates
    @Nullable
    fun findAgg(nodes: SqlNodeList?): SqlCall {
        return findAgg(nodes as List<SqlNode?>?)
    }

    @Nullable
    fun findAgg(nodes: List<SqlNode?>): SqlCall? {
        return try {
            for (node in nodes) {
                node.accept(this)
            }
            null
        } catch (e: Util.FoundOne) {
            Util.swallow(e, null)
            e.getNode() as SqlCall
        }
    }

    @Override
    protected fun found(call: SqlCall?): Void {
        throw FoundOne(call)
    }

    /** Creates a copy of this finder that has the same parameters as this,
     * then returns the list of all aggregates found.  */
    fun findAll(nodes: Iterable<SqlNode?>): Iterable<SqlCall> {
        val aggIterable = AggIterable(opTab, over, aggregate, group, delegate, nameMatcher)
        for (node in nodes) {
            node.accept(aggIterable)
        }
        return aggIterable.calls
    }

    /** Iterates over all aggregates.  */
    internal class AggIterable(
        opTab: SqlOperatorTable?, over: Boolean, aggregate: Boolean,
        group: Boolean, @Nullable delegate: AggFinder?, nameMatcher: SqlNameMatcher?
    ) : AggVisitor(opTab, over, aggregate, group, delegate, nameMatcher), Iterable<SqlCall?> {
        val calls: List<SqlCall> = ArrayList()
        @Override
        protected fun found(call: SqlCall?): Void? {
            calls.add(call)
            return null
        }

        @Override
        override fun iterator(): Iterator<SqlCall> {
            return calls.iterator()
        }
    }
}
