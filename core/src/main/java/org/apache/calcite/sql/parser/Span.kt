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
package org.apache.calcite.sql.parser

import org.apache.calcite.sql.SqlNode

/**
 * Builder for [SqlParserPos].
 *
 *
 * Because it is mutable, it is convenient for keeping track of the
 * positions of the tokens that go into a non-terminal. It can be passed
 * into methods, which can add the positions of tokens consumed to it.
 *
 *
 * Some patterns:
 *
 *
 *  * `final Span s;` declaration of a Span at the top of a production
 *  * `s = span();` initializes s to a Span that includes the token we
 * just saw; very often occurs immediately after the first token in the
 * production
 *  * `s.end(this);` adds the most recent token to span s and evaluates
 * to a SqlParserPosition that spans from beginning to end; commonly used
 * when making a call to a function
 *  * `s.pos()` returns a position spanning all tokens in the list
 *  * `s.add(node);` adds a SqlNode's parser position to a span
 *  * `s.addAll(nodeList);` adds several SqlNodes' parser positions to
 * a span
 *  * `s = Span.of();` initializes s to an empty Span, not even
 * including the most recent token; rarely used
 *
 */
class Span
/** Use one of the [.of] methods.  */
private constructor() {
    private val posList: List<SqlParserPos> = ArrayList()

    /** Adds a node's position to the list,
     * and returns this Span.  */
    fun add(n: SqlNode): Span {
        return add(n.getParserPosition())
    }

    /** Adds a node's position to the list if the node is not null,
     * and returns this Span.  */
    fun addIf(n: SqlNode?): Span {
        return if (n == null) this else add(n)
    }

    /** Adds a position to the list,
     * and returns this Span.  */
    fun add(pos: SqlParserPos?): Span {
        posList.add(pos)
        return this
    }

    /** Adds the positions of a collection of nodes to the list,
     * and returns this Span.  */
    fun addAll(nodes: Iterable<SqlNode?>): Span {
        for (node in nodes) {
            add(node)
        }
        return this
    }

    /** Adds the position of the last token emitted by a parser to the list,
     * and returns this Span.  */
    fun add(parser: SqlAbstractParserImpl): Span {
        return try {
            val pos: SqlParserPos = parser.getPos()
            add(pos)
        } catch (e: Exception) {
            // getPos does not really throw an exception
            throw AssertionError(e)
        }
    }

    /** Returns a position spanning the earliest position to the latest.
     * Does not assume that the positions are sorted.
     * Throws if the list is empty.  */
    fun pos(): SqlParserPos {
        return SqlParserPos.sum(posList)
    }

    /** Adds the position of the last token emitted by a parser to the list,
     * and returns a position that covers the whole range.  */
    fun end(parser: SqlAbstractParserImpl?): SqlParserPos {
        return add(parser).pos()
    }

    /** Adds a node's position to the list,
     * and returns a position that covers the whole range.  */
    fun end(n: SqlNode): SqlParserPos {
        return add(n).pos()
    }

    /** Clears the contents of this Span, and returns this Span.  */
    fun clear(): Span {
        posList.clear()
        return this
    }

    companion object {
        /** Creates an empty Span.  */
        fun of(): Span {
            return Span()
        }

        /** Creates a Span with one position.  */
        fun of(p: SqlParserPos?): Span {
            return Span().add(p)
        }

        /** Creates a Span of one node.  */
        fun of(n: SqlNode): Span {
            return Span().add(n)
        }

        /** Creates a Span between two nodes.  */
        fun of(n0: SqlNode, n1: SqlNode): Span {
            return Span().add(n0).add(n1)
        }

        /** Creates a Span of a list of nodes.  */
        fun of(nodes: Collection<SqlNode?>): Span {
            return Span().addAll(nodes)
        }

        /** Creates a Span of a node list.  */
        fun of(nodeList: SqlNodeList): Span {
            // SqlNodeList has its own position, so just that position, not all of the
            // constituent nodes.
            return Span().add(nodeList)
        }
    }
}
