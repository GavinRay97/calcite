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
package org.apache.calcite.rex

import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlWindow
import com.google.common.collect.ImmutableList
import java.util.Objects

/**
 * Helpers for [RexWindowBound].
 */
object RexWindowBounds {
    /** UNBOUNDED PRECEDING.  */
    val UNBOUNDED_PRECEDING: RexWindowBound = RexUnboundedWindowBound(true)

    /** UNBOUNDED FOLLOWING.  */
    val UNBOUNDED_FOLLOWING: RexWindowBound = RexUnboundedWindowBound(false)

    /** CURRENT ROW.  */
    val CURRENT_ROW: RexWindowBound = RexCurrentRowWindowBound()

    /**
     * Creates a window bound from a [SqlNode].
     *
     * @param node SqlNode of the bound
     * @param rexNode offset value when bound is not UNBOUNDED/CURRENT ROW
     * @return window bound
     */
    fun create(node: SqlNode?, @Nullable rexNode: RexNode?): RexWindowBound {
        if (SqlWindow.isUnboundedPreceding(node)) {
            return UNBOUNDED_PRECEDING
        }
        if (SqlWindow.isUnboundedFollowing(node)) {
            return UNBOUNDED_FOLLOWING
        }
        if (SqlWindow.isCurrentRow(node)) {
            return CURRENT_ROW
        }
        assert(rexNode != null) { "offset value cannot be null for bounded window" }
        return RexBoundedWindowBound(rexNode as RexCall?)
    }

    fun following(offset: RexNode): RexWindowBound {
        return RexBoundedWindowBound(
            RexCall(
                offset.getType(),
                SqlWindow.FOLLOWING_OPERATOR, ImmutableList.of(offset)
            )
        )
    }

    fun preceding(offset: RexNode): RexWindowBound {
        return RexBoundedWindowBound(
            RexCall(
                offset.getType(),
                SqlWindow.PRECEDING_OPERATOR, ImmutableList.of(offset)
            )
        )
    }

    /**
     * Implements UNBOUNDED PRECEDING/FOLLOWING bound.
     */
    private class RexUnboundedWindowBound internal constructor(preceding: Boolean) : RexWindowBound() {
        @get:Override
        override val isPreceding: Boolean

        init {
            preceding = preceding
        }

        @get:Override
        override val isUnbounded: Boolean
            get() = true

        @get:Override
        override val isFollowing: Boolean
            get() = !preceding

        @Override
        override fun toString(): String {
            return if (preceding) "UNBOUNDED PRECEDING" else "UNBOUNDED FOLLOWING"
        }

        @get:Override
        override val orderKey: Int
            get() = if (preceding) 0 else 2

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || o is RexUnboundedWindowBound
                    && preceding == (o as RexUnboundedWindowBound).preceding)
        }

        @Override
        override fun hashCode(): Int {
            return if (preceding) 1357 else 1358
        }
    }

    /**
     * Implements CURRENT ROW bound.
     */
    private class RexCurrentRowWindowBound : RexWindowBound() {
        @get:Override
        override val isCurrentRow: Boolean
            get() = true

        @Override
        override fun toString(): String {
            return "CURRENT ROW"
        }

        @get:Override
        override val orderKey: Int
            get() = 1

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || o is RexCurrentRowWindowBound)
        }

        @Override
        override fun hashCode(): Int {
            return 123
        }
    }

    /**
     * Implements XX PRECEDING/FOLLOWING bound where XX is not UNBOUNDED.
     */
    private class RexBoundedWindowBound : RexWindowBound {
        private val sqlKind: SqlKind
        private override val offset: RexNode

        internal constructor(node: RexCall?) {
            offset = Objects.requireNonNull(node.operands.get(0))
            sqlKind = Objects.requireNonNull(node.getKind())
        }

        private constructor(sqlKind: SqlKind, offset: RexNode) {
            this.sqlKind = sqlKind
            this.offset = offset
        }

        @get:Override
        override val isPreceding: Boolean
            get() = sqlKind === SqlKind.PRECEDING

        @get:Override
        override val isFollowing: Boolean
            get() = sqlKind === SqlKind.FOLLOWING

        @Override
        fun getOffset(): RexNode {
            return offset
        }

        @Override
        override fun nodeCount(): Int {
            return super.nodeCount() + offset.nodeCount()
        }

        @Override
        override fun <R> accept(visitor: RexVisitor<R>?): RexWindowBound {
            val r: R = offset.accept(visitor)
            return if (r is RexNode && r !== offset) {
                RexBoundedWindowBound(sqlKind, r as RexNode)
            } else this
        }

        @Override
        override fun toString(): String {
            return offset.toString() + " " + sqlKind
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || (o is RexBoundedWindowBound
                    && offset.equals((o as RexBoundedWindowBound).offset)
                    && sqlKind === (o as RexBoundedWindowBound).sqlKind))
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(sqlKind, offset)
        }
    }
}
