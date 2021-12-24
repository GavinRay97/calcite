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

import org.apache.calcite.sql.SqlNode
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf
import org.checkerframework.dataflow.qual.Pure

/**
 * Abstracts "XX PRECEDING/FOLLOWING" and "CURRENT ROW" bounds for windowed
 * aggregates.
 *
 * @see RexWindowBounds
 */
abstract class RexWindowBound {
    /**
     * Returns if the bound is unbounded.
     * @return if the bound is unbounded
     */
    @get:SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
    @get:EnsuresNonNullIf(expression = "getOffset()", result = false)
    @get:Pure
    val isUnbounded: Boolean
        get() = false

    /**
     * Returns if the bound is PRECEDING.
     * @return if the bound is PRECEDING
     */
    val isPreceding: Boolean
        get() = false

    /**
     * Returns if the bound is FOLLOWING.
     * @return if the bound is FOLLOWING
     */
    val isFollowing: Boolean
        get() = false

    /**
     * Returns if the bound is CURRENT ROW.
     * @return if the bound is CURRENT ROW
     */
    @get:SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
    @get:EnsuresNonNullIf(expression = "getOffset()", result = false)
    @get:Pure
    val isCurrentRow: Boolean
        get() = false

    /**
     * Returns offset from XX PRECEDING/FOLLOWING.
     *
     * @return offset from XX PRECEDING/FOLLOWING
     */
    @get:Nullable
    @get:Pure
    val offset: RexNode?
        get() = null

    /**
     * Returns relative sort offset when known at compile time.
     * For instance, UNBOUNDED PRECEDING is less than CURRENT ROW.
     *
     * @return relative order or -1 when order is not known
     */
    val orderKey: Int
        get() = -1

    /**
     * Transforms the bound via [org.apache.calcite.rex.RexVisitor].
     *
     * @param visitor visitor to accept
     * @param <R> return type of the visitor
     * @return transformed bound
    </R> */
    fun <R> accept(visitor: RexVisitor<R>?): RexWindowBound {
        return this
    }

    /**
     * Transforms the bound via [org.apache.calcite.rex.RexBiVisitor].
     *
     * @param visitor visitor to accept
     * @param arg Payload
     * @param <R> return type of the visitor
     * @return transformed bound
    </R> */
    fun <R, P> accept(visitor: RexBiVisitor<R, P>?, arg: P): RexWindowBound {
        return this
    }

    /**
     * Returns the number of nodes in this bound.
     *
     * @see RexNode.nodeCount
     */
    fun nodeCount(): Int {
        return 1
    }

    companion object {
        /** Use [RexWindowBounds.create].  */
        @Deprecated // to be removed before 2.0
        fun create(node: SqlNode?, rexNode: RexNode?): RexWindowBound {
            return RexWindowBounds.create(node, rexNode)
        }
    }
}
