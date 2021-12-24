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

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlKind
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import java.util.Collection
import java.util.Objects.requireNonNull

/**
 * Row expression.
 *
 *
 * Every row-expression has a type.
 * (Compare with [org.apache.calcite.sql.SqlNode], which is created before
 * validation, and therefore types may not be available.)
 *
 *
 * Some common row-expressions are: [RexLiteral] (constant value),
 * [RexVariable] (variable), [RexCall] (call to operator with
 * operands). Expressions are generally created using a [RexBuilder]
 * factory.
 *
 *
 * All sub-classes of RexNode are immutable.
 */
abstract class RexNode {
    //~ Instance fields --------------------------------------------------------
    // Effectively final. Set in each sub-class constructor, and never re-set.
    @MonotonicNonNull
    protected var digest: String? = null

    //~ Methods ----------------------------------------------------------------
    abstract val type: RelDataType

    /**
     * Returns whether this expression always returns true. (Such as if this
     * expression is equal to the literal `TRUE`.)
     */
    val isAlwaysTrue: Boolean
        get() = false

    /**
     * Returns whether this expression always returns false. (Such as if this
     * expression is equal to the literal `FALSE`.)
     */
    val isAlwaysFalse: Boolean
        get() = false

    fun isA(kind: SqlKind): Boolean {
        return kind === kind
    }

    fun isA(kinds: Collection<SqlKind?>?): Boolean {
        return kind.belongsTo(kinds)
    }

    /**
     * Returns the kind of node this is.
     *
     * @return Node kind, never null
     */
    val kind: SqlKind
        get() = SqlKind.OTHER

    @Override
    override fun toString(): String {
        return requireNonNull(digest, "digest")
    }

    /** Returns the number of nodes in this expression.
     *
     *
     * Leaf nodes, such as [RexInputRef] or [RexLiteral], have
     * a count of 1. Calls have a count of 1 plus the sum of their operands.
     *
     *
     * Node count is a measure of expression complexity that is used by some
     * planner rules to prevent deeply nested expressions.
     */
    fun nodeCount(): Int {
        return 1
    }

    /**
     * Accepts a visitor, dispatching to the right overloaded
     * [visitXxx][RexVisitor.visitInputRef] method.
     *
     *
     * Also see [RexUtil.apply],
     * which applies a visitor to several expressions simultaneously.
     */
    abstract fun <R> accept(visitor: RexVisitor<R>?): R

    /**
     * Accepts a visitor with a payload, dispatching to the right overloaded
     * [RexBiVisitor.visitInputRef] visitXxx} method.
     */
    abstract fun <R, P> accept(visitor: RexBiVisitor<R, P>?, arg: P): R

    /** {@inheritDoc}
     *
     *
     * Every node must implement [.equals] based on its content
     */
    @Override
    abstract override fun equals(@Nullable obj: Object?): Boolean

    /** {@inheritDoc}
     *
     *
     * Every node must implement [.hashCode] consistent with
     * [.equals]
     */
    @Override
    abstract override fun hashCode(): Int
}
