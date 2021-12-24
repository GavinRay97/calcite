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
package org.apache.calcite.rel.core

import org.apiguardian.api.API

/**
 * Enumeration of join types.
 */
enum class JoinRelType {
    /**
     * Inner join.
     */
    INNER,

    /**
     * Left-outer join.
     */
    LEFT,

    /**
     * Right-outer join.
     */
    RIGHT,

    /**
     * Full-outer join.
     */
    FULL,

    /**
     * Semi-join.
     *
     *
     * For example, `EMP semi-join DEPT` finds all `EMP` records
     * that have a corresponding `DEPT` record:
     *
     * <blockquote><pre>
     * SELECT * FROM EMP
     * WHERE EXISTS (SELECT 1 FROM DEPT
     * WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
    </blockquote> *
     */
    SEMI,

    /**
     * Anti-join (also known as Anti-semi-join).
     *
     *
     * For example, `EMP anti-join DEPT` finds all `EMP` records
     * that do not have a corresponding `DEPT` record:
     *
     * <blockquote><pre>
     * SELECT * FROM EMP
     * WHERE NOT EXISTS (SELECT 1 FROM DEPT
     * WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
    </blockquote> *
     */
    ANTI;

    /** Lower-case name.  */
    val lowerName: String = name().toLowerCase(Locale.ROOT)

    /**
     * Returns whether a join of this type may generate NULL values on the
     * right-hand side.
     */
    fun generatesNullsOnRight(): Boolean {
        return this == LEFT || this == FULL
    }

    /**
     * Returns whether a join of this type may generate NULL values on the
     * left-hand side.
     */
    fun generatesNullsOnLeft(): Boolean {
        return this == RIGHT || this == FULL
    }

    /**
     * Returns whether a join of this type is an outer join, returns true if the join type may
     * generate NULL values, either on the left-hand side or right-hand side.
     */
    val isOuterJoin: Boolean
        get() = this == LEFT || this == RIGHT || this == FULL

    /**
     * Swaps left to right, and vice versa.
     */
    fun swap(): JoinRelType {
        return when (this) {
            LEFT -> RIGHT
            RIGHT -> LEFT
            else -> this
        }
    }

    /** Returns whether this join type generates nulls on side #`i`.  */
    fun generatesNullsOn(i: Int): Boolean {
        return when (i) {
            0 -> generatesNullsOnLeft()
            1 -> generatesNullsOnRight()
            else -> throw IllegalArgumentException("invalid: $i")
        }
    }

    /** Returns a join type similar to this but that does not generate nulls on
     * the left.  */
    fun cancelNullsOnLeft(): JoinRelType {
        return when (this) {
            RIGHT -> INNER
            FULL -> LEFT
            else -> this
        }
    }

    /** Returns a join type similar to this but that does not generate nulls on
     * the right.  */
    fun cancelNullsOnRight(): JoinRelType {
        return when (this) {
            LEFT -> INNER
            FULL -> RIGHT
            else -> this
        }
    }

    fun projectsRight(): Boolean {
        return this != SEMI && this != ANTI
    }

    /** Returns whether this join type accepts pushing predicates from above into its predicate.  */
    @API(since = "1.28", status = API.Status.EXPERIMENTAL)
    fun canPushIntoFromAbove(): Boolean {
        return this == INNER || this == SEMI
    }

    /** Returns whether this join type accepts pushing predicates from above into its left input.  */
    @API(since = "1.28", status = API.Status.EXPERIMENTAL)
    fun canPushLeftFromAbove(): Boolean {
        return this == INNER || this == LEFT || this == SEMI || this == ANTI
    }

    /** Returns whether this join type accepts pushing predicates from above into its right input.  */
    @API(since = "1.28", status = API.Status.EXPERIMENTAL)
    fun canPushRightFromAbove(): Boolean {
        return this == INNER || this == RIGHT
    }

    /** Returns whether this join type accepts pushing predicates from within into its left input.  */
    @API(since = "1.28", status = API.Status.EXPERIMENTAL)
    fun canPushLeftFromWithin(): Boolean {
        return this == INNER || this == RIGHT || this == SEMI
    }

    /** Returns whether this join type accepts pushing predicates from within into its right input.  */
    @API(since = "1.28", status = API.Status.EXPERIMENTAL)
    fun canPushRightFromWithin(): Boolean {
        return this == INNER || this == LEFT || this == SEMI
    }
}
