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
package org.apache.calcite.rel

import org.apache.calcite.sql.validate.SqlMonotonicity

/**
 * Definition of the ordering of one field of a [RelNode] whose
 * output is to be sorted.
 *
 * @see RelCollation
 */
class RelFieldCollation @JvmOverloads constructor(
    /**
     * 0-based index of field being sorted.
     */
    val fieldIndex: Int,
    direction: Direction = Direction.ASCENDING,
    nullDirection: NullDirection? = direction.defaultNullDirection()
) {
    //~ Enums ------------------------------------------------------------------
    /**
     * Direction that a field is ordered in.
     */
    enum class Direction(val shortString: String) {
        /**
         * Ascending direction: A value is always followed by a greater or equal
         * value.
         */
        ASCENDING("ASC"),

        /**
         * Strictly ascending direction: A value is always followed by a greater
         * value.
         */
        STRICTLY_ASCENDING("SASC"),

        /**
         * Descending direction: A value is always followed by a lesser or equal
         * value.
         */
        DESCENDING("DESC"),

        /**
         * Strictly descending direction: A value is always followed by a lesser
         * value.
         */
        STRICTLY_DESCENDING("SDESC"),

        /**
         * Clustered direction: Values occur in no particular order, and the
         * same value may occur in contiguous groups, but never occurs after
         * that. This sort order tends to occur when values are ordered
         * according to a hash-key.
         */
        CLUSTERED("CLU");

        /** Converts the direction to a
         * [org.apache.calcite.sql.validate.SqlMonotonicity].  */
        fun monotonicity(): SqlMonotonicity {
            return when (this) {
                ASCENDING -> SqlMonotonicity.INCREASING
                STRICTLY_ASCENDING -> SqlMonotonicity.STRICTLY_INCREASING
                DESCENDING -> SqlMonotonicity.DECREASING
                STRICTLY_DESCENDING -> SqlMonotonicity.STRICTLY_DECREASING
                CLUSTERED -> SqlMonotonicity.MONOTONIC
                else -> throw AssertionError("unknown: $this")
            }
        }

        /** Returns the null direction if not specified. Consistent with Oracle,
         * NULLS are sorted as if they were positive infinity.  */
        fun defaultNullDirection(): NullDirection {
            return when (this) {
                ASCENDING, STRICTLY_ASCENDING -> NullDirection.LAST
                DESCENDING, STRICTLY_DESCENDING -> NullDirection.FIRST
                else -> NullDirection.UNSPECIFIED
            }
        }

        /** Returns whether this is [.DESCENDING] or
         * [.STRICTLY_DESCENDING].  */
        val isDescending: Boolean
            get() = when (this) {
                DESCENDING, STRICTLY_DESCENDING -> true
                else -> false
            }

        /**
         * Returns the reverse of this direction.
         *
         * @return reverse of the input direction
         */
        fun reverse(): Direction {
            return when (this) {
                ASCENDING -> DESCENDING
                STRICTLY_ASCENDING -> STRICTLY_DESCENDING
                DESCENDING -> ASCENDING
                STRICTLY_DESCENDING -> STRICTLY_ASCENDING
                else -> this
            }
        }

        /** Removes strictness.  */
        fun lax(): Direction {
            return when (this) {
                STRICTLY_ASCENDING -> ASCENDING
                STRICTLY_DESCENDING -> DESCENDING
                else -> this
            }
        }

        companion object {
            /** Converts a [SqlMonotonicity] to a direction.  */
            fun of(monotonicity: SqlMonotonicity): Direction {
                return when (monotonicity) {
                    INCREASING -> ASCENDING
                    DECREASING -> DESCENDING
                    STRICTLY_INCREASING -> STRICTLY_ASCENDING
                    STRICTLY_DECREASING -> STRICTLY_DESCENDING
                    MONOTONIC -> CLUSTERED
                    else -> throw AssertionError("unknown: $monotonicity")
                }
            }
        }
    }

    /**
     * Ordering of nulls.
     */
    enum class NullDirection(val nullComparison: Int) {
        FIRST(-1), LAST(1), UNSPECIFIED(1);
    }
    //~ Instance fields --------------------------------------------------------

    /**
     * Direction of sorting.
     */
    val direction: Direction

    /**
     * Direction of sorting of nulls.
     */
    val nullDirection: NullDirection
    /**
     * Creates a field collation.
     */
    /**
     * Creates a field collation with unspecified null direction.
     */
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an ascending field collation.
     */
    init {
        this.direction = Objects.requireNonNull(direction, "direction")
        this.nullDirection = Objects.requireNonNull(nullDirection, "nullDirection")
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Creates a copy of this RelFieldCollation against a different field.
     */
    fun withFieldIndex(fieldIndex: Int): RelFieldCollation {
        return if (this.fieldIndex == fieldIndex) this else RelFieldCollation(fieldIndex, direction, nullDirection)
    }

    @Deprecated // to be removed before 2.0
    fun copy(target: Int): RelFieldCollation {
        return withFieldIndex(target)
    }

    /** Creates a copy of this RelFieldCollation with a different direction.  */
    fun withDirection(direction: Direction): RelFieldCollation {
        return if (this.direction == direction) this else RelFieldCollation(fieldIndex, direction, nullDirection)
    }

    /** Creates a copy of this RelFieldCollation with a different null
     * direction.  */
    fun withNullDirection(nullDirection: NullDirection): RelFieldCollation {
        return if (this.nullDirection == nullDirection) this else RelFieldCollation(
            fieldIndex,
            direction,
            nullDirection
        )
    }

    /**
     * Returns a copy of this RelFieldCollation with the field index shifted
     * `offset` to the right.
     */
    fun shift(offset: Int): RelFieldCollation {
        return withFieldIndex(fieldIndex + offset)
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        return (this === o
                || (o is RelFieldCollation
                && fieldIndex == (o as RelFieldCollation).fieldIndex && direction == (o as RelFieldCollation).direction && nullDirection == (o as RelFieldCollation).nullDirection))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(fieldIndex, direction, nullDirection)
    }

    @Override
    override fun toString(): String {
        if (direction == Direction.ASCENDING
            && nullDirection == direction.defaultNullDirection()
        ) {
            return String.valueOf(fieldIndex)
        }
        val sb = StringBuilder()
        sb.append(fieldIndex).append(" ").append(direction.shortString)
        if (nullDirection != direction.defaultNullDirection()) {
            sb.append(" ").append(nullDirection)
        }
        return sb.toString()
    }

    fun shortString(): String {
        return if (nullDirection == direction.defaultNullDirection()) {
            direction.shortString
        } else when (nullDirection) {
            NullDirection.FIRST -> direction.shortString + "-nulls-first"
            NullDirection.LAST -> direction.shortString + "-nulls-last"
            else -> direction.shortString
        }
    }

    companion object {
        /** Utility method that compares values taking into account null
         * direction.  */
        fun compare(@Nullable c1: Comparable?, @Nullable c2: Comparable?, nullComparison: Int): Int {
            return if (c1 === c2) {
                0
            } else if (c1 == null) {
                nullComparison
            } else if (c2 == null) {
                -nullComparison
            } else {
                c1.compareTo(c2)
            }
        }
    }
}
