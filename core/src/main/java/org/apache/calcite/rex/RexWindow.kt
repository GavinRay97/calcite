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

import org.apache.calcite.util.Pair
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import java.util.List
import java.util.Objects

/**
 * Specification of the window of rows over which a [RexOver] windowed
 * aggregate is evaluated.
 *
 *
 * Treat it as immutable!
 */
class RexWindow @SuppressWarnings("method.invocation.invalid") internal constructor(
    partitionKeys: List<RexNode?>?,
    orderKeys: List<RexFieldCollation?>?,
    lowerBound: RexWindowBound,
    upperBound: RexWindowBound,
    isRows: Boolean
) {
    //~ Instance fields --------------------------------------------------------
    val partitionKeys: ImmutableList<RexNode>
    val orderKeys: ImmutableList<RexFieldCollation>
    private val lowerBound: RexWindowBound?
    private val upperBound: RexWindowBound?
    val isRows: Boolean
    private val digest: String
    val nodeCount: Int
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a window.
     *
     *
     * If you need to create a window from outside this package, use
     * [RexBuilder.makeOver].
     *
     *
     * If `orderKeys` is empty the bracket will usually be
     * "BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING".
     *
     *
     * The digest assumes 'default' brackets, and does not print brackets or
     * bounds that are the default.
     *
     *
     * If `orderKeys` is empty, assumes the bracket is "RANGE BETWEEN
     * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" and does not print the
     * bracket.
     *
     *  * If `orderKeys` is not empty, the default top is "CURRENT ROW".
     * The default bracket is "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
     * which will be printed as blank.
     * "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" is different, and is
     * printed as "ROWS UNBOUNDED PRECEDING".
     * "ROWS BETWEEN 5 PRECEDING AND CURRENT ROW" is printed as
     * "ROWS 5 PRECEDING".
     */
    init {
        this.partitionKeys = ImmutableList.copyOf(partitionKeys)
        this.orderKeys = ImmutableList.copyOf(orderKeys)
        this.lowerBound = Objects.requireNonNull(lowerBound, "lowerBound")
        this.upperBound = Objects.requireNonNull(upperBound, "upperBound")
        this.isRows = isRows
        nodeCount = computeCodeCount()
        digest = computeDigest()
        Preconditions.checkArgument(
            !(lowerBound.isUnbounded() && lowerBound.isPreceding()
                    && upperBound.isUnbounded() && upperBound.isFollowing() && isRows),
            "use RANGE for unbounded, not ROWS"
        )
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun toString(): String {
        return digest
    }

    @Override
    override fun hashCode(): Int {
        return digest.hashCode()
    }

    @Override
    override fun equals(@Nullable that: Object): Boolean {
        if (that is RexWindow) {
            val window = that as RexWindow
            return digest.equals(window.digest)
        }
        return false
    }

    private fun computeDigest(): String {
        return appendDigest_(StringBuilder(), true).toString()
    }

    fun appendDigest(sb: StringBuilder, allowFraming: Boolean): StringBuilder {
        return if (allowFraming) {
            // digest was calculated with allowFraming=true; reuse it
            sb.append(digest)
        } else {
            appendDigest_(sb, allowFraming)
        }
    }

    private fun appendDigest_(sb: StringBuilder, allowFraming: Boolean): StringBuilder {
        val initialLength: Int = sb.length()
        if (partitionKeys.size() > 0) {
            sb.append("PARTITION BY ")
            for (i in 0 until partitionKeys.size()) {
                if (i > 0) {
                    sb.append(", ")
                }
                sb.append(partitionKeys.get(i))
            }
        }
        if (orderKeys.size() > 0) {
            sb.append(if (sb.length() > initialLength) " ORDER BY " else "ORDER BY ")
            for (i in 0 until orderKeys.size()) {
                if (i > 0) {
                    sb.append(", ")
                }
                sb.append(orderKeys.get(i))
            }
        }
        // There are 3 reasons to skip the ROWS/RANGE clause.
        // 1. If this window is being used with a RANK-style function that does not
        //    allow framing, or
        // 2. If there is no ORDER BY (in which case a frame is invalid), or
        // 3. If the ROWS/RANGE clause is the default, "RANGE BETWEEN UNBOUNDED
        //    PRECEDING AND CURRENT ROW"
        if (!allowFraming // 1
            || orderKeys.isEmpty() // 2
            || (lowerBound!!.isUnbounded() // 3
                    && lowerBound!!.isPreceding()
                    && upperBound!!.isCurrentRow()
                    && !isRows)
        ) {
            // No ROWS or RANGE clause
        } else if (upperBound!!.isCurrentRow()) {
            // Per MSSQL: If ROWS/RANGE is specified and <window frame preceding>
            // is used for <window frame extent> (short syntax) then this
            // specification is used for the window frame boundary starting point and
            // CURRENT ROW is used for the boundary ending point. For example
            // "ROWS 5 PRECEDING" is equal to "ROWS BETWEEN 5 PRECEDING AND CURRENT
            // ROW".
            //
            // By similar reasoning to (3) above, we print the shorter option if it is
            // the default. If the RexWindow is, say, "ROWS BETWEEN 5 PRECEDING AND
            // CURRENT ROW", we output "ROWS 5 PRECEDING" because it is equivalent and
            // is shorter.
            sb.append(if (sb.length() > initialLength) if (isRows) " ROWS " else " RANGE " else if (isRows) "ROWS " else "RANGE ")
                .append(lowerBound)
        } else {
            sb.append(if (sb.length() > initialLength) if (isRows) " ROWS BETWEEN " else " RANGE BETWEEN " else if (isRows) "ROWS BETWEEN " else "RANGE BETWEEN ")
                .append(lowerBound)
                .append(" AND ")
                .append(upperBound)
        }
        return sb
    }

    fun getLowerBound(): RexWindowBound? {
        return lowerBound
    }

    fun getUpperBound(): RexWindowBound? {
        return upperBound
    }

    private fun computeCodeCount(): Int {
        return (RexUtil.nodeCount(partitionKeys)
                + RexUtil.nodeCount(Pair.left(orderKeys))
                + (if (lowerBound == null) 0 else lowerBound.nodeCount())
                + if (upperBound == null) 0 else upperBound.nodeCount())
    }
}
