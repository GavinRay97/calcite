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

import org.apache.calcite.runtime.Utilities

/**
 * Utilities concerning relational expressions.
 */
object RelNodes {
    /** Comparator that provides an arbitrary but stable ordering to
     * [RelNode]s.  */
    val COMPARATOR: Comparator<RelNode> = RelNodeComparator()

    /** Ordering for [RelNode]s.  */
    val ORDERING: Ordering<RelNode> = Ordering.from(COMPARATOR)

    /** Compares arrays of [RelNode].  */
    fun compareRels(rels0: Array<RelNode?>, rels1: Array<RelNode?>): Int {
        var c: Int = Utilities.compare(rels0.size, rels1.size)
        if (c != 0) {
            return c
        }
        for (i in rels0.indices) {
            c = COMPARATOR.compare(rels0[i], rels1[i])
            if (c != 0) {
                return c
            }
        }
        return 0
    }

    /** Arbitrary stable comparator for [RelNode]s.  */
    private class RelNodeComparator : Comparator<RelNode?> {
        @Override
        fun compare(o1: RelNode, o2: RelNode): Int {
            // Compare on field count first. It is more stable than id (when rules
            // are added to the set of active rules).
            val c: Int = Utilities.compare(
                o1.getRowType().getFieldCount(),
                o2.getRowType().getFieldCount()
            )
            return if (c != 0) {
                -c
            } else Utilities.compare(o1.getId(), o2.getId())
        }
    }
}
