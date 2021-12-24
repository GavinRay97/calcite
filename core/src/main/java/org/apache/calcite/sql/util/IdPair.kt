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
package org.apache.calcite.sql.util

import java.util.List

/** Similar to [org.apache.calcite.util.Pair] but identity is based
 * on identity of values.
 *
 *
 * Also, uses `hashCode` algorithm of [List],
 * not [Map.Entry.hashCode].
 *
 * @param <L> Left type
 * @param <R> Right type
</R></L> */
class IdPair<L, R> protected constructor(left: L, right: R) {
    val left: L
    val right: R

    init {
        this.left = Objects.requireNonNull(left, "left")
        this.right = Objects.requireNonNull(right, "right")
    }

    @Override
    override fun toString(): String {
        return left.toString() + "=" + right
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is IdPair<*, *>
                && left === (obj as IdPair<*, *>).left && right === (obj as IdPair<*, *>).right))
    }

    @Override
    override fun hashCode(): Int {
        return ((31
                + System.identityHashCode(left)) * 31
                + System.identityHashCode(right))
    }

    companion object {
        /** Creates an IdPair.  */
        fun <L, R> of(left: L, right: R): IdPair<L, R> {
            return IdPair(left, right)
        }
    }
}
