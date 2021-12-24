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
package org.apache.calcite.rel.mutable

import org.apache.calcite.rel.RelCollation

/** Mutable equivalent of [org.apache.calcite.rel.core.Sort].  */
class MutableSort private constructor(
    input: MutableRel, collation: RelCollation,
    @Nullable offset: RexNode?, @Nullable fetch: RexNode?
) : MutableSingleRel(MutableRelType.SORT, input.rowType, input) {
    val collation: RelCollation

    @Nullable
    val offset: RexNode?

    @Nullable
    val fetch: RexNode?

    init {
        this.collation = collation
        this.offset = offset
        this.fetch = fetch
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableSort
                && collation.equals((obj as MutableSort).collation)
                && Objects.equals(offset, (obj as MutableSort).offset)
                && Objects.equals(fetch, (obj as MutableSort).fetch)
                && input!!.equals((obj as MutableSort).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(input, collation, offset, fetch)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        buf.append("Sort(collation: ").append(collation)
        if (offset != null) {
            buf.append(", offset: ").append(offset)
        }
        if (fetch != null) {
            buf.append(", fetch: ").append(fetch)
        }
        return buf.append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(input!!.clone(), collation, offset, fetch)
    }

    companion object {
        /**
         * Creates a MutableSort.
         *
         * @param input     Input relational expression
         * @param collation Array of sort specifications
         * @param offset    Expression for number of rows to discard before returning
         * first row
         * @param fetch     Expression for number of rows to fetch
         */
        fun of(
            input: MutableRel, collation: RelCollation,
            @Nullable offset: RexNode?, @Nullable fetch: RexNode?
        ): MutableSort {
            return MutableSort(input, collation, offset, fetch)
        }
    }
}
