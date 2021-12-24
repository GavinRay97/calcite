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

/** Mutable equivalent of [org.apache.calcite.rel.core.Match].  */
class MutableMatch private constructor(
    rowType: RelDataType, input: MutableRel,
    pattern: RexNode, strictStart: Boolean, strictEnd: Boolean,
    patternDefinitions: Map<String, RexNode>, measures: Map<String, RexNode>,
    after: RexNode, subsets: Map<String?, SortedSet<String?>?>,
    allRows: Boolean, partitionKeys: ImmutableBitSet, orderKeys: RelCollation,
    @Nullable interval: RexNode
) : MutableSingleRel(MutableRelType.MATCH, rowType, input) {
    val pattern: RexNode
    val strictStart: Boolean
    val strictEnd: Boolean
    val patternDefinitions: Map<String, RexNode>
    val measures: Map<String, RexNode>
    val after: RexNode
    val subsets: Map<String?, SortedSet<String?>?>
    val allRows: Boolean
    val partitionKeys: ImmutableBitSet
    val orderKeys: RelCollation

    @Nullable
    val interval: RexNode

    init {
        this.pattern = pattern
        this.strictStart = strictStart
        this.strictEnd = strictEnd
        this.patternDefinitions = patternDefinitions
        this.measures = measures
        this.after = after
        this.subsets = subsets
        this.allRows = allRows
        this.partitionKeys = partitionKeys
        this.orderKeys = orderKeys
        this.interval = interval
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableMatch
                && pattern.equals((obj as MutableMatch).pattern)
                && strictStart == (obj as MutableMatch).strictStart && strictEnd == (obj as MutableMatch).strictEnd && allRows == (obj as MutableMatch).allRows && patternDefinitions.equals(
            (obj as MutableMatch).patternDefinitions
        )
                && measures.equals((obj as MutableMatch).measures)
                && after.equals((obj as MutableMatch).after)
                && subsets.equals((obj as MutableMatch).subsets)
                && partitionKeys.equals((obj as MutableMatch).partitionKeys)
                && orderKeys.equals((obj as MutableMatch).orderKeys)
                && Objects.equals(interval, (obj as MutableMatch).interval)
                && input!!.equals((obj as MutableMatch).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(
            input, pattern, strictStart, strictEnd,
            patternDefinitions, measures, after, subsets, allRows,
            partitionKeys, orderKeys, interval
        )
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Match(pattern: ").append(pattern)
            .append(", strictStart: ").append(strictStart)
            .append(", strictEnd: ").append(strictEnd)
            .append(", patternDefinitions: ").append(patternDefinitions)
            .append(", measures: ").append(measures)
            .append(", after: ").append(after)
            .append(", subsets: ").append(subsets)
            .append(", allRows: ").append(allRows)
            .append(", partitionKeys: ").append(partitionKeys)
            .append(", orderKeys: ").append(orderKeys)
            .append(", interval: ").append(interval)
            .append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(
            rowType, input!!.clone(), pattern, strictStart,
            strictEnd, patternDefinitions, measures, after, subsets, allRows,
            partitionKeys, orderKeys, interval
        )
    }

    companion object {
        /**
         * Creates a MutableMatch.
         *
         */
        fun of(
            rowType: RelDataType,
            input: MutableRel, pattern: RexNode, strictStart: Boolean, strictEnd: Boolean,
            patternDefinitions: Map<String, RexNode>, measures: Map<String, RexNode>,
            after: RexNode, subsets: Map<String?, SortedSet<String?>?>,
            allRows: Boolean, partitionKeys: ImmutableBitSet, orderKeys: RelCollation,
            @Nullable interval: RexNode
        ): MutableMatch {
            return MutableMatch(
                rowType, input, pattern, strictStart, strictEnd,
                patternDefinitions, measures, after, subsets, allRows, partitionKeys,
                orderKeys, interval
            )
        }
    }
}
