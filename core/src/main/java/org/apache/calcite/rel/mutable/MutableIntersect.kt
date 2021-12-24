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

import org.apache.calcite.plan.RelOptCluster

/** Mutable equivalent of [org.apache.calcite.rel.core.Intersect].  */
class MutableIntersect private constructor(
    cluster: RelOptCluster, rowType: RelDataType,
    inputs: List<MutableRel>, all: Boolean
) : MutableSetOp(cluster, rowType, MutableRelType.INTERSECT, inputs, all) {
    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Intersect(all: ").append(all).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(rowType, cloneChildren(), all)
    }

    companion object {
        /**
         * Creates a MutableIntersect.
         *
         * @param rowType Row type
         * @param inputs  Input relational expressions
         * @param all     Whether to perform a multiset intersection or a set
         * intersection
         */
        fun of(
            rowType: RelDataType, inputs: List<MutableRel>, all: Boolean
        ): MutableIntersect {
            assert(inputs.size() >= 2)
            val input0: MutableRel = inputs[0]
            return MutableIntersect(input0.cluster, rowType, inputs, all)
        }
    }
}
