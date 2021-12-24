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

import org.apache.calcite.rel.RelDistribution

/** Mutable equivalent of [org.apache.calcite.rel.core.Exchange].  */
class MutableExchange private constructor(input: MutableRel, distribution: RelDistribution) :
    MutableSingleRel(MutableRelType.EXCHANGE, input.rowType, input) {
    val distribution: RelDistribution

    init {
        this.distribution = distribution
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableExchange
                && distribution.equals((obj as MutableExchange).distribution)
                && input!!.equals((obj as MutableExchange).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(input, distribution)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Exchange(distribution: ").append(distribution).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(input!!.clone(), distribution)
    }

    companion object {
        /**
         * Creates a MutableExchange.
         *
         * @param input         Input relational expression
         * @param distribution  Distribution specification
         */
        fun of(input: MutableRel, distribution: RelDistribution): MutableExchange {
            return MutableExchange(input, distribution)
        }
    }
}
