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

import org.apache.calcite.rel.type.RelDataType

/** Mutable equivalent of [org.apache.calcite.rel.core.Uncollect].  */
class MutableUncollect private constructor(
    rowType: RelDataType,
    input: MutableRel, val withOrdinality: Boolean
) : MutableSingleRel(MutableRelType.UNCOLLECT, rowType, input) {
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableUncollect
                && withOrdinality == (obj as MutableUncollect).withOrdinality && input!!.equals((obj as MutableUncollect).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(input, withOrdinality)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Uncollect(withOrdinality: ")
            .append(withOrdinality).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(rowType, input!!.clone(), withOrdinality)
    }

    companion object {
        /**
         * Creates a MutableUncollect.
         *
         * @param rowType         Row type
         * @param input           Input relational expression
         * @param withOrdinality  Whether the output contains an extra
         * `ORDINALITY` column
         */
        fun of(
            rowType: RelDataType,
            input: MutableRel, withOrdinality: Boolean
        ): MutableUncollect {
            return MutableUncollect(rowType, input, withOrdinality)
        }
    }
}
