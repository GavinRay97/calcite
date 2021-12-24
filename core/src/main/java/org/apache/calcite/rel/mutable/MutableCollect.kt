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

/** Mutable equivalent of [org.apache.calcite.rel.core.Collect].  */
class MutableCollect private constructor(
    rowType: RelDataType,
    input: MutableRel, val fieldName: String
) : MutableSingleRel(MutableRelType.COLLECT, rowType, input) {
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableCollect
                && fieldName.equals((obj as MutableCollect).fieldName)
                && input!!.equals((obj as MutableCollect).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(input, fieldName)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Collect(fieldName: ").append(fieldName).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(rowType, input!!.clone(), fieldName)
    }

    companion object {
        /**
         * Creates a MutableCollect.
         *
         * @param rowType   Row type
         * @param input     Input relational expression
         * @param fieldName Name of the sole output field
         */
        fun of(
            rowType: RelDataType,
            input: MutableRel, fieldName: String
        ): MutableCollect {
            return MutableCollect(rowType, input, fieldName)
        }
    }
}
