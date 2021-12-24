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

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlKind

/**
 * Variable that references a field of an input relational expression.
 */
class RexPatternFieldRef(val alpha: String, index: Int, type: RelDataType?) : RexInputRef(index, type) {

    init {
        digest = "$alpha.$$index"
    }

    @Override
    override fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitPatternFieldRef(this)
    }

    @Override
    override fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitPatternFieldRef(this, arg)
    }

    @get:Override
    override val kind: SqlKind
        get() = SqlKind.PATTERN_INPUT_REF

    companion object {
        fun of(alpha: String, index: Int, type: RelDataType?): RexPatternFieldRef {
            return RexPatternFieldRef(alpha, index, type)
        }

        fun of(alpha: String, ref: RexInputRef): RexPatternFieldRef {
            return RexPatternFieldRef(alpha, ref.getIndex(), ref.getType())
        }
    }
}
