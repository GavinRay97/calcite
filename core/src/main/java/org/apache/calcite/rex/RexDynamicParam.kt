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
import java.util.Objects

/**
 * Dynamic parameter reference in a row-expression.
 */
class RexDynamicParam
/**
 * Creates a dynamic parameter.
 *
 * @param type  inferred type of parameter
 * @param index 0-based index of dynamic parameter in statement
 */(
    type: RelDataType?,
    //~ Instance fields --------------------------------------------------------
    val index: Int
) : RexVariable("?$index", type) {
    //~ Constructors -----------------------------------------------------------
    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.DYNAMIC_PARAM

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitDynamicParam(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitDynamicParam(this, arg)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is RexDynamicParam
                && type.equals((obj as RexDynamicParam).type)
                && index == (obj as RexDynamicParam).index))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(type, index)
    }
}
