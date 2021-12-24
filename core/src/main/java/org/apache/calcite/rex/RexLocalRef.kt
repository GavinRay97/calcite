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
import java.util.List
import java.util.Objects

/**
 * Local variable.
 *
 *
 * Identity is based upon type and index. We want multiple references to the
 * same slot in the same context to be equal. A side effect is that references
 * to slots in different contexts which happen to have the same index and type
 * will be considered equal; this is not desired, but not too damaging, because
 * of the immutability.
 *
 *
 * Variables are immutable.
 */
class RexLocalRef(index: Int, type: RelDataType?) : RexSlot(createName(index), index, type) {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a local variable.
     *
     * @param index Index of the field in the underlying row type
     * @param type  Type of the column
     */
    init {
        assert(type != null)
        assert(index >= 0)
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.LOCAL_REF

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is RexLocalRef
                && Objects.equals(this.type, (obj as RexLocalRef).type)
                && this.index === (obj as org.apache.calcite.rex.RexLocalRef).index))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(type, index)
    }

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitLocalRef(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitLocalRef(this, arg)
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        // array of common names, to reduce memory allocations
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private val NAMES: List<String> = SelfPopulatingList("\$t", 30)
        private fun createName(index: Int): String {
            return NAMES[index]
        }
    }
}
