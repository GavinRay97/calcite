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
import java.util.Objects

/**
 * Reference to a range of columns.
 *
 *
 * This construct is used only during the process of translating a
 * [SQL][org.apache.calcite.sql.SqlNode] tree to a
 * [rel][org.apache.calcite.rel.RelNode]/[rex][RexNode]
 * tree. *Regular [rex][RexNode] trees do not contain this
 * construct.*
 *
 *
 * While translating a join of EMP(EMPNO, ENAME, DEPTNO) to DEPT(DEPTNO2,
 * DNAME) we create `RexRangeRef(DeptType,3)` to represent the pair
 * of columns (DEPTNO2, DNAME) which came from DEPT. The type has 2 columns, and
 * therefore the range represents columns {3, 4} of the input.
 *
 *
 * Suppose we later create a reference to the DNAME field of this
 * RexRangeRef; it will return a `[RexInputRef](5,Integer)`,
 * and the [org.apache.calcite.rex.RexRangeRef] will disappear.
 */
class RexRangeRef internal constructor(
    rangeType: RelDataType,
    offset: Int
) : RexNode() {
    //~ Instance fields --------------------------------------------------------
    private override val type: RelDataType
    val offset: Int
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a range reference.
     *
     * @param rangeType Type of the record returned
     * @param offset    Offset of the first column within the input record
     */
    init {
        type = rangeType
        this.offset = offset
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun getType(): RelDataType {
        return type
    }

    @Override
    override fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitRangeRef(this)
    }

    @Override
    override fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitRangeRef(this, arg)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is RexRangeRef
                && type.equals((obj as RexRangeRef).type)
                && offset == (obj as RexRangeRef).offset))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(type, offset)
    }

    @Override
    override fun toString(): String {
        return "offset($offset)"
    }
}
