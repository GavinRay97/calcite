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
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.Pair
import java.util.List

/**
 * Variable which references a field of an input relational expression.
 *
 *
 * Fields of the input are 0-based. If there is more than one input, they are
 * numbered consecutively. For example, if the inputs to a join are
 *
 *
 *  * Input #0: EMP(EMPNO, ENAME, DEPTNO) and
 *  * Input #1: DEPT(DEPTNO AS DEPTNO2, DNAME)
 *
 *
 *
 * then the fields are:
 *
 *
 *  * Field #0: EMPNO
 *  * Field #1: ENAME
 *  * Field #2: DEPTNO (from EMP)
 *  * Field #3: DEPTNO2 (from DEPT)
 *  * Field #4: DNAME
 *
 *
 *
 * So `RexInputRef(3, Integer)` is the correct reference for the
 * field DEPTNO2.
 */
class RexInputRef  //~ Constructors -----------------------------------------------------------
/**
 * Creates an input variable.
 *
 * @param index Index of the field in the underlying row-type
 * @param type  Type of the column
 */
    (index: Int, type: RelDataType?) : RexSlot(createName(index), index, type) {
    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || obj is RexInputRef
                && index === (obj as org.apache.calcite.rex.RexInputRef).index)
    }

    @Override
    override fun hashCode(): Int {
        return index
    }

    @get:Override
    val kind: SqlKind
        get() = SqlKind.INPUT_REF

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitInputRef(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitInputRef(this, arg)
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        // list of common names, to reduce memory allocations
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private val NAMES: List<String> = SelfPopulatingList("$", 30)

        /**
         * Creates a reference to a given field in a row type.
         */
        fun of(index: Int, rowType: RelDataType): RexInputRef {
            return of(index, rowType.getFieldList())
        }

        /**
         * Creates a reference to a given field in a list of fields.
         */
        fun of(index: Int, fields: List<RelDataTypeField>): RexInputRef {
            return RexInputRef(index, fields[index].getType())
        }

        /**
         * Creates a reference to a given field in a list of fields.
         */
        fun of2(
            index: Int,
            fields: List<RelDataTypeField>
        ): Pair<RexNode, String> {
            val field: RelDataTypeField = fields[index]
            return Pair.of(
                RexInputRef(index, field.getType()) as RexNode,
                field.getName()
            )
        }

        /**
         * Creates a name for an input reference, of the form "$index". If the index
         * is low, uses a cache of common names, to reduce gc.
         */
        fun createName(index: Int): String {
            return NAMES[index]
        }
    }
}
