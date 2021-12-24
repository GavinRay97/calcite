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

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlKind
import java.util.List
import java.util.Objects

/**
 * Variable which references a column of a table occurrence in a relational plan.
 *
 *
 * This object is used by
 * [org.apache.calcite.rel.metadata.BuiltInMetadata.ExpressionLineage]
 * and [org.apache.calcite.rel.metadata.BuiltInMetadata.AllPredicates].
 *
 *
 * Given a relational expression, its purpose is to be able to reference uniquely
 * the provenance of a given expression. For that, it uses a unique table reference
 * (contained in a [RelTableRef]) and an column index within the table.
 *
 *
 * For example, `A.#0.$3 + 2` column `$3` in the `0`
 * occurrence of table `A` in the plan.
 *
 *
 * Note that this kind of [RexNode] is an auxiliary data structure with
 * a very specific purpose and should not be used in relational expressions.
 */
class RexTableInputRef private constructor(val tableRef: RelTableRef, index: Int, type: RelDataType) :
    RexInputRef(index, type) {

    init {
        this.digest = "$tableRef.$$index"
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is RexTableInputRef
                && tableRef.equals((obj as RexTableInputRef).tableRef)
                && index === (obj as org.apache.calcite.rex.RexTableInputRef?).index))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hashCode(digest)
    }

    val qualifiedName: List<String>
        get() = tableRef.qualifiedName
    val identifier: Int
        get() = tableRef.entityNumber

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitTableInputRef(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitTableInputRef(this, arg)
    }

    @get:Override
    val kind: SqlKind
        get() = SqlKind.TABLE_INPUT_REF

    /** Identifies uniquely a table by its qualified name and its entity number
     * (occurrence).  */
    class RelTableRef private constructor(table: RelOptTable, entityNumber: Int) : Comparable<RelTableRef?> {
        private val table: RelOptTable
        val entityNumber: Int
        private val digest: String

        init {
            this.table = table
            this.entityNumber = entityNumber
            digest = table.getQualifiedName() + ".#" + entityNumber
        }

        //~ Methods ----------------------------------------------------------------
        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (this === obj
                    || (obj is RelTableRef
                    && table.getQualifiedName().equals((obj as RelTableRef).qualifiedName)
                    && entityNumber == (obj as RelTableRef).entityNumber))
        }

        @Override
        override fun hashCode(): Int {
            return digest.hashCode()
        }

        fun getTable(): RelOptTable {
            return table
        }

        val qualifiedName: List<String>
            get() = table.getQualifiedName()

        @Override
        override fun toString(): String {
            return digest
        }

        @Override
        operator fun compareTo(o: RelTableRef): Int {
            return digest.compareTo(o.digest)
        }

        companion object {
            fun of(table: RelOptTable, entityNumber: Int): RelTableRef {
                return RelTableRef(table, entityNumber)
            }
        }
    }

    companion object {
        fun of(tableRef: RelTableRef, index: Int, type: RelDataType): RexTableInputRef {
            return RexTableInputRef(tableRef, index, type)
        }

        fun of(tableRef: RelTableRef, ref: RexInputRef): RexTableInputRef {
            return RexTableInputRef(tableRef, ref.getIndex(), ref.getType())
        }
    }
}
