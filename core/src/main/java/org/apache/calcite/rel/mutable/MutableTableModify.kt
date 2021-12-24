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

import org.apache.calcite.plan.RelOptTable

/** Mutable equivalent of [org.apache.calcite.rel.core.TableModify].  */
class MutableTableModify private constructor(
    rowType: RelDataType, input: MutableRel,
    table: RelOptTable, catalogReader: CatalogReader,
    operation: Operation, @Nullable updateColumnList: List<String>?,
    @Nullable sourceExpressionList: List<RexNode>?, flattened: Boolean
) : MutableSingleRel(MutableRelType.TABLE_MODIFY, rowType, input) {
    val catalogReader: CatalogReader
    val table: RelOptTable
    val operation: Operation

    @Nullable
    val updateColumnList: List<String>?

    @Nullable
    val sourceExpressionList: List<RexNode>?
    val flattened: Boolean

    init {
        this.table = table
        this.catalogReader = catalogReader
        this.operation = operation
        this.updateColumnList = updateColumnList
        this.sourceExpressionList = sourceExpressionList
        this.flattened = flattened
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableTableModify
                && table.getQualifiedName().equals(
            (obj as MutableTableModify).table.getQualifiedName()
        )
                && operation === (obj as MutableTableModify).operation && Objects.equals(
            updateColumnList,
            (obj as MutableTableModify).updateColumnList
        )
                && MutableRel.PAIRWISE_STRING_EQUIVALENCE.equivalent(
            sourceExpressionList, (obj as MutableTableModify).sourceExpressionList
        )
                && flattened == (obj as MutableTableModify).flattened && input!!.equals((obj as MutableTableModify).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(
            input, table.getQualifiedName(),
            operation, updateColumnList,
            MutableRel.PAIRWISE_STRING_EQUIVALENCE.hash(sourceExpressionList),
            flattened
        )
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        buf.append("TableModify(table: ").append(table.getQualifiedName())
            .append(", operation: ").append(operation)
        if (updateColumnList != null) {
            buf.append(", updateColumnList: ").append(updateColumnList)
        }
        if (sourceExpressionList != null) {
            buf.append(", sourceExpressionList: ").append(sourceExpressionList)
        }
        return buf.append(", flattened: ").append(flattened).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(
            rowType, input!!.clone(), table, catalogReader,
            operation, updateColumnList, sourceExpressionList, flattened
        )
    }

    companion object {
        /**
         * Creates a MutableTableModify.
         *
         * @param rowType               Row type
         * @param input                 Input relational expression
         * @param table                 Target table to modify
         * @param catalogReader         Accessor to the table metadata
         * @param operation             Modify operation (INSERT, UPDATE, DELETE)
         * @param updateColumnList      List of column identifiers to be updated
         * (e.g. ident1, ident2); null if not UPDATE
         * @param sourceExpressionList  List of value expressions to be set
         * (e.g. exp1, exp2); null if not UPDATE
         * @param flattened             Whether set flattens the input row type
         */
        fun of(
            rowType: RelDataType,
            input: MutableRel, table: RelOptTable,
            catalogReader: CatalogReader,
            operation: Operation, @Nullable updateColumnList: List<String>?,
            @Nullable sourceExpressionList: List<RexNode>?, flattened: Boolean
        ): MutableTableModify {
            return MutableTableModify(
                rowType, input, table, catalogReader,
                operation, updateColumnList, sourceExpressionList, flattened
            )
        }
    }
}
