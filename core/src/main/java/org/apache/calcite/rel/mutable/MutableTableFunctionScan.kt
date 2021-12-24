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

import org.apache.calcite.plan.RelOptCluster

/** Mutable equivalent of
 * [org.apache.calcite.rel.core.TableFunctionScan].  */
class MutableTableFunctionScan private constructor(
    cluster: RelOptCluster,
    rowType: RelDataType, inputs: List<MutableRel>, rexCall: RexNode,
    @Nullable elementType: Type?, @Nullable columnMappings: Set<RelColumnMapping>
) : MutableMultiRel(cluster, rowType, MutableRelType.TABLE_FUNCTION_SCAN, inputs) {
    val rexCall: RexNode

    @Nullable
    val elementType: Type?

    @Nullable
    val columnMappings: Set<RelColumnMapping>

    init {
        this.rexCall = rexCall
        this.elementType = elementType
        this.columnMappings = columnMappings
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableTableFunctionScan
                && STRING_EQUIVALENCE.equivalent(
            rexCall,
            (obj as MutableTableFunctionScan).rexCall
        )
                && Objects.equals(
            elementType,
            (obj as MutableTableFunctionScan).elementType
        )
                && Objects.equals(
            columnMappings,
            (obj as MutableTableFunctionScan).columnMappings
        )
                && inputs.equals((obj as MutableTableFunctionScan).getInputs())))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(
            inputs,
            STRING_EQUIVALENCE.hash(rexCall), elementType, columnMappings
        )
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        buf.append("TableFunctionScan(rexCall: ").append(rexCall)
        if (elementType != null) {
            buf.append(", elementType: ").append(elementType)
        }
        return buf.append(")")
    }

    @Override
    override fun clone(): MutableRel {
        // TODO Auto-generated method stub
        return of(
            cluster, rowType,
            cloneChildren(), rexCall, elementType, columnMappings
        )
    }

    companion object {
        /**
         * Creates a MutableTableFunctionScan.
         *
         * @param cluster         Cluster that this relational expression belongs to
         * @param rowType         Row type
         * @param inputs          Input relational expressions
         * @param rexCall         Function invocation expression
         * @param elementType     Element type of the collection that will implement
         * this table
         * @param columnMappings  Column mappings associated with this function
         */
        fun of(
            cluster: RelOptCluster,
            rowType: RelDataType, inputs: List<MutableRel>, rexCall: RexNode,
            @Nullable elementType: Type?, @Nullable columnMappings: Set<RelColumnMapping>
        ): MutableTableFunctionScan {
            return MutableTableFunctionScan(
                cluster, rowType, inputs, rexCall, elementType, columnMappings
            )
        }
    }
}
