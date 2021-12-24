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
package org.apache.calcite.rel.logical

import org.apache.calcite.plan.Convention

/**
 * Sub-class of [org.apache.calcite.rel.core.TableModify]
 * not targeted at any particular engine or calling convention.
 */
class LogicalTableModify : TableModify {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalTableModify.
     *
     *
     * Use [.create] unless you know what you're doing.
     */
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        table: RelOptTable?, schema: CatalogReader?, input: RelNode?,
        operation: Operation?, @Nullable updateColumnList: List<String?>?,
        @Nullable sourceExpressionList: List<RexNode?>?, flattened: Boolean
    ) : super(
        cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened
    ) {
    }

    /**
     * Creates a LogicalTableModify by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster, table: RelOptTable?,
        schema: CatalogReader?, input: RelNode?, operation: Operation?,
        updateColumnList: List<String?>?, flattened: Boolean
    ) : this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        table,
        schema,
        input,
        operation,
        updateColumnList,
        null,
        flattened
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet,
        inputs: List<RelNode?>?
    ): LogicalTableModify {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return LogicalTableModify(
            getCluster(), traitSet, table, catalogReader,
            sole(inputs), getOperation(), getUpdateColumnList(),
            getSourceExpressionList(), isFlattened()
        )
    }

    companion object {
        /** Creates a LogicalTableModify.  */
        fun create(
            table: RelOptTable?,
            schema: CatalogReader?, input: RelNode,
            operation: Operation?, @Nullable updateColumnList: List<String?>?,
            @Nullable sourceExpressionList: List<RexNode?>?, flattened: Boolean
        ): LogicalTableModify {
            val cluster: RelOptCluster = input.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return LogicalTableModify(
                cluster, traitSet, table, schema, input,
                operation, updateColumnList, sourceExpressionList, flattened
            )
        }
    }
}
