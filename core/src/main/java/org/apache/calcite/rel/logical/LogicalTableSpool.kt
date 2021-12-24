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

import org.apache.calcite.linq4j.function.Experimental

/**
 * Sub-class of [TableSpool] not targeted at any particular engine or
 * calling convention.
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
class LogicalTableSpool  //~ Constructors -----------------------------------------------------------
    (
    cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
    readType: Type?, writeType: Type?, table: RelOptTable?
) : TableSpool(cluster, traitSet, input, readType, writeType, table) {
    //~ Methods ----------------------------------------------------------------
    @Override
    protected fun copy(
        traitSet: RelTraitSet?, input: RelNode,
        readType: Type?, writeType: Type?
    ): Spool {
        return LogicalTableSpool(
            input.getCluster(), traitSet, input,
            readType, writeType, table
        )
    }

    companion object {
        /** Creates a LogicalTableSpool.  */
        fun create(
            input: RelNode, readType: Type?,
            writeType: Type?, table: RelOptTable?
        ): LogicalTableSpool {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { mq.collations(input) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { mq.distribution(input) }
            return LogicalTableSpool(cluster, traitSet, input, readType, writeType, table)
        }
    }
}
