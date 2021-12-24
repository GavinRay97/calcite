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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.plan.Convention

/** Planner rule that converts a [LogicalTableScan] to an [EnumerableTableScan].
 * You may provide a custom config to convert other nodes that extend [TableScan].
 *
 * @see EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE
 */
class EnumerableTableScanRule protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    @Nullable
    fun convert(rel: RelNode): RelNode? {
        val scan: TableScan = rel as TableScan
        val relOptTable: RelOptTable = scan.getTable()
        val table: Table = relOptTable.unwrap(Table::class.java)
        // The QueryableTable can only be implemented as ENUMERABLE convention,
        // but some test QueryableTables do not really implement the expressions,
        // just skips the QueryableTable#getExpression invocation and returns early.
        return if (table is QueryableTable || relOptTable.getExpression(Object::class.java) != null) {
            EnumerableTableScan.create(scan.getCluster(), relOptTable)
        } else null
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalTableScan::class.java,
                { r -> EnumerableTableScan.canHandle(r.getTable()) },
                Convention.NONE, EnumerableConvention.INSTANCE,
                "EnumerableTableScanRule"
            )
            .withRuleFactory { config: Config? -> EnumerableTableScanRule(config) }
    }
}
