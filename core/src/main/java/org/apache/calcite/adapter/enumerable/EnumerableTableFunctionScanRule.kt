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

/** Rule to convert a [LogicalTableFunctionScan] to an [EnumerableTableFunctionScan].
 * You may provide a custom config to convert other nodes that extend [TableFunctionScan].
 *
 * @see EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE
 */
class EnumerableTableFunctionScanRule
/** Creates an EnumerableTableFunctionScanRule.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val traitSet: RelTraitSet = rel.getTraitSet().replace(EnumerableConvention.INSTANCE)
        val scan: TableFunctionScan = rel as TableFunctionScan
        return EnumerableTableFunctionScan(
            rel.getCluster(), traitSet,
            convertList(scan.getInputs(), traitSet.getTrait(0)),
            scan.getElementType(), scan.getRowType(),
            scan.getCall(), scan.getColumnMappings()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalTableFunctionScan::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableTableFunctionScanRule"
            )
            .withRuleFactory { config: Config? -> EnumerableTableFunctionScanRule(config) }
    }
}
