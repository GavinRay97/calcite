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

/** Planner rule that converts a [LogicalTableModify] to an [EnumerableTableModify].
 * You may provide a custom config to convert other nodes that extend [TableModify].
 *
 * @see EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE
 */
class EnumerableTableModifyRule
/** Creates an EnumerableTableModifyRule.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    @Nullable
    fun convert(rel: RelNode): RelNode? {
        val modify: TableModify = rel as TableModify
        val modifiableTable: ModifiableTable = modify.getTable().unwrap(ModifiableTable::class.java) ?: return null
        val traitSet: RelTraitSet = modify.getTraitSet().replace(EnumerableConvention.INSTANCE)
        return EnumerableTableModify(
            modify.getCluster(), traitSet,
            modify.getTable(),
            modify.getCatalogReader(),
            convert(modify.getInput(), traitSet),
            modify.getOperation(),
            modify.getUpdateColumnList(),
            modify.getSourceExpressionList(),
            modify.isFlattened()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalTableModify::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableTableModificationRule"
            )
            .withRuleFactory { config: Config? -> EnumerableTableModifyRule(config) }
    }
}
