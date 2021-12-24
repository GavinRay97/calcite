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

import org.apache.calcite.linq4j.function.Experimental

/**
 * Rule to convert a [LogicalTableSpool] into an [EnumerableTableSpool].
 * You may provide a custom config to convert other nodes that extend [TableSpool].
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 *
 * @see EnumerableRules.ENUMERABLE_TABLE_SPOOL_RULE
 */
@Experimental
class EnumerableTableSpoolRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val spool: TableSpool = rel as TableSpool
        return EnumerableTableSpool.create(
            convert(
                spool.getInput(),
                spool.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE)
            ),
            spool.readType,
            spool.writeType,
            spool.getTable()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalTableSpool::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableTableSpoolRule"
            )
            .withRuleFactory { config: Config? -> EnumerableTableSpoolRule(config) }
    }
}
