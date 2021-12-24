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

/**
 * Rule to convert a [LogicalWindow] to an [EnumerableWindow].
 * You may provide a custom config to convert other nodes that extend [Window].
 *
 * @see EnumerableRules.ENUMERABLE_WINDOW_RULE
 */
internal class EnumerableWindowRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val winAgg: Window = rel as Window
        val traitSet: RelTraitSet = winAgg.getTraitSet().replace(EnumerableConvention.INSTANCE)
        val child: RelNode = winAgg.getInput()
        val convertedChild: RelNode = convert(
            child,
            child.getTraitSet().replace(EnumerableConvention.INSTANCE)
        )
        return EnumerableWindow(
            rel.getCluster(), traitSet, convertedChild,
            winAgg.getConstants(), winAgg.getRowType(), winAgg.groups
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalWindow::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableWindowRule"
            )
            .withRuleFactory { config: Config? -> EnumerableWindowRule(config) }
    }
}
