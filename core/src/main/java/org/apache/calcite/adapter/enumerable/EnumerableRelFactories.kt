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

import org.apache.calcite.plan.RelOptTable

/**
 * Contains factory interface and default implementation for creating various
 * rel nodes.
 */
object EnumerableRelFactories {
    val ENUMERABLE_TABLE_SCAN_FACTORY: org.apache.calcite.rel.core.RelFactories.TableScanFactory =
        TableScanFactoryImpl()
    val ENUMERABLE_PROJECT_FACTORY: org.apache.calcite.rel.core.RelFactories.ProjectFactory = ProjectFactoryImpl()
    val ENUMERABLE_FILTER_FACTORY: FilterFactory = FilterFactoryImpl()
    val ENUMERABLE_SORT_FACTORY: org.apache.calcite.rel.core.RelFactories.SortFactory = SortFactoryImpl()

    /**
     * Implementation of [org.apache.calcite.rel.core.RelFactories.TableScanFactory] that
     * returns a vanilla [EnumerableTableScan].
     */
    private class TableScanFactoryImpl : org.apache.calcite.rel.core.RelFactories.TableScanFactory {
        @Override
        fun createScan(toRelContext: RelOptTable.ToRelContext, table: RelOptTable?): RelNode {
            return EnumerableTableScan.create(toRelContext.getCluster(), table)
        }
    }

    /**
     * Implementation of [org.apache.calcite.rel.core.RelFactories.ProjectFactory] that
     * returns a vanilla [EnumerableProject].
     */
    private class ProjectFactoryImpl : org.apache.calcite.rel.core.RelFactories.ProjectFactory {
        @Override
        fun createProject(
            input: RelNode, hints: List<RelHint?>?,
            childExprs: List<RexNode?>?,
            @Nullable fieldNames: List<String?>?
        ): RelNode {
            val rowType: RelDataType = RexUtil.createStructType(
                input.getCluster().getTypeFactory(), childExprs,
                fieldNames, SqlValidatorUtil.F_SUGGESTER
            )
            return EnumerableProject.create(input, childExprs, rowType)
        }
    }

    /**
     * Implementation of [org.apache.calcite.rel.core.RelFactories.FilterFactory] that
     * returns a vanilla [EnumerableFilter].
     */
    private class FilterFactoryImpl : FilterFactory {
        @Override
        fun createFilter(
            input: RelNode, condition: RexNode?,
            variablesSet: Set<CorrelationId?>?
        ): RelNode {
            return EnumerableFilter.create(input, condition)
        }
    }

    /**
     * Implementation of [org.apache.calcite.rel.core.RelFactories.SortFactory] that
     * returns a vanilla [EnumerableSort].
     */
    private class SortFactoryImpl : org.apache.calcite.rel.core.RelFactories.SortFactory {
        @Override
        fun createSort(
            input: RelNode?, collation: RelCollation?,
            @Nullable offset: RexNode?, @Nullable fetch: RexNode?
        ): RelNode {
            return EnumerableSort.create(input, collation, offset, fetch)
        }
    }
}
