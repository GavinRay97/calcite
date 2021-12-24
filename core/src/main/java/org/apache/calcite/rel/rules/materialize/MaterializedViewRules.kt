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
package org.apache.calcite.rel.rules.materialize

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.rules.MaterializedViewFilterScanRule

/**
 * Collection of rules pertaining to materialized views.
 *
 *
 * Also may contain utilities for [MaterializedViewRule].
 */
object MaterializedViewRules {
    /** Rule that matches [Project] on [Aggregate].  */
    val PROJECT_AGGREGATE: RelOptRule = MaterializedViewProjectAggregateRule.Config.DEFAULT.toRule()

    /** Rule that matches [Aggregate].  */
    val AGGREGATE: RelOptRule = MaterializedViewOnlyAggregateRule.Config.DEFAULT.toRule()

    /** Rule that matches [Filter].  */
    val FILTER: RelOptRule = MaterializedViewOnlyFilterRule.Config.DEFAULT.toRule()

    /** Rule that matches [Join].  */
    val JOIN: RelOptRule = MaterializedViewOnlyJoinRule.Config.DEFAULT.toRule()

    /** Rule that matches [Project] on [Filter].  */
    val PROJECT_FILTER: RelOptRule = MaterializedViewProjectFilterRule.Config.DEFAULT.toRule()

    /** Rule that matches [Project] on [Join].  */
    val PROJECT_JOIN: RelOptRule = MaterializedViewProjectJoinRule.Config.DEFAULT.toRule()

    /** Rule that converts a [Filter] on a [TableScan]
     * to a [Filter] on a Materialized View.  */
    val FILTER_SCAN: MaterializedViewFilterScanRule = MaterializedViewFilterScanRule.Config.DEFAULT.toRule()
}
