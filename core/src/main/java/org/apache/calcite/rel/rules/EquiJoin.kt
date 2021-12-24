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
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.RelOptCluster

/**
 * Base class for any join whose condition is based on column equality.
 *
 */
@Deprecated // to be removed before 2.0
@Deprecated(
    """Use
  {@link org.apache.calcite.rel.core.EquiJoin EquiJoin in 'core' package}"""
)
abstract class EquiJoin protected constructor(
    cluster: RelOptCluster?, traits: RelTraitSet?, left: RelNode?,
    right: RelNode?, condition: RexNode?, leftKeys: ImmutableIntList?,
    rightKeys: ImmutableIntList?, joinType: JoinRelType?,
    variablesStopped: Set<String?>?
) : org.apache.calcite.rel.core.EquiJoin(
    cluster, traits, left, right, condition, leftKeys, rightKeys,
    CorrelationId.setOf(variablesStopped), joinType
)
