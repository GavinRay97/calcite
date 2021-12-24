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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.RelOptCluster

/**
 * Base class for any join whose condition is based on column equality.
 *
 *
 * For most of the cases, [JoinInfo.isEqui] can already decide
 * if the join condition is based on column equality.
 *
 *
 * `EquiJoin` is an abstract class for inheritance of Calcite enumerable
 * joins and join implementation of other system. You should inherit the `EquiJoin`
 * if your join implementation does not support non-equi join conditions. Calcite would
 * eliminate some optimize logic for `EquiJoin` in some planning rules.
 * e.g. [org.apache.calcite.rel.rules.FilterJoinRule] would not push non-equi
 * join conditions of the above filter into the join underneath if it is an `EquiJoin`.
 *
 */
@Deprecated // to be removed before 2.0
@Deprecated(
    """This class is no longer needed; if you are writing a sub-class of
  Join that only accepts equi conditions, it is sufficient that it extends
  {@link Join}. It will be evident that it is an equi-join when its
  {@link JoinInfo#nonEquiConditions} is an empty list."""
)
abstract class EquiJoin : Join {
    val leftKeys: ImmutableIntList
    val rightKeys: ImmutableIntList

    /** Creates an EquiJoin.  */
    protected constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?, left: RelNode?,
        right: RelNode?, condition: RexNode?, variablesSet: Set<CorrelationId?>?,
        joinType: JoinRelType?
    ) : super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType) {
        leftKeys = Objects.requireNonNull(joinInfo.leftKeys)
        rightKeys = Objects.requireNonNull(joinInfo.rightKeys)
        assert(joinInfo.isEqui()) { "Create EquiJoin with non-equi join condition." }
    }

    /** Creates an EquiJoin.  */
    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?, left: RelNode?,
        right: RelNode?, condition: RexNode?, leftKeys: ImmutableIntList?,
        rightKeys: ImmutableIntList?, variablesSet: Set<CorrelationId?>?,
        joinType: JoinRelType?
    ) : super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType) {
        this.leftKeys = Objects.requireNonNull(leftKeys, "leftKeys")
        this.rightKeys = Objects.requireNonNull(rightKeys, "rightKeys")
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?, left: RelNode?,
        right: RelNode?, condition: RexNode?, leftKeys: ImmutableIntList?,
        rightKeys: ImmutableIntList?, joinType: JoinRelType?,
        variablesStopped: Set<String>
    ) : this(
        cluster, traits, left, right, condition, leftKeys, rightKeys,
        CorrelationId.setOf(variablesStopped), joinType
    ) {
    }

    fun getLeftKeys(): ImmutableIntList {
        return leftKeys
    }

    fun getRightKeys(): ImmutableIntList {
        return rightKeys
    }
}
