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
package org.apache.calcite.rel.mutable

import org.apache.calcite.rel.core.CorrelationId

/** Mutable equivalent of [org.apache.calcite.rel.core.Join].  */
class MutableJoin private constructor(
    rowType: RelDataType,
    left: MutableRel,
    right: MutableRel,
    condition: RexNode,
    joinType: JoinRelType,
    variablesSet: Set<CorrelationId>
) : MutableBiRel(MutableRelType.JOIN, left.cluster, rowType, left, right) {
    val condition: RexNode
    val variablesSet: Set<CorrelationId>
    val joinType: JoinRelType

    init {
        this.condition = condition
        this.variablesSet = variablesSet
        this.joinType = joinType
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableJoin
                && joinType === (obj as MutableJoin).joinType && condition.equals((obj as MutableJoin).condition)
                && Objects.equals(
            variablesSet,
            (obj as MutableJoin).variablesSet
        )
                && left!!.equals((obj as MutableJoin).left)
                && right!!.equals((obj as MutableJoin).right)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(left, right, condition, joinType, variablesSet)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Join(joinType: ").append(joinType)
            .append(", condition: ").append(condition)
            .append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(
            rowType, left!!.clone(),
            right!!.clone(), condition, joinType, variablesSet
        )
    }

    companion object {
        /**
         * Creates a MutableJoin.
         *
         * @param rowType           Row type
         * @param left              Left input relational expression
         * @param right             Right input relational expression
         * @param condition         Join condition
         * @param joinType          Join type
         * @param variablesStopped  Set of variables that are set by the LHS and
         * used by the RHS and are not available to
         * nodes above this join in the tree
         */
        fun of(
            rowType: RelDataType, left: MutableRel,
            right: MutableRel, condition: RexNode, joinType: JoinRelType,
            variablesStopped: Set<CorrelationId>
        ): MutableJoin {
            return MutableJoin(
                rowType, left, right, condition, joinType,
                variablesStopped
            )
        }
    }
}
