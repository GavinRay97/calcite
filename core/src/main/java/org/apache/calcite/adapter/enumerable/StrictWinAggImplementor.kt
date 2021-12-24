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

import org.apache.calcite.linq4j.tree.Expression

/**
 * The base implementation of strict window aggregate function.
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.FirstLastValueImplementor
 *
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.RankImplementor
 *
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.RowNumberImplementor
 */
abstract class StrictWinAggImplementor : StrictAggImplementor(), WinAggImplementor {
    protected abstract override fun implementNotNullAdd(
        info: WinAggContext?,
        add: WinAggAddContext?
    )

    protected override fun nonDefaultOnEmptySet(info: WinAggContext): Boolean {
        return super.nonDefaultOnEmptySet(info)
    }

    override fun getNotNullState(info: WinAggContext): List<Type> {
        return super.getNotNullState(info)
    }

    protected override fun implementNotNullReset(
        info: WinAggContext?,
        reset: WinAggResetContext
    ) {
        super.implementNotNullReset(info, reset)
    }

    protected override fun implementNotNullResult(
        info: WinAggContext?,
        result: WinAggResultContext
    ): Expression {
        return super.implementNotNullResult(info, result)
    }

    @Override
    protected override fun implementNotNullAdd(
        info: AggContext?,
        add: AggAddContext?
    ) {
        implementNotNullAdd(info as WinAggContext?, add as WinAggAddContext?)
    }

    @Override
    protected override fun nonDefaultOnEmptySet(info: AggContext?): Boolean {
        return nonDefaultOnEmptySet(info as WinAggContext?)
    }

    @Override
    override fun getNotNullState(info: AggContext?): List<Type> {
        return getNotNullState(info as WinAggContext?)
    }

    @Override
    protected override fun implementNotNullReset(
        info: AggContext?,
        reset: AggResetContext?
    ) {
        implementNotNullReset(info as WinAggContext?, reset as WinAggResetContext?)
    }

    @Override
    protected override fun implementNotNullResult(
        info: AggContext?,
        result: AggResultContext?
    ): Expression {
        return implementNotNullResult(
            info as WinAggContext?,
            result as WinAggResultContext?
        )
    }

    @Override
    override fun needCacheWhenFrameIntact(): Boolean {
        return true
    }
}
