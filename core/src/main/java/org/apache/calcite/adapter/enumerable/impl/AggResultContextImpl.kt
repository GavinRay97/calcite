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
package org.apache.calcite.adapter.enumerable.impl

import org.apache.calcite.adapter.enumerable.AggResultContext
import org.apache.calcite.adapter.enumerable.PhysType
import org.apache.calcite.linq4j.tree.BlockBuilder
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.tree.ParameterExpression
import org.apache.calcite.rel.core.AggregateCall
import java.util.List
import java.util.Objects.requireNonNull

/**
 * Implementation of
 * [org.apache.calcite.adapter.enumerable.AggResultContext].
 */
class AggResultContextImpl(
    block: BlockBuilder?, @Nullable call: AggregateCall,
    accumulator: List<Expression>, @Nullable key: ParameterExpression,
    @Nullable keyPhysType: PhysType
) : AggResetContextImpl(block, accumulator), AggResultContext {
    @Nullable
    private val call: AggregateCall

    @Nullable
    private val key: ParameterExpression

    @Nullable
    private val keyPhysType: PhysType

    /**
     * Creates aggregate result context.
     *
     * @param block Code block that will contain the result calculation statements
     * @param call Aggregate call
     * @param accumulator Accumulator variables that store the intermediate
     * aggregate state
     * @param key Key
     */
    init {
        this.call = call // null for AggAddContextImpl
        this.key = key
        this.keyPhysType = keyPhysType // null for AggAddContextImpl
    }

    @Override
    @Nullable
    fun key(): Expression {
        return key
    }

    @Override
    fun keyField(i: Int): Expression {
        return requireNonNull(keyPhysType, "keyPhysType")
            .fieldReference(requireNonNull(key, "key"), i)
    }

    @Override
    override fun call(): AggregateCall {
        return requireNonNull(call, "call")
    }
}
