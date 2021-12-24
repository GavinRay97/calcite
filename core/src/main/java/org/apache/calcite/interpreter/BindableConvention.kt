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
package org.apache.calcite.interpreter

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.RelTraitDef
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode

/**
 * Calling convention that returns results as an
 * [org.apache.calcite.linq4j.Enumerable] of object arrays.
 *
 *
 * The relational expression needs to implement
 * [org.apache.calcite.runtime.ArrayBindable].
 * Unlike [org.apache.calcite.adapter.enumerable.EnumerableConvention],
 * no code generation is required.
 */
enum class BindableConvention : Convention {
    INSTANCE;

    @Override
    override fun toString(): String {
        return name
    }

    @get:Override
    val `interface`: Class
        get() = BindableRel::class.java

    @get:Override
    override val name: String
        get() = "BINDABLE"

    @Override
    @Nullable
    fun enforce(input: RelNode?, required: RelTraitSet?): RelNode? {
        return null
    }

    @get:Override
    val traitDef: RelTraitDef
        get() = ConventionTraitDef.INSTANCE

    @Override
    fun satisfies(trait: RelTrait): Boolean {
        return this == trait
    }

    @Override
    fun register(planner: RelOptPlanner?) {
    }

    @Override
    fun canConvertConvention(toConvention: Convention?): Boolean {
        return false
    }

    @Override
    fun useAbstractConvertersForConversion(
        fromTraits: RelTraitSet?,
        toTraits: RelTraitSet?
    ): Boolean {
        return false
    }

    companion object {
        /** Cost of a bindable node versus implementing an equivalent node in a
         * "typical" calling convention.  */
        const val COST_MULTIPLIER = 2.0
    }
}
