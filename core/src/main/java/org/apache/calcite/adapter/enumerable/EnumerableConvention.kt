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

import org.apache.calcite.plan.Contexts

/**
 * Family of calling conventions that return results as an
 * [org.apache.calcite.linq4j.Enumerable].
 */
enum class EnumerableConvention : Convention {
    INSTANCE;

    @Override
    override fun toString(): String {
        return name
    }

    @get:Override
    val `interface`: Class
        get() = EnumerableRel::class.java

    @get:Override
    override val name: String
        get() = "ENUMERABLE"

    @Override
    @Nullable
    fun enforce(
        input: RelNode,
        required: RelTraitSet
    ): RelNode {
        var rel: RelNode = input
        if (input.getConvention() !== INSTANCE) {
            rel = ConventionTraitDef.INSTANCE.convert(
                input.getCluster().getPlanner(),
                input, INSTANCE, true
            )
            requireNonNull(
                rel
            ) { "Unable to convert input to " + INSTANCE + ", input = " + input }
        }
        val collation: RelCollation = required.getCollation()
        if (collation != null && collation !== RelCollations.EMPTY) {
            rel = EnumerableSort.create(rel, collation, null, null)
        }
        return rel
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
        return true
    }

    @get:Override
    val relFactories: Struct
        get() = RelFactories.Struct.fromContext(
            Contexts.of(
                EnumerableRelFactories.ENUMERABLE_TABLE_SCAN_FACTORY,
                EnumerableRelFactories.ENUMERABLE_PROJECT_FACTORY,
                EnumerableRelFactories.ENUMERABLE_FILTER_FACTORY,
                EnumerableRelFactories.ENUMERABLE_SORT_FACTORY
            )
        )

    companion object {
        /** Cost of an enumerable node versus implementing an equivalent node in a
         * "typical" calling convention.  */
        const val COST_MULTIPLIER = 1.0
    }
}
