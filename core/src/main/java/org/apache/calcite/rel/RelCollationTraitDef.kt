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
package org.apache.calcite.rel

import org.apache.calcite.plan.RelOptPlanner

/**
 * Definition of the ordering trait.
 *
 *
 * Ordering is a physical property (i.e. a trait) because it can be changed
 * without loss of information. The converter to do this is the
 * [org.apache.calcite.rel.core.Sort] operator.
 *
 *
 * Unlike other current traits, a [RelNode] can have more than one
 * value of this trait simultaneously. For example,
 * `LogicalTableScan(table=TIME_BY_DAY)` might be sorted by
 * `{the_year, the_month, the_date}` and also by
 * `{time_id}`. We have to allow a RelNode to belong to more than
 * one RelSubset (these RelSubsets are always in the same set).
 */
class RelCollationTraitDef private constructor() : RelTraitDef<RelCollation?>() {
    val traitClass: Class<RelCollation>
        @Override get() = RelCollation::class.java
    val simpleName: String
        @Override get() = "sort"

    @Override
    fun multiple(): Boolean {
        return true
    }

    val default: org.apache.calcite.rel.RelCollation
        @Override get() = RelCollations.EMPTY

    @Override
    @Nullable
    fun convert(
        planner: RelOptPlanner,
        rel: RelNode,
        toCollation: RelCollation,
        allowInfiniteCostConverters: Boolean
    ): RelNode? {
        if (toCollation.getFieldCollations().isEmpty()) {
            // An empty sort doesn't make sense.
            return null
        }

        // Create a logical sort, then ask the planner to convert its remaining
        // traits (e.g. convert it to an EnumerableSortRel if rel is enumerable
        // convention)
        val sort: Sort = LogicalSort.create(rel, toCollation, null, null)
        var newRel: RelNode = planner.register(sort, rel)
        val newTraitSet: RelTraitSet = rel.getTraitSet().replace(toCollation)
        if (!newRel.getTraitSet().equals(newTraitSet)) {
            newRel = planner.changeTraits(newRel, newTraitSet)
        }
        return newRel
    }

    @Override
    fun canConvert(
        planner: RelOptPlanner?, fromTrait: RelCollation?, toTrait: RelCollation?
    ): Boolean {
        return true
    }

    companion object {
        val INSTANCE = RelCollationTraitDef()
    }
}
