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
package org.apache.calcite.plan

import org.apache.calcite.rel.RelDistributions
import org.apache.calcite.rel.core.Project
import org.apache.calcite.util.mapping.Mappings

/**
 * RelTrait represents the manifestation of a relational expression trait within
 * a trait definition. For example, a `CallingConvention.JAVA` is a trait
 * of the [ConventionTraitDef] trait definition.
 *
 * <h2><a id="EqualsHashCodeNote">Note about equals() and hashCode()</a></h2>
 *
 *
 * If all instances of RelTrait for a particular RelTraitDef are defined in
 * an `enum` and no new RelTraits can be introduced at runtime, you need
 * not override [.hashCode] and [.equals]. If, however,
 * new RelTrait instances are generated at runtime (e.g. based on state external
 * to the planner), you must implement [.hashCode] and
 * [.equals] for proper [canonization][RelTraitDef.canonize]
 * of your RelTrait objects.
 */
interface RelTrait {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the RelTraitDef that defines this RelTrait.
     *
     * @return the RelTraitDef that defines this RelTrait
     */
    val traitDef: RelTraitDef

    /**
     * See [note about equals() and hashCode()](#EqualsHashCodeNote).
     */
    @Override
    override fun hashCode(): Int

    /**
     * See [note about equals() and hashCode()](#EqualsHashCodeNote).
     */
    @Override
    override fun equals(@Nullable o: Object?): Boolean

    /**
     * Returns whether this trait satisfies a given trait.
     *
     *
     * A trait satisfies another if it is the same or stricter. For example,
     * `ORDER BY x, y` satisfies `ORDER BY x`.
     *
     *
     * A trait's `satisfies` relation must be a partial order (reflexive,
     * anti-symmetric, transitive). Many traits cannot be "loosened"; their
     * `satisfies` is an equivalence relation, where only X satisfies X.
     *
     *
     * If a trait has multiple values
     * (see [org.apache.calcite.plan.RelCompositeTrait])
     * a collection (T0, T1, ...) satisfies T if any Ti satisfies T.
     *
     * @param trait Given trait
     * @return Whether this trait subsumes a given trait
     */
    fun satisfies(trait: RelTrait?): Boolean

    /**
     * Returns a succinct name for this trait. The planner may use this String
     * to describe the trait.
     */
    @Override
    override fun toString(): String

    /**
     * Registers a trait instance with the planner.
     *
     *
     * This is an opportunity to add rules that relate to that trait. However,
     * typical implementations will do nothing.
     *
     * @param planner Planner
     */
    fun register(planner: RelOptPlanner?)

    /**
     * Applies a mapping to this trait.
     *
     *
     * Some traits may be changed if the columns order is changed by a mapping
     * of the [Project] operator.
     *
     *
     * For example, if relation `SELECT a, b ORDER BY a, b` is sorted by
     * columns [0, 1], then the project `SELECT b, a` over this relation
     * will be sorted by columns [1, 0]. In the same time project `SELECT b`
     * will not be sorted at all because it doesn't contain the collation
     * prefix and this method will return an empty collation.
     *
     *
     * Other traits are independent from the columns remapping. For example
     * [Convention] or [RelDistributions.SINGLETON].
     *
     * @param mapping   Mapping
     * @return trait with mapping applied
     */
    fun <T : RelTrait?> apply(mapping: Mappings.TargetMapping?): T {
        return this as T
    }

    /**
     * Returns whether this trait is the default trait value.
     */
    val isDefault: Boolean
        get() = this === traitDef.getDefault()
}
