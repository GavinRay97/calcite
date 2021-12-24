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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import com.google.common.collect.Interner
import com.google.common.collect.Interners

/**
 * RelTraitDef represents a class of [RelTrait]s. Implementations of
 * RelTraitDef may be singletons under the following conditions:
 *
 *
 *  1. if the set of all possible associated RelTraits is finite and fixed (e.g.
 * all RelTraits for this RelTraitDef are known at compile time). For example,
 * the CallingConvention trait meets this requirement, because CallingConvention
 * is effectively an enumeration.
 *  1. Either
 *
 *
 *  *  [.canConvert] and
 * [.convert] do not require
 * planner-instance-specific information, **or**
 *
 *  * the RelTraitDef manages separate sets of conversion data internally. See
 * [ConventionTraitDef] for an example of this.
 *
 *
 *
 *
 *
 * Otherwise, a new instance of RelTraitDef must be constructed and
 * registered with each new planner instantiated.
 *
 * @param <T> Trait that this trait definition is based upon
</T> */
abstract class RelTraitDef<T : RelTrait?>  //~ Constructors -----------------------------------------------------------
protected constructor() {
    //~ Instance fields --------------------------------------------------------
    /**
     * Cache of traits.
     *
     *
     * Uses weak interner to allow GC.
     */
    @SuppressWarnings("BetaApi")
    private val interner: Interner<T> = Interners.newWeakInterner()
    //~ Methods ----------------------------------------------------------------
    /**
     * Whether a relational expression may possess more than one instance of
     * this trait simultaneously.
     *
     *
     * A subset has only one instance of a trait.
     */
    fun multiple(): Boolean {
        return false
    }

    /** Returns the specific RelTrait type associated with this RelTraitDef.  */
    abstract val traitClass: Class<T>?

    /** Returns a simple name for this RelTraitDef (for use in
     * [org.apache.calcite.rel.RelNode.explain]).  */
    abstract val simpleName: String?

    /**
     * Takes an arbitrary RelTrait and returns the canonical representation of
     * that RelTrait. Canonized RelTrait objects may always be compared using
     * the equality operator (`==`).
     *
     *
     * If an equal RelTrait has already been canonized and is still in use,
     * it will be returned. Otherwise, the given RelTrait is made canonical and
     * returned.
     *
     * @param trait a possibly non-canonical RelTrait
     * @return a canonical RelTrait.
     */
    @SuppressWarnings("BetaApi")
    fun canonize(trait: T): T {
        if (trait !is RelCompositeTrait) {
            assert(traitClass.isInstance(trait)) {
                (getClass().getName()
                        + " cannot canonize a "
                        + trait.getClass().getName())
            }
        }
        return interner.intern(trait)
    }

    /**
     * Converts the given RelNode to the given RelTrait.
     *
     * @param planner                     the planner requesting the conversion
     * @param rel                         RelNode to convert
     * @param toTrait                     RelTrait to convert to
     * @param allowInfiniteCostConverters flag indicating whether infinite cost
     * converters are allowed
     * @return a converted RelNode or null if conversion is not possible
     */
    @Nullable
    abstract fun convert(
        planner: RelOptPlanner?,
        rel: RelNode?,
        toTrait: T,
        allowInfiniteCostConverters: Boolean
    ): RelNode?

    /**
     * Tests whether the given RelTrait can be converted to another RelTrait.
     *
     * @param planner   the planner requesting the conversion test
     * @param fromTrait the RelTrait to convert from
     * @param toTrait   the RelTrait to convert to
     * @return true if fromTrait can be converted to toTrait
     */
    abstract fun canConvert(
        planner: RelOptPlanner?,
        fromTrait: T,
        toTrait: T
    ): Boolean

    /**
     * Provides notification of the registration of a particular
     * [ConverterRule] with a [RelOptPlanner]. The default
     * implementation does nothing.
     *
     * @param planner       the planner registering the rule
     * @param converterRule the registered converter rule
     */
    fun registerConverterRule(
        planner: RelOptPlanner?,
        converterRule: ConverterRule?
    ) {
    }

    /**
     * Provides notification that a particular [ConverterRule] has been
     * de-registered from a [RelOptPlanner]. The default implementation
     * does nothing.
     *
     * @param planner       the planner registering the rule
     * @param converterRule the registered converter rule
     */
    fun deregisterConverterRule(
        planner: RelOptPlanner?,
        converterRule: ConverterRule?
    ) {
    }

    /**
     * Returns the default member of this trait.
     */
    abstract val default: T
}
