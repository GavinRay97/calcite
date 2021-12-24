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

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelCollationTraitDef
import org.apache.calcite.rel.RelDistribution
import org.apache.calcite.rel.RelDistributionTraitDef
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import java.util.AbstractList
import java.util.Arrays
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.function.Supplier

/**
 * RelTraitSet represents an ordered set of [RelTrait]s.
 */
class RelTraitSet private constructor(//~ Instance fields --------------------------------------------------------
    private val cache: Cache, traits: Array<RelTrait?>
) : AbstractList<RelTrait?>() {
    private val traits: Array<RelTrait?>

    @Nullable
    private var string: String? = null

    /** Caches the hash code for the traits.  */
    private var hash // Default to 0
            = 0
    //~ Constructors -----------------------------------------------------------
    /**
     * Constructs a RelTraitSet with the given set of RelTraits.
     *
     * @param cache  Trait set cache (and indirectly cluster) that this set
     * belongs to
     * @param traits Traits
     */
    init {
        // NOTE: We do not copy the array. It is important that the array is not
        //   shared. However, since this constructor is private, we assume that
        //   the caller has made a copy.
        this.traits = traits
    }

    /**
     * Retrieves a RelTrait from the set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @return the RelTrait
     * @throws ArrayIndexOutOfBoundsException if index greater than or equal to
     * [.size] or less than 0.
     */
    fun getTrait(index: Int): RelTrait? {
        return traits[index]
    }

    /**
     * Retrieves a list of traits from the set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @return the RelTrait
     * @throws ArrayIndexOutOfBoundsException if index greater than or equal to
     * [.size] or less than 0.
     */
    fun <E : RelMultipleTrait?> getTraits(index: Int): List<E> {
        val trait: RelTrait? = traits[index]
        return if (trait is RelCompositeTrait) {
            (trait as RelCompositeTrait<E>?).traitList()
        } else {
            ImmutableList.of(trait as E?)
        }
    }

    @Override
    operator fun get(index: Int): RelTrait? {
        return getTrait(index)
    }

    /**
     * Returns whether a given kind of trait is enabled.
     */
    fun <T : RelTrait?> isEnabled(traitDef: RelTraitDef<T>): Boolean {
        return getTrait(traitDef) != null
    }

    /**
     * Retrieves a RelTrait of the given type from the set.
     *
     * @param traitDef the type of RelTrait to retrieve
     * @return the RelTrait, or null if not found
     */
    fun <T : RelTrait?> getTrait(traitDef: RelTraitDef<T>): @Nullable T? {
        val index = findIndex(traitDef)
        return if (index >= 0) {
            getTrait(index) as T?
        } else null
    }

    /**
     * Retrieves a list of traits of the given type from the set.
     *
     *
     * Only valid for traits that support multiple entries. (E.g. collation.)
     *
     * @param traitDef the type of RelTrait to retrieve
     * @return the RelTrait, or null if not found
     */
    fun <T : RelMultipleTrait?> getTraits(
        traitDef: RelTraitDef<T>
    ): @Nullable List<T>? {
        val index = findIndex(traitDef)
        return if (index >= 0) {
            getTraits<RelMultipleTrait>(index) as List<T>?
        } else null
    }

    /**
     * Replaces an existing RelTrait in the set.
     * Returns a different trait set; does not modify this trait set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @param trait the new RelTrait
     * @return the old RelTrait at the index
     */
    fun replace(index: Int, trait: RelTrait?): RelTraitSet {
        assert(traits[index].getTraitDef() === trait.getTraitDef()) { "RelTrait has different RelTraitDef than replacement" }
        val canonizedTrait: RelTrait = canonize<RelTrait?>(trait)
        if (traits[index] === canonizedTrait) {
            return this
        }
        val newTraits: Array<RelTrait?> = traits.clone()
        newTraits[index] = canonizedTrait
        return cache.getOrAdd(RelTraitSet(cache, newTraits))
    }

    /**
     * Returns a trait set consisting of the current set plus a new trait.
     *
     *
     * If the set does not contain a trait of the same [RelTraitDef],
     * the trait is ignored, and this trait set is returned.
     *
     * @param trait the new trait
     * @return New set
     * @see .plus
     */
    fun replace(
        trait: RelTrait
    ): RelTraitSet {
        // Quick check for common case
        if (containsShallow(traits, trait)) {
            return this
        }
        val traitDef: RelTraitDef = trait.getTraitDef()
        val index = findIndex(traitDef)
        return if (index < 0) {
            // Trait is not present. Ignore it.
            this
        } else replace(index, trait)
    }

    /** Replaces the trait(s) of a given type with a list of traits of the same
     * type.
     *
     *
     * The list must not be empty, and all traits must be of the same type.
     */
    fun <T : RelMultipleTrait?> replace(traits: List<T>): RelTraitSet {
        assert(!traits.isEmpty())
        val def: RelTraitDef = traits[0].getTraitDef()
        return replace(RelCompositeTrait.of(def, traits))
    }

    /** Replaces the trait(s) of a given type with a list of traits of the same
     * type.
     *
     *
     * The list must not be empty, and all traits must be of the same type.
     */
    fun <T : RelMultipleTrait?> replace(
        def: RelTraitDef<T>?,
        traits: List<T>?
    ): RelTraitSet {
        return replace(RelCompositeTrait.of(def, traits))
    }

    /** If a given multiple trait is enabled, replaces it by calling the given
     * function.  */
    fun <T : RelMultipleTrait?> replaceIfs(
        def: RelTraitDef<T>,
        traitSupplier: Supplier<out List<T>?>
    ): RelTraitSet {
        val index = findIndex(def)
        if (index < 0) {
            return this // trait is not enabled; ignore it
        }
        val traitList: List<T> = traitSupplier.get() ?: return replace(index, def.getDefault())
        return replace(index, RelCompositeTrait.of(def, traitList))
    }

    /** If a given trait is enabled, replaces it by calling the given function.  */
    fun <T : RelTrait?> replaceIf(
        def: RelTraitDef<T>,
        traitSupplier: Supplier<out T>
    ): RelTraitSet {
        val index = findIndex(def)
        if (index < 0) {
            return this // trait is not enabled; ignore it
        }
        var traitList: T = traitSupplier.get()
        if (traitList == null) {
            traitList = def.getDefault()
        }
        return replace(index, traitList)
    }

    /**
     * Applies a mapping to this traitSet.
     *
     * @param mapping   Mapping
     * @return traitSet with mapping applied
     */
    fun apply(mapping: Mappings.TargetMapping?): RelTraitSet {
        val newTraits: Array<RelTrait?> = arrayOfNulls<RelTrait>(traits.size)
        for (i in traits.indices) {
            newTraits[i] = traits[i]!!.apply(mapping)
        }
        return cache.getOrAdd(RelTraitSet(cache, newTraits))
    }

    /**
     * Returns whether all the traits are default trait value.
     */
    val isDefault: Boolean
        get() {
            for (trait in traits) {
                if (trait !== trait.getTraitDef().getDefault()) {
                    return false
                }
            }
            return true
        }

    /**
     * Returns whether all the traits except [Convention]
     * are default trait value.
     */
    val isDefaultSansConvention: Boolean
        get() {
            for (trait in traits) {
                if (trait.getTraitDef() === ConventionTraitDef.INSTANCE) {
                    continue
                }
                if (trait !== trait.getTraitDef().getDefault()) {
                    return false
                }
            }
            return true
        }

    /**
     * Returns whether all the traits except [Convention]
     * equals with traits in `other` traitSet.
     */
    fun equalsSansConvention(other: RelTraitSet): Boolean {
        if (this == other) {
            return true
        }
        if (size() != other.size()) {
            return false
        }
        for (i in traits.indices) {
            if (traits[i].getTraitDef() === ConventionTraitDef.INSTANCE) {
                continue
            }
            // each trait should be canonized already
            if (traits[i] !== other.traits[i]) {
                return false
            }
        }
        return true
    }

    /**
     * Returns a new traitSet with same traitDefs with
     * current traitSet, but each trait is the default
     * trait value.
     */
    fun getDefault(): RelTraitSet {
        val newTraits: Array<RelTrait?> = arrayOfNulls<RelTrait>(traits.size)
        for (i in traits.indices) {
            newTraits[i] = traits[i].getTraitDef().getDefault()
        }
        return cache.getOrAdd(RelTraitSet(cache, newTraits))
    }

    /**
     * Returns a new traitSet with same traitDefs with
     * current traitSet, but each trait except [Convention]
     * is the default trait value. [Convention] trait
     * remains the same with current traitSet.
     */
    fun getDefaultSansConvention(): RelTraitSet {
        val newTraits: Array<RelTrait?> = arrayOfNulls<RelTrait>(traits.size)
        for (i in traits.indices) {
            if (traits[i].getTraitDef() === ConventionTraitDef.INSTANCE) {
                newTraits[i] = traits[i]
            } else {
                newTraits[i] = traits[i].getTraitDef().getDefault()
            }
        }
        return cache.getOrAdd(RelTraitSet(cache, newTraits))
    }

    /**
     * Returns [Convention] trait defined by
     * [ConventionTraitDef.INSTANCE], or null if the
     * [ConventionTraitDef.INSTANCE] is not registered
     * in this traitSet.
     */
    @get:Nullable
    val convention: Convention?
        get() = getTrait(ConventionTraitDef.INSTANCE)

    /**
     * Returns [RelDistribution] trait defined by
     * [RelDistributionTraitDef.INSTANCE], or null if the
     * [RelDistributionTraitDef.INSTANCE] is not registered
     * in this traitSet.
     */
    @SuppressWarnings("unchecked")
    fun <T : RelDistribution?> getDistribution(): @Nullable T? {
        return getTrait(RelDistributionTraitDef.INSTANCE) as T
    }

    /**
     * Returns [RelCollation] trait defined by
     * [RelCollationTraitDef.INSTANCE], or null if the
     * [RelCollationTraitDef.INSTANCE] is not registered
     * in this traitSet.
     */
    @SuppressWarnings("unchecked")
    fun <T : RelCollation?> getCollation(): @Nullable T? {
        return getTrait(RelCollationTraitDef.INSTANCE) as T
    }

    /**
     * Returns the size of the RelTraitSet.
     *
     * @return the size of the RelTraitSet.
     */
    @Override
    fun size(): Int {
        return traits.size
    }

    /**
     * Converts a trait to canonical form.
     *
     *
     * After canonization, t1.equals(t2) if and only if t1 == t2.
     *
     * @param trait Trait
     * @return Trait in canonical form
     */
    fun <T : RelTrait?> canonize(trait: T?): T? {
        if (trait == null) {
            // Return "trait" makes the input type to be the same as the output type,
            // so checkerframework is happy
            return trait
        }
        return // Composite traits are canonized on creation
        trait as? RelCompositeTrait ?: trait.getTraitDef().canonize(trait) as T
    }

    /**
     * Compares two RelTraitSet objects for equality.
     *
     * @param obj another RelTraitSet
     * @return true if traits are equal and in the same order, false otherwise
     */
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (this == obj) {
            return true
        }
        if (obj !is RelTraitSet) {
            return false
        }
        val that = obj as RelTraitSet
        if (hash != 0 && that.hash != 0 && hash != that.hash) {
            return false
        }
        if (traits.size != that.traits.size) {
            return false
        }
        for (i in traits.indices) {
            if (traits[i] !== that.traits[i]) {
                return false
            }
        }
        return true
    }

    @Override
    override fun hashCode(): Int {
        if (hash == 0) {
            hash = Arrays.hashCode(traits)
        }
        return hash
    }

    /**
     * Returns whether this trait set satisfies another trait set.
     *
     *
     * For that to happen, each trait satisfies the corresponding trait in the
     * other set. In particular, each trait set satisfies itself, because each
     * trait subsumes itself.
     *
     *
     * Intuitively, if a relational expression is needed that has trait set
     * S (A, B), and trait set S1 (A1, B1) subsumes S, then any relational
     * expression R in S1 meets that need.
     *
     *
     * For example, if we need a relational expression that has
     * trait set S = {enumerable convention, sorted on [C1 asc]}, and R
     * has {enumerable convention, sorted on [C3], [C1, C2]}. R has two
     * sort keys, but one them [C1, C2] satisfies S [C1], and that is enough.
     *
     * @param that another RelTraitSet
     * @return whether this trait set satisfies other trait set
     *
     * @see org.apache.calcite.plan.RelTrait.satisfies
     */
    fun satisfies(that: RelTraitSet): Boolean {
        if (this == that) {
            return true
        }
        val n: Int = Math.min(
            size(),
            that.size()
        )
        for (i in 0 until n) {
            val thisTrait: RelTrait? = traits[i]
            val thatTrait: RelTrait? = that.traits[i]
            if (!thisTrait!!.satisfies(thatTrait)) {
                return false
            }
        }
        return true
    }

    /**
     * Compares two RelTraitSet objects to see if they match for the purposes of
     * firing a rule. A null RelTrait within a RelTraitSet indicates a wildcard:
     * any RelTrait in the other RelTraitSet will match. If one RelTraitSet is
     * smaller than the other, comparison stops when the last RelTrait from the
     * smaller set has been examined and the remaining RelTraits in the larger
     * set are assumed to match.
     *
     * @param that another RelTraitSet
     * @return true if the RelTraitSets match, false otherwise
     */
    fun matches(that: RelTraitSet): Boolean {
        val n: Int = Math.min(
            size(),
            that.size()
        )
        for (i in 0 until n) {
            val thisTrait: RelTrait? = traits[i]
            val thatTrait: RelTrait? = that.traits[i]
            if (thisTrait == null || thatTrait == null) {
                continue
            }
            if (thisTrait !== thatTrait) {
                return false
            }
        }
        return true
    }

    /**
     * Returns whether this trait set contains a given trait.
     *
     * @param trait Sought trait
     * @return Whether set contains given trait
     */
    operator fun contains(trait: RelTrait?): Boolean {
        for (relTrait in traits) {
            if (trait === relTrait) {
                return true
            }
        }
        return false
    }

    /**
     * Returns whether this trait set contains the given trait, or whether the
     * trait is not present because its [RelTraitDef] is not enabled.
     * Returns false if another trait of the same `RelTraitDef` is
     * present.
     *
     * @param trait Trait
     * @return Whether trait is present, or is absent because disabled
     */
    fun containsIfApplicable(trait: RelTrait): Boolean {
        // Note that '==' is sufficient, because trait should be canonized.
        val trait1: RelTrait? = getTrait(trait.getTraitDef())
        return trait1 == null || trait1 === trait
    }

    /**
     * Returns whether this trait set comprises precisely the list of given
     * traits.
     *
     * @param relTraits Traits
     * @return Whether this trait set's traits are the same as the argument
     */
    fun comprises(vararg relTraits: RelTrait?): Boolean {
        return Arrays.equals(traits, relTraits)
    }

    @Override
    override fun toString(): String {
        if (string == null) {
            string = computeString()
        }
        return string!!
    }

    /**
     * Outputs the traits of this set as a String. Traits are output in order,
     * separated by periods.
     */
    fun computeString(): String {
        val s = StringBuilder()
        for (i in traits.indices) {
            val trait: RelTrait? = traits[i]
            if (i > 0) {
                s.append('.')
            }
            if (trait == null
                && traits.size == 1
            ) {
                // Special format for a list containing a single null trait;
                // otherwise its string appears as "null", which is the same
                // as if the whole trait set were null, and so confusing.
                s.append("{null}")
            } else {
                s.append(trait)
            }
        }
        return s.toString()
    }

    /**
     * Finds the index of a trait of a given type in this set.
     *
     * @param traitDef Sought trait definition
     * @return index of trait, or -1 if not found
     */
    private fun findIndex(traitDef: RelTraitDef): Int {
        for (i in traits.indices) {
            val trait: RelTrait? = traits[i]
            if (trait != null && trait.getTraitDef() === traitDef) {
                return i
            }
        }
        return -1
    }

    /**
     * Returns this trait set with a given trait added or overridden. Does not
     * modify this trait set.
     *
     * @param trait Trait
     * @return Trait set with given trait
     */
    operator fun plus(trait: RelTrait?): RelTraitSet {
        if (contains(trait)) {
            return this
        }
        val i = findIndex(trait.getTraitDef())
        if (i >= 0) {
            return replace(i, trait)
        }
        val canonizedTrait: RelTrait = canonize<RelTrait?>(trait)
        assert(canonizedTrait != null)
        val newTraits: Array<RelTrait?> = arrayOfNulls<RelTrait>(traits.size + 1)
        System.arraycopy(traits, 0, newTraits, 0, traits.size)
        newTraits[traits.size] = canonizedTrait
        return cache.getOrAdd(RelTraitSet(cache, newTraits))
    }

    fun plusAll(traits: Array<RelTrait?>): RelTraitSet {
        var t = this
        for (trait in traits) {
            t = t.plus(trait)
        }
        return t
    }

    fun merge(additionalTraits: RelTraitSet): RelTraitSet {
        return plusAll(additionalTraits.traits)
    }

    /** Returns a list of traits that are in `traitSet` but not in this
     * RelTraitSet.  */
    fun difference(traitSet: RelTraitSet): ImmutableList<RelTrait> {
        val builder: ImmutableList.Builder<RelTrait> = ImmutableList.builder()
        val n: Int = Math.min(
            size(),
            traitSet.size()
        )
        for (i in 0 until n) {
            val thisTrait: RelTrait? = traits[i]
            val thatTrait: RelTrait? = traitSet.traits[i]
            if (thisTrait !== thatTrait) {
                builder.add(thatTrait)
            }
        }
        return builder.build()
    }

    /** Returns whether there are any composite traits in this set.  */
    fun allSimple(): Boolean {
        for (trait in traits) {
            if (trait is RelCompositeTrait) {
                return false
            }
        }
        return true
    }

    /** Returns a trait set similar to this one but with all composite traits
     * flattened.  */
    fun simplify(): RelTraitSet {
        var x = this
        for (i in traits.indices) {
            val trait: RelTrait? = traits[i]
            if (trait is RelCompositeTrait) {
                x = x.replace(
                    i,
                    if ((trait as RelCompositeTrait?).size() === 1) (trait as RelCompositeTrait?).trait(0) else trait.getTraitDef()
                        .getDefault()
                )
            }
        }
        return x
    }

    /** Cache of trait sets.  */
    private class Cache internal constructor() {
        val map: Map<RelTraitSet, RelTraitSet> = HashMap()
        fun getOrAdd(t: RelTraitSet): RelTraitSet {
            val exist: RelTraitSet = map.putIfAbsent(t, t)
            return exist ?: t
        }
    }

    companion object {
        private val EMPTY_TRAITS: Array<RelTrait?> = arrayOfNulls<RelTrait>(0)
        //~ Methods ----------------------------------------------------------------
        /**
         * Creates an empty trait set.
         *
         *
         * It has a new cache, which will be shared by any trait set created from
         * it. Thus each empty trait set is the start of a new ancestral line.
         */
        fun createEmpty(): RelTraitSet {
            return RelTraitSet(Cache(), EMPTY_TRAITS)
        }

        /** Returns whether an element occurs within an array.
         *
         *
         * Uses `==`, not [.equals]. Nulls are allowed.  */
        private fun <T> containsShallow(ts: Array<T?>, seek: RelTrait): Boolean {
            for (t in ts) {
                if (t === seek) {
                    return true
                }
            }
            return false
        }
    }
}
