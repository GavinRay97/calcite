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

import com.google.common.collect.ImmutableList
import com.google.common.collect.Ordering
import java.util.Arrays
import java.util.List
import java.util.Objects

/**
 * A trait that consists of a list of traits, all of the same type.
 *
 *
 * It exists so that multiple traits of the same type
 * ([org.apache.calcite.plan.RelTraitDef]) can be stored in the same
 * [org.apache.calcite.plan.RelTraitSet].
 *
 * @param <T> Member trait
</T> */
internal class RelCompositeTrait<T : RelMultipleTrait?> private constructor(traitDef: RelTraitDef, traits: Array<T>) :
    RelTrait {
    private val traitDef: RelTraitDef
    private val traits: Array<T>

    /** Creates a RelCompositeTrait.  */ // Must remain private. Does not copy the array.
    init {
        this.traitDef = traitDef
        this.traits = Objects.requireNonNull(traits, "traits")
        assert(
            Ordering.natural()
                .isStrictlyOrdered(Arrays.asList(traits as Array<Comparable>))
        ) { Arrays.toString(traits) }
        for (trait in traits) {
            assert(trait.getTraitDef() === this.traitDef)
        }
    }

    @Override
    fun getTraitDef(): RelTraitDef {
        return traitDef
    }

    @Override
    override fun hashCode(): Int {
        return Arrays.hashCode(traits)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || obj is RelCompositeTrait<*>
                && Arrays.equals(traits, (obj as RelCompositeTrait<*>).traits))
    }

    @Override
    override fun toString(): String {
        return Arrays.toString(traits)
    }

    @Override
    fun satisfies(trait: RelTrait?): Boolean {
        for (t in traits) {
            if (t.satisfies(trait)) {
                return true
            }
        }
        return false
    }

    @Override
    fun register(planner: RelOptPlanner?) {
    }

    /** Returns an immutable list of the traits in this composite trait.  */
    fun traitList(): List<T> {
        return ImmutableList.copyOf(traits)
    }

    /** Returns the `i`th trait.  */
    fun trait(i: Int): T {
        return traits[i]
    }

    /** Returns the number of traits.  */
    fun size(): Int {
        return traits.size
    }

    companion object {
        /** Creates a RelCompositeTrait. The constituent traits are canonized.  */
        @SuppressWarnings("unchecked")
        fun <T : RelMultipleTrait?> of(
            def: RelTraitDef,
            traitList: List<T>
        ): RelTrait {
            val compositeTrait: RelCompositeTrait<T>
            if (traitList.isEmpty()) {
                return def.getDefault()
            } else if (traitList.size() === 1) {
                return def.canonize(traitList[0])
            } else {
                val traits: Array<RelMultipleTrait> = traitList.toArray(arrayOfNulls<RelMultipleTrait>(0))
                for (i in traits.indices) {
                    traits[i] = def.canonize(traits[i]) as T
                }
                compositeTrait = RelCompositeTrait(def, traits as Array<T>)
            }
            return def.canonize(compositeTrait)
        }
    }
}
