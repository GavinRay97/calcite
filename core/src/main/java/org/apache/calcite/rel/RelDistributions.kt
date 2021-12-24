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

import org.apache.calcite.plan.RelMultipleTrait

/**
 * Utilities concerning [org.apache.calcite.rel.RelDistribution].
 */
object RelDistributions {
    val EMPTY: ImmutableIntList = ImmutableIntList.of()

    /** The singleton singleton distribution.  */
    val SINGLETON: RelDistribution = RelDistributionImpl(RelDistribution.Type.SINGLETON, EMPTY)

    /** The singleton random distribution.  */
    val RANDOM_DISTRIBUTED: RelDistribution = RelDistributionImpl(RelDistribution.Type.RANDOM_DISTRIBUTED, EMPTY)

    /** The singleton round-robin distribution.  */
    val ROUND_ROBIN_DISTRIBUTED: RelDistribution = RelDistributionImpl(
        RelDistribution.Type.ROUND_ROBIN_DISTRIBUTED,
        EMPTY
    )

    /** The singleton broadcast distribution.  */
    val BROADCAST_DISTRIBUTED: RelDistribution = RelDistributionImpl(
        RelDistribution.Type.BROADCAST_DISTRIBUTED,
        EMPTY
    )
    val ANY: RelDistribution = RelDistributionImpl(RelDistribution.Type.ANY, EMPTY)

    /** Creates a hash distribution.  */
    fun hash(numbers: Collection<Number?>): RelDistribution {
        val list: ImmutableIntList = normalizeKeys(numbers)
        return of(RelDistribution.Type.HASH_DISTRIBUTED, list)
    }

    /** Creates a range distribution.  */
    fun range(numbers: Collection<Number?>?): RelDistribution {
        val list: ImmutableIntList = ImmutableIntList.copyOf(numbers)
        return of(RelDistribution.Type.RANGE_DISTRIBUTED, list)
    }

    fun of(type: RelDistribution.Type, keys: ImmutableIntList): RelDistribution {
        val distribution: RelDistribution = RelDistributionImpl(type, keys)
        return RelDistributionTraitDef.INSTANCE.canonize(distribution)
    }

    /** Creates ordered immutable copy of keys collection.   */
    private fun normalizeKeys(keys: Collection<Number?>): ImmutableIntList {
        var list: ImmutableIntList = ImmutableIntList.copyOf(keys)
        if (list.size() > 1
            && !Ordering.natural().isOrdered(list)
        ) {
            list = ImmutableIntList.copyOf(Ordering.natural().sortedCopy(list))
        }
        return list
    }

    /** Implementation of [org.apache.calcite.rel.RelDistribution].  */
    private class RelDistributionImpl(type: Type, keys: ImmutableIntList) : RelDistribution {
        private override val type: Type
        private override val keys: ImmutableIntList

        init {
            this.type = Objects.requireNonNull(type, "type")
            this.keys = ImmutableIntList.copyOf(keys)
            assert(
                type !== Type.HASH_DISTRIBUTED || keys.size() < 2 || Ordering.natural().isOrdered(keys)
            ) { "key columns of hash distribution must be in order" }
            assert(type === Type.HASH_DISTRIBUTED || type === Type.RANDOM_DISTRIBUTED || keys.isEmpty())
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(type, keys)
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (this === obj
                    || (obj is RelDistributionImpl
                    && type === (obj as RelDistributionImpl).type && keys.equals((obj as RelDistributionImpl).keys)))
        }

        @Override
        override fun toString(): String {
            return if (keys.isEmpty()) {
                type.shortName
            } else {
                type.shortName + keys
            }
        }

        @Override
        fun getType(): Type {
            return type
        }

        @Override
        fun getKeys(): List<Integer> {
            return keys
        }

        val traitDef: org.apache.calcite.rel.RelDistributionTraitDef
            @Override get() = RelDistributionTraitDef.INSTANCE

        @Override
        override fun apply(mapping: Mappings.TargetMapping): RelDistribution {
            if (keys.isEmpty()) {
                return this
            }
            for (key in keys) {
                if (mapping.getTargetOpt(key) === -1) {
                    return ANY // Some distribution keys are not mapped => any.
                }
            }
            val mappedKeys0: List<Integer> = Mappings.apply2(mapping as Mapping, keys)
            val mappedKeys: ImmutableIntList = normalizeKeys(mappedKeys0)
            return of(type, mappedKeys)
        }

        @Override
        fun satisfies(trait: RelTrait): Boolean {
            if (trait === this || trait === ANY) {
                return true
            }
            if (trait is RelDistributionImpl) {
                val distribution = trait as RelDistributionImpl
                if (type === distribution.type) {
                    return when (type) {
                        HASH_DISTRIBUTED ->             // The "leading edge" property of Range does not apply to Hash.
                            // Only Hash[x, y] satisfies Hash[x, y].
                            keys.equals(distribution.keys)
                        RANGE_DISTRIBUTED ->             // Range[x, y] satisfies Range[x, y, z] but not Range[x]
                            Util.startsWith(distribution.keys, keys)
                        else -> true
                    }
                }
            }
            return if (trait === RANDOM_DISTRIBUTED) {
                // RANDOM is satisfied by HASH, ROUND-ROBIN, RANDOM, RANGE;
                // we've already checked RANDOM
                type === Type.HASH_DISTRIBUTED || type === Type.ROUND_ROBIN_DISTRIBUTED || type === Type.RANGE_DISTRIBUTED
            } else false
        }

        @Override
        fun register(planner: RelOptPlanner?) {
        }

        val isTop: Boolean
            @Override get() = type === Type.ANY

        @Override
        operator fun compareTo(o: RelMultipleTrait): Int {
            val distribution: RelDistribution = o
            return if (type === distribution.getType()
                && (type === Type.HASH_DISTRIBUTED
                        || type === Type.RANGE_DISTRIBUTED)
            ) {
                ORDERING.compare(
                    getKeys(),
                    distribution.getKeys()
                )
            } else type.compareTo(distribution.getType())
        }

        companion object {
            private val ORDERING: Ordering<Iterable<Integer>> =
                Ordering.< Integer > natural < Integer ? > ().lexicographical()
        }
    }
}
