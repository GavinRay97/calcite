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

import org.apache.calcite.rel.type.RelDataType

/**
 * Utilities concerning [org.apache.calcite.rel.RelCollation]
 * and [org.apache.calcite.rel.RelFieldCollation].
 */
object RelCollations {
    /**
     * A collation indicating that a relation is not sorted. Ordering by no
     * columns.
     */
    val EMPTY: RelCollation = RelCollationTraitDef.INSTANCE.canonize(
        RelCollationImpl(ImmutableList.of())
    )

    /**
     * A collation that cannot be replicated by applying a sort. The only
     * implementation choice is to apply operations that preserve order.
     */
    @Deprecated // to be removed before 2.0
    val PRESERVE: RelCollation = RelCollationTraitDef.INSTANCE.canonize(
        object : RelCollationImpl(
            ImmutableList.of(RelFieldCollation(-1))
        ) {
            @Override
            override fun toString(): String {
                return "PRESERVE"
            }
        })

    fun of(vararg fieldCollations: RelFieldCollation?): RelCollation {
        return of(ImmutableList.copyOf(fieldCollations))
    }

    fun of(fieldCollations: List<RelFieldCollation>): RelCollation {
        val collation: RelCollation
        if (Util.isDistinct(ordinals(fieldCollations))) {
            collation = RelCollationImpl(ImmutableList.copyOf(fieldCollations))
        } else {
            // Remove field collations whose field has already been seen
            val builder: ImmutableList.Builder<RelFieldCollation> = ImmutableList.builder()
            val set: Set<Integer> = HashSet()
            for (fieldCollation in fieldCollations) {
                if (set.add(fieldCollation.getFieldIndex())) {
                    builder.add(fieldCollation)
                }
            }
            collation = RelCollationImpl(builder.build())
        }
        return RelCollationTraitDef.INSTANCE.canonize(collation)
    }

    /**
     * Creates a collation containing one field.
     */
    fun of(fieldIndex: Int): RelCollation {
        return of(RelFieldCollation(fieldIndex))
    }

    /**
     * Creates a collation containing multiple fields.
     */
    fun of(keys: ImmutableIntList): RelCollation {
        val cols: List<RelFieldCollation> = keys.stream()
            .map { k -> RelFieldCollation(k) }
            .collect(Collectors.toList())
        return of(cols)
    }

    /**
     * Creates a list containing one collation containing one field.
     */
    fun createSingleton(fieldIndex: Int): List<RelCollation> {
        return ImmutableList.of(of(fieldIndex))
    }

    /**
     * Checks that a collection of collations is valid.
     *
     * @param rowType       Row type of the relational expression
     * @param collationList List of collations
     * @param fail          Whether to fail if invalid
     * @return Whether valid
     */
    fun isValid(
        rowType: RelDataType,
        collationList: List<RelCollation>,
        fail: Boolean
    ): Boolean {
        val fieldCount: Int = rowType.getFieldCount()
        for (collation in collationList) {
            for (fieldCollation in collation.getFieldCollations()) {
                val index: Int = fieldCollation.getFieldIndex()
                if (index < 0 || index >= fieldCount) {
                    assert(!fail)
                    return false
                }
            }
        }
        return true
    }

    fun equal(
        collationList1: List<RelCollation?>,
        collationList2: List<RelCollation?>?
    ): Boolean {
        return collationList1.equals(collationList2)
    }

    /** Returns the indexes of the field collations in a given collation.  */
    fun ordinals(collation: RelCollation): List<Integer> {
        return ordinals(collation.getFieldCollations())
    }

    /** Returns the indexes of the fields in a list of field collations.  */
    fun ordinals(
        fieldCollations: List<RelFieldCollation?>?
    ): List<Integer> {
        return Util.transform(fieldCollations, RelFieldCollation::getFieldIndex)
    }

    /** Returns whether a collation indicates that the collection is sorted on
     * a given list of keys.
     *
     * @param collation Collation
     * @param keys List of keys
     * @return Whether the collection is sorted on the given keys
     */
    fun contains(
        collation: RelCollation,
        keys: Iterable<Integer?>?
    ): Boolean {
        return contains(collation, Util.distinctList(keys))
    }

    private fun contains(
        collation: RelCollation,
        keys: List<Integer>
    ): Boolean {
        val n: Int = collation.getFieldCollations().size()
        val iterator: Iterator<Integer> = keys.iterator()
        for (i in 0 until n) {
            val fieldCollation: RelFieldCollation = collation.getFieldCollations().get(i)
            if (!iterator.hasNext()) {
                return true
            }
            if (fieldCollation.getFieldIndex() !== iterator.next()) {
                return false
            }
        }
        return !iterator.hasNext()
    }

    /** Returns whether one of a list of collations indicates that the collection
     * is sorted on the given list of keys.  */
    fun contains(
        collations: List<RelCollation>,
        keys: ImmutableIntList?
    ): Boolean {
        val distinctKeys: List<Integer> = Util.distinctList(keys)
        for (collation in collations) {
            if (contains(collation, distinctKeys)) {
                return true
            }
        }
        return false
    }

    /** Returns whether a collation contains a given list of keys regardless
     * the order.
     *
     * @param collation Collation
     * @param keys List of keys
     * @return Whether the collection contains the given keys
     */
    fun containsOrderless(
        collation: RelCollation,
        keys: List<Integer?>?
    ): Boolean {
        val distinctKeys: List<Integer> = Util.distinctList(keys)
        val keysBitSet: ImmutableBitSet = ImmutableBitSet.of(distinctKeys)
        val colKeys: List<Integer> = Util.distinctList(collation.getKeys())
        return if (colKeys.size() < distinctKeys.size()) {
            false
        } else {
            val bitset: ImmutableBitSet = ImmutableBitSet.of(
                colKeys.subList(0, distinctKeys.size())
            )
            bitset.equals(keysBitSet)
        }
    }

    /** Returns whether a collation is contained by a given list of keys regardless ordering.
     *
     * @param collation Collation
     * @param keys List of keys
     * @return Whether the collection contains the given keys
     */
    fun containsOrderless(
        keys: List<Integer?>?, collation: RelCollation
    ): Boolean {
        val distinctKeys: List<Integer> = Util.distinctList(keys)
        val colKeys: List<Integer> = Util.distinctList(collation.getKeys())
        return if (colKeys.size() > distinctKeys.size()) {
            false
        } else {
            colKeys.stream().allMatch { i -> distinctKeys.contains(i) }
        }
    }

    /**
     * Returns whether one of a list of collations contains the given list of keys
     * regardless the order.
     */
    fun collationsContainKeysOrderless(
        collations: List<RelCollation>, keys: List<Integer?>?
    ): Boolean {
        for (collation in collations) {
            if (containsOrderless(collation, keys)) {
                return true
            }
        }
        return false
    }

    /**
     * Returns whether one of a list of collations is contained by the given list of keys
     * regardless the order.
     */
    fun keysContainCollationsOrderless(
        keys: List<Integer?>?, collations: List<RelCollation>
    ): Boolean {
        for (collation in collations) {
            if (containsOrderless(keys, collation)) {
                return true
            }
        }
        return false
    }

    fun shift(collation: RelCollation, offset: Int): RelCollation {
        if (offset == 0) {
            return collation // save some effort
        }
        val fieldCollations: ImmutableList.Builder<RelFieldCollation> = ImmutableList.builder()
        for (fc in collation.getFieldCollations()) {
            fieldCollations.add(fc.shift(offset))
        }
        return RelCollationImpl(fieldCollations.build())
    }

    /** Creates a copy of this collation that changes the ordinals of input
     * fields.  */
    fun permute(
        collation: RelCollation,
        mapping: Map<Integer?, Integer?>
    ): RelCollation {
        return of(
            Util.transform(
                collation.getFieldCollations()
            ) { fc ->
                fc.withFieldIndex(
                    requireNonNull(
                        mapping[fc.getFieldIndex()]
                    ) { "no entry for " + fc.getFieldIndex().toString() + " in " + mapping })
            })
    }

    /** Creates a copy of this collation that changes the ordinals of input
     * fields.  */
    fun permute(
        collation: RelCollation,
        mapping: Mappings.TargetMapping
    ): RelCollation {
        return of(
            Util.transform(
                collation.getFieldCollations()
            ) { fc -> fc.withFieldIndex(mapping.getTarget(fc.getFieldIndex())) })
    }
}
