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
package org.apache.calcite.util

import com.google.common.collect.ImmutableSortedMap

/** Set of elements organized into equivalence classes.
 *
 *
 * Elements are equivalent by the rules of a mathematical equivalence
 * relation:
 *
 * <dl>
 * <dt>Reflexive
</dt> * <dd>Every element `e` is equivalent to itself
</dd> * <dt>Symmetric
</dt> * <dd>If `e` is equivalent to `f`,
 * then `f` is equivalent to `e`
</dd> * <dt>Transitive
</dt> * <dd>If `e` is equivalent to `f`,
 * and `f` is equivalent to `g`,
 * then `e` is equivalent to `g`
</dd></dl> *
 *
 *
 * For any given pair of elements, answers in O(log N) (two hash-table
 * lookups) whether they are equivalent to each other.
 *
 * @param <E> Element type
</E> */
class EquivalenceSet<E : Comparable<E>?> {
    private val parents: Map<E, E> = HashMap()

    /** Adds an element, and returns the element (which is its own parent).
     * If already present, returns the element's parent.  */
    fun add(e: E): E {
        val parent = parents[Objects.requireNonNull(e, "e")]
        return if (parent == null) {
            // Element is new. Add it to the map, as its own parent.
            parents.put(e, e)
            e
        } else {
            parent
        }
    }

    /** Marks two elements as equivalent.
     * They may or may not be registered, and they may or may not be equal.  */
    fun equiv(e: E, f: E): E {
        val eParent = add(e)
        if (!eParent!!.equals(e)) {
            assert(Objects.equals(parents[eParent], eParent))
            val root = equiv(eParent, f)
            parents.put(e, root)
            return root
        }
        val fParent = add(f)
        if (!fParent!!.equals(f)) {
            assert(Objects.equals(parents[fParent], fParent))
            val root = equiv(e, fParent)
            parents.put(f, root)
            return root
        }
        val c = e!!.compareTo(f)
        if (c == 0) {
            return e
        }
        return if (c < 0) {
            // e is a better (lower) parent of f
            parents.put(f, e)
            e
        } else {
            // f is a better (lower) parent of e
            parents.put(e, f)
            f
        }
    }

    /** Returns whether two elements are in the same equivalence class.
     * Returns false if either or both of the elements are not registered.  */
    fun areEquivalent(e: E, f: E): Boolean {
        val eParent = parents[e]
        val fParent = parents[f]
        return Objects.equals(eParent, fParent)
    }

    /** Returns a map of the canonical element in each equivalence class to the
     * set of elements in that class. The keys are sorted in natural order, as
     * are the elements within each key.  */
    fun map(): NavigableMap<E, SortedSet<E>> {
        val multimap: TreeMultimap<E, E> = TreeMultimap.create()
        for (entry in parents.entrySet()) {
            multimap.put(entry.getValue(), entry.getKey())
        }
        // Create an immutable copy. Keys and values remain in sorted order.
        val builder: ImmutableSortedMap.Builder<E, SortedSet<E>> = ImmutableSortedMap.naturalOrder()
        for (entry in multimap.asMap().entrySet()) {
            builder.put(entry.getKey(), ImmutableSortedSet.copyOf(entry.getValue()))
        }
        return builder.build()
    }

    /** Removes all elements in this equivalence set.  */
    fun clear() {
        parents.clear()
    }

    /** Returns the number of elements in this equivalence set.  */
    fun size(): Int {
        return parents.size()
    }

    /** Returns the number of equivalence classes in this equivalence set.  */
    fun classCount(): Int {
        return HashSet(parents.values()).size()
    }
}
