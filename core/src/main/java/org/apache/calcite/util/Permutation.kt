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

import org.apache.calcite.util.mapping.IntPair
import org.apache.calcite.util.mapping.Mapping
import org.apache.calcite.util.mapping.MappingType
import org.apache.calcite.util.mapping.Mappings
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import org.checkerframework.checker.nullness.qual.RequiresNonNull
import java.util.Arrays
import java.util.Iterator

/**
 * Represents a mapping which reorders elements in an array.
 */
class Permutation : Mapping, Mappings.TargetMapping, Cloneable {
    //~ Instance fields --------------------------------------------------------
    private var targets: IntArray
    private var sources: IntArray
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a permutation of a given size.
     *
     *
     * It is initialized to the identity permutation, such as "[0, 1, 2, 3]".
     *
     * @param size Number of elements in the permutation
     */
    @SuppressWarnings("method.invocation.invalid")
    constructor(size: Int) {
        targets = IntArray(size)
        sources = IntArray(size)

        // Initialize to identity.
        identity()
    }

    /**
     * Creates a permutation from an array.
     *
     * @param targets Array of targets
     * @throws IllegalArgumentException       if elements of array are not unique
     * @throws ArrayIndexOutOfBoundsException if elements of array are not
     * between 0 through targets.length - 1
     * inclusive
     */
    constructor(targets: IntArray) {
        this.targets = targets.clone()
        sources = IntArray(targets.size)
        Arrays.fill(sources, -1)
        for (i in targets.indices) {
            val target = targets[i]
            if (target < 0 || target >= sources.size) {
                throw IllegalArgumentException("target out of range")
            }
            if (sources[target] != -1) {
                throw IllegalArgumentException(
                    "more than one permutation element maps to position $target"
                )
            }
            sources[target] = i
        }
        assert(isValid(true))
    }

    /**
     * Creates a permutation. Arrays are not copied, and are assumed to be valid
     * permutations.
     */
    private constructor(targets: IntArray, sources: IntArray) {
        this.targets = targets
        this.sources = sources
        assert(isValid(true))
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun clone(): Object {
        return Permutation(
            targets.clone(),
            sources.clone()
        )
    }

    /**
     * Initializes this permutation to the identity permutation.
     */
    fun identity() {
        for (i in targets.indices) {
            sources[i] = i
            targets[i] = sources[i]
        }
    }

    /**
     * Returns the number of elements in this permutation.
     */
    @Override
    fun size(): Int {
        return targets.size
    }

    @Override
    fun clear() {
        throw UnsupportedOperationException(
            "Cannot clear: permutation must always contain one mapping per element"
        )
    }

    /**
     * Returns a string representation of this permutation.
     *
     *
     * For example, the mapping
     *
     * <table>
     * <caption>Example mapping</caption>
     * <tr>
     * <th>source</th>
     * <th>target</th>
    </tr> *
     * <tr>
     * <td>0</td>
     * <td>2</td>
    </tr> *
     * <tr>
     * <td>1</td>
     * <td>0</td>
    </tr> *
     * <tr>
     * <td>2</td>
     * <td>1</td>
    </tr> *
     * <tr>
     * <td>3</td>
     * <td>3</td>
    </tr> *
    </table> *
     *
     *
     * is represented by the string "[2, 0, 1, 3]".
     */
    @Override
    override fun toString(): String {
        val buf = StringBuilder()
        buf.append("[")
        for (i in targets.indices) {
            if (i > 0) {
                buf.append(", ")
            }
            buf.append(targets[i])
        }
        buf.append("]")
        return buf.toString()
    }

    /**
     * Maps source position to target position.
     *
     *
     * To preserve the 1:1 nature of the permutation, the previous target of
     * source becomes the new target of the previous source.
     *
     *
     * For example, given the permutation
     *
     * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
     *
     *
     * suppose we map position 2 to target 1. Position 2 currently has target
     * 0, and the source of position 1 is position 3. We preserve the permutation
     * property by mapping the previous source 3 to the previous target 0. The new
     * permutation is
     *
     * <blockquote><pre>[3, 2, 1, 0].</pre></blockquote>
     *
     *
     * Another example. Again starting from
     *
     * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
     *
     *
     * suppose we map position 2 to target 3. We map the previous source 0 to
     * the previous target 0, which gives
     *
     * <blockquote><pre>[0, 2, 3, 1].</pre></blockquote>
     *
     * @param source Source position
     * @param target Target position
     * @throws ArrayIndexOutOfBoundsException if source or target is negative or
     * greater than or equal to the size of
     * the permuation
     */
    @Override
    operator fun set(source: Int, target: Int) {
        set(source, target, false)
    }

    /**
     * Maps source position to target position, automatically resizing if source
     * or target is out of bounds.
     *
     *
     * To preserve the 1:1 nature of the permutation, the previous target of
     * source becomes the new target of the previous source.
     *
     *
     * For example, given the permutation
     *
     * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
     *
     *
     * suppose we map position 2 to target 1. Position 2 currently has target
     * 0, and the source of position 1 is position 3. We preserve the permutation
     * property by mapping the previous source 3 to the previous target 0. The new
     * permutation is
     *
     * <blockquote><pre>[3, 2, 1, 0].</pre></blockquote>
     *
     *
     * Another example. Again starting from
     *
     * <blockquote><pre>[3, 2, 0, 1]</pre></blockquote>
     *
     *
     * suppose we map position 2 to target 3. We map the previous source 0 to
     * the previous target 0, which gives
     *
     * <blockquote><pre>[0, 2, 3, 1].</pre></blockquote>
     *
     * @param source      Source position
     * @param target      Target position
     * @param allowResize Whether to resize the permutation if the source or
     * target is greater than the current capacity
     * @throws ArrayIndexOutOfBoundsException if source or target is negative,
     * or greater than or equal to the size
     * of the permutation, and
     * `allowResize` is false
     */
    operator fun set(source: Int, target: Int, allowResize: Boolean) {
        val maxSourceTarget: Int = Math.max(source, target)
        if (maxSourceTarget >= sources.size) {
            if (allowResize) {
                resize(maxSourceTarget + 1)
            } else {
                throw ArrayIndexOutOfBoundsException(maxSourceTarget)
            }
        }
        val prevTarget = targets[source]
        assert(sources[prevTarget] == source)
        val prevSource = sources[target]
        assert(targets[prevSource] == target)
        setInternal(source, target)

        // To balance things up, make the previous source reference the
        // previous target. This ensures that each ordinal occurs precisely
        // once in the sources array and the targets array.
        setInternal(prevSource, prevTarget)

        // For example:
        // Before: [2, 1, 0, 3]
        // Now we set(source=1, target=0)
        //  previous target of source (1) was 1, is now 0
        //  previous source of target (0) was 2, is now 1
        //  something now has to have target 1 -- use previous source
        // After:  [2, 0, 1, 3]
    }

    /**
     * Inserts into the targets.
     *
     *
     * For example, consider the permutation
     *
     * <table border="1">
     * <caption>Example permutation</caption>
     * <tr>
     * <td>source</td>
     * <td>0</td>
     * <td>1</td>
     * <td>2</td>
     * <td>3</td>
     * <td>4</td>
    </tr> *
     * <tr>
     * <td>target</td>
     * <td>3</td>
     * <td>0</td>
     * <td>4</td>
     * <td>2</td>
     * <td>1</td>
    </tr> *
    </table> *
     *
     *
     * After applying `insertTarget(2)` every target 2 or higher is
     * shifted up one.
     *
     * <table border="1">
     * <caption>Mapping after applying insertTarget(2)</caption>
     * <tr>
     * <td>source</td>
     * <td>0</td>
     * <td>1</td>
     * <td>2</td>
     * <td>3</td>
     * <td>4</td>
     * <td>5</td>
    </tr> *
     * <tr>
     * <td>target</td>
     * <td>4</td>
     * <td>0</td>
     * <td>5</td>
     * <td>3</td>
     * <td>1</td>
     * <td>2</td>
    </tr> *
    </table> *
     *
     *
     * Note that the array has been extended to accommodate the new target, and
     * the previously unmapped source 5 is mapped to the unused target slot 2.
     *
     * @param x Ordinal of position to add to target
     */
    fun insertTarget(x: Int) {
        assert(isValid(true))
        resize(sources.size + 1)

        // Shuffle sources up.
        shuffleUp(sources, x)

        // Shuffle targets.
        increment(x, targets)
        assert(isValid(true))
    }

    /**
     * Inserts into the sources.
     *
     *
     * Behavior is analogous to [.insertTarget].
     *
     * @param x Ordinal of position to add to source
     */
    fun insertSource(x: Int) {
        assert(isValid(true))
        resize(targets.size + 1)

        // Shuffle targets up.
        shuffleUp(targets, x)

        // Increment sources.
        increment(x, sources)
        assert(isValid(true))
    }

    private fun increment(x: Int, zzz: IntArray) {
        val size = zzz.size
        for (i in 0 until size) {
            if (targets[i] == size - 1) {
                targets[i] = x
            } else if (targets[i] >= x) {
                ++targets[i]
            }
        }
    }

    private fun resize(newSize: Int) {
        assert(isValid(true))
        val size = targets.size
        val newTargets = IntArray(newSize)
        System.arraycopy(targets, 0, newTargets, 0, size)
        val newSources = IntArray(newSize)
        System.arraycopy(sources, 0, newSources, 0, size)

        // Initialize the new elements to the identity mapping.
        for (i in size until newSize) {
            newSources[i] = i
            newTargets[i] = i
        }
        targets = newTargets
        sources = newSources
        assert(isValid(true))
    }

    private fun setInternal(source: Int, target: Int) {
        targets[source] = target
        sources[target] = source
    }

    /**
     * Returns the inverse permutation.
     */
    @Override
    fun inverse(): Permutation {
        return Permutation(
            sources.clone(),
            targets.clone()
        )
    }

    /**
     * Returns whether this is the identity permutation.
     */
    @get:Override
    val isIdentity: Boolean
        get() {
            for (i in targets.indices) {
                if (targets[i] != i) {
                    return false
                }
            }
            return true
        }

    /**
     * Returns the position that `source` is mapped to.
     */
    @Override
    fun getTarget(source: Int): Int {
        return targets[source]
    }

    /**
     * Returns the position which maps to `target`.
     */
    @Override
    fun getSource(target: Int): Int {
        return sources[target]
    }

    /**
     * Checks whether this permutation is valid.
     *
     *
     *
     * @param fail Whether to assert if invalid
     * @return Whether valid
     */
    @RequiresNonNull(["sources", "targets"])
    private fun isValid(fail: Boolean): Boolean {
        val size = targets.size
        if (sources.size != size) {
            assert(!fail) { "different lengths" }
            return false
        }

        // Every element in sources has corresponding element in targets.
        val occurCount = IntArray(size)
        for (i in 0 until size) {
            val target = targets[i]
            if (sources[target] != i) {
                assert(!fail) {
                    ("source[" + target + "] = " + sources[target]
                            + ", should be " + i)
                }
                return false
            }
            val source = sources[i]
            if (targets[source] != i) {
                assert(!fail) {
                    ("target[" + source + "] = " + targets[source]
                            + ", should be " + i)
                }
                return false
            }

            // Every member should occur once.
            if (occurCount[target] != 0) {
                assert(!fail) { "target $target occurs more than once" }
                return false
            }
            occurCount[target]++
        }
        return true
    }

    @Override
    override fun hashCode(): Int {
        // not very efficient
        return toString().hashCode()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        // not very efficient
        return (obj is Permutation
                && toString().equals(obj.toString()))
    }

    // implement Mapping
    @Override
    operator fun iterator(): Iterator<IntPair> {
        return object : Iterator<IntPair?>() {
            private var i = 0

            @Override
            override fun hasNext(): Boolean {
                return i < targets.size
            }

            @Override
            override fun next(): IntPair {
                val pair = IntPair(i, targets[i])
                ++i
                return pair
            }

            @Override
            fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    @Override
    fun getSourceCount(): Int {
        return targets.size
    }

    @Override
    fun getTargetCount(): Int {
        return targets.size
    }

    @Override
    fun getMappingType(): MappingType {
        return MappingType.BIJECTION
    }

    @Override
    fun getTargetOpt(source: Int): Int {
        return getTarget(source)
    }

    @Override
    fun getSourceOpt(target: Int): Int {
        return getSource(target)
    }

    fun setAll(mapping: Mapping) {
        for (pair in mapping) {
            set(pair.source, pair.target)
        }
    }

    /**
     * Returns the product of this Permutation with a given Permutation. Does
     * not modify this Permutation or `permutation`.
     *
     *
     * For example, perm.product(perm.inverse()) yields the identity.
     */
    fun product(permutation: Permutation): Permutation {
        val product = Permutation(sources.size)
        for (i in targets.indices) {
            product[i] = permutation.getTarget(targets[i])
        }
        return product
    }

    companion object {
        private fun shuffleUp(zz: IntArray, x: Int) {
            val size = zz.size
            val t = zz[size - 1]
            System.arraycopy(zz, x, zz, x + 1, size - 1 - x)
            zz[x] = t
        }
    }
}
