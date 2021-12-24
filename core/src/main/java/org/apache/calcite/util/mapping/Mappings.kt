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
package org.apache.calcite.util.mapping

import org.apache.calcite.util.BitSets

/**
 * Utility functions related to mappings.
 *
 * @see MappingType
 *
 * @see Mapping
 *
 * @see org.apache.calcite.util.Permutation
 */
object Mappings {
    //~ Methods ----------------------------------------------------------------
    /**
     * Creates a mapping with required properties.
     */
    fun create(
        mappingType: MappingType,
        sourceCount: Int,
        targetCount: Int
    ): Mapping {
        return when (mappingType) {
            BIJECTION -> {
                assert(sourceCount == targetCount)
                Permutation(sourceCount)
            }
            INVERSE_SURJECTION -> {
                assert(sourceCount >= targetCount)
                SurjectionWithInverse(
                    sourceCount,
                    targetCount
                )
            }
            PARTIAL_SURJECTION, SURJECTION -> PartialMapping(
                sourceCount,
                targetCount,
                mappingType
            )
            PARTIAL_FUNCTION, FUNCTION -> PartialFunctionImpl(
                sourceCount,
                targetCount,
                mappingType
            )
            INVERSE_FUNCTION, INVERSE_PARTIAL_FUNCTION -> InverseMapping(
                create(mappingType.inverse(), targetCount, sourceCount)
            )
            else -> throw Util.needToImplement(
                "no known implementation for mapping type $mappingType"
            )
        }
    }

    /**
     * Creates the identity mapping.
     *
     *
     * For example, `createIdentity(2)` returns the mapping
     * {0:0, 1:1, 2:2}.
     *
     * @param fieldCount Number of sources/targets
     * @return Identity mapping
     */
    fun createIdentity(fieldCount: Int): IdentityMapping {
        return IdentityMapping(fieldCount)
    }

    /**
     * Converts a mapping to its inverse.
     */
    fun invert(mapping: Mapping): Mapping {
        return if (mapping is InverseMapping) {
            mapping.parent
        } else InverseMapping(mapping)
    }

    /**
     * Divides one mapping by another.
     *
     *
     * `divide(A, B)` returns a mapping C such that B . C (the mapping
     * B followed by the mapping C) is equivalent to A.
     *
     * @param mapping1 First mapping
     * @param mapping2 Second mapping
     * @return Mapping mapping3 such that mapping1 = mapping2 . mapping3
     */
    fun divide(mapping1: Mapping, mapping2: Mapping): Mapping {
        if (mapping1.getSourceCount() !== mapping2.getSourceCount()) {
            throw IllegalArgumentException()
        }
        val remaining: Mapping = create(
            MappingType.INVERSE_SURJECTION,
            mapping2.getTargetCount(),
            mapping1.getTargetCount()
        )
        for (target in 0 until mapping1.getTargetCount()) {
            val source: Int = mapping1.getSourceOpt(target)
            if (source >= 0) {
                val x: Int = mapping2.getTarget(source)
                remaining.set(x, target)
            }
        }
        return remaining
    }

    /**
     * Multiplies one mapping by another.
     *
     *
     * `multiply(A, B)` returns a mapping C such that A . B (the mapping
     * A followed by the mapping B) is equivalent to C.
     *
     * @param mapping1 First mapping
     * @param mapping2 Second mapping
     * @return Mapping mapping3 such that mapping1 = mapping2 . mapping3
     */
    fun multiply(mapping1: Mapping, mapping2: Mapping): Mapping {
        if (mapping1.getTargetCount() !== mapping2.getSourceCount()) {
            throw IllegalArgumentException()
        }
        val product: Mapping = create(
            MappingType.INVERSE_SURJECTION,
            mapping1.getSourceCount(),
            mapping2.getTargetCount()
        )
        for (source in 0 until mapping1.getSourceCount()) {
            val x: Int = mapping1.getTargetOpt(source)
            if (x >= 0) {
                val target: Int = mapping2.getTarget(x)
                product.set(source, target)
            }
        }
        return product
    }

    /**
     * Applies a mapping to a BitSet.
     *
     *
     * If the mapping does not affect the bit set, returns the original.
     * Never changes the original.
     *
     * @param mapping Mapping
     * @param bitSet  Bit set
     * @return Bit set with mapping applied
     */
    fun apply(mapping: Mapping, bitSet: BitSet): BitSet {
        val newBitSet = BitSet()
        for (source in BitSets.toIter(bitSet)) {
            val target: Int = mapping.getTarget(source)
            newBitSet.set(target)
        }
        return if (newBitSet.equals(bitSet)) {
            bitSet
        } else newBitSet
    }

    /**
     * Applies a mapping to an `ImmutableBitSet`.
     *
     *
     * If the mapping does not affect the bit set, returns the original.
     * Never changes the original.
     *
     * @param mapping Mapping
     * @param bitSet  Bit set
     * @return Bit set with mapping applied
     */
    fun apply(mapping: Mapping, bitSet: ImmutableBitSet): ImmutableBitSet {
        val builder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (source in bitSet) {
            val target: Int = mapping.getTarget(source)
            builder.set(target)
        }
        return builder.build(bitSet)
    }

    /**
     * Applies a mapping to a collection of `ImmutableBitSet`s.
     *
     * @param mapping Mapping
     * @param bitSets Collection of bit sets
     * @return Sorted bit sets with mapping applied
     */
    fun apply2(
        mapping: Mapping?,
        bitSets: Iterable<ImmutableBitSet?>?
    ): ImmutableList<ImmutableBitSet> {
        return ImmutableList.copyOf(
            ImmutableBitSet.ORDERING.sortedCopy(
                Util.transform(bitSets) { input1 -> apply(mapping, input1) })
        )
    }

    /**
     * Applies a mapping to a list.
     *
     * @param mapping Mapping
     * @param list    List
     * @param <T>     Element type
     * @return List with elements permuted according to mapping
    </T> */
    fun <T> apply(mapping: Mapping, list: List<T>): List<T> {
        if (mapping.getSourceCount() !== list.size()) {
            // REVIEW: too strict?
            throw IllegalArgumentException(
                ("mapping source count "
                        + mapping.getSourceCount()
                        ) + " does not match list size " + list.size()
            )
        }
        val targetCount: Int = mapping.getTargetCount()
        val list2: List<T> = ArrayList(targetCount)
        for (target in 0 until targetCount) {
            val source: Int = mapping.getSource(target)
            list2.add(list[source])
        }
        return list2
    }

    fun apply2(
        mapping: Mapping,
        list: List<Integer>
    ): List<Integer> {
        return object : AbstractList<Integer?>() {
            @Override
            operator fun get(index: Int): Integer {
                val source: Int = list[index]
                return mapping.getTarget(source)
            }

            @Override
            fun size(): Int {
                return list.size()
            }
        }
    }

    /**
     * Creates a view of a list, permuting according to a mapping.
     *
     * @param mapping Mapping
     * @param list    List
     * @param <T>     Element type
     * @return Permuted view of list
    </T> */
    fun <T> apply3(
        mapping: Mapping,
        list: List<T>
    ): List<T> {
        return object : AbstractList<T>() {
            @Override
            operator fun get(index: Int): T {
                return list[mapping.getSource(index)]
            }

            @Override
            fun size(): Int {
                return mapping.getTargetCount()
            }
        }
    }

    /**
     * Creates a view of a list, permuting according to a target mapping.
     *
     * @param mapping Mapping
     * @param list    List
     * @param <T>     Element type
     * @return Permuted view of list
    </T> */
    fun <T> permute(
        list: List<T>,
        mapping: TargetMapping
    ): List<T> {
        return object : AbstractList<T>() {
            @Override
            operator fun get(index: Int): T {
                return list[mapping.getTarget(index)]
            }

            @Override
            fun size(): Int {
                return mapping.sourceCount
            }
        }
    }

    /**
     * Returns a mapping as a list such that `list.get(source)` is
     * `mapping.getTarget(source)` and `list.size()` is
     * `mapping.getSourceCount()`.
     *
     *
     * Converse of [.target]
     * @see .asListNonNull
     */
    @CheckReturnValue
    fun asList(mapping: TargetMapping): List<Integer> {
        return object : AbstractList<Integer?>() {
            @Override
            @Nullable
            operator fun get(source: Int): Integer? {
                val target = mapping.getTargetOpt(source)
                return if (target < 0) null else target
            }

            @Override
            fun size(): Int {
                return mapping.sourceCount
            }
        }
    }

    /**
     * Returns a mapping as a list such that `list.get(source)` is
     * `mapping.getTarget(source)` and `list.size()` is
     * `mapping.getSourceCount()`.
     *
     *
     * The resulting list never contains null elements
     *
     *
     * Converse of [.target]
     * @see .asList
     */
    @CheckReturnValue
    fun asListNonNull(mapping: TargetMapping): List<Integer> {
        return object : AbstractList<Integer?>() {
            @Override
            operator fun get(source: Int): Integer {
                val target = mapping.getTargetOpt(source)
                if (target < 0) {
                    throw IllegalArgumentException(
                        "Element " + source + " is not found in mapping "
                                + mapping
                    )
                }
                return target
            }

            @Override
            fun size(): Int {
                return mapping.sourceCount
            }
        }
    }

    /**
     * Converts a [Map] of integers to a [TargetMapping].
     */
    fun target(
        map: Map<Integer?, Integer?>,
        sourceCount: Int,
        targetCount: Int
    ): TargetMapping {
        val mapping = PartialFunctionImpl(
            sourceCount, targetCount, MappingType.FUNCTION
        )
        for (entry in map.entrySet()) {
            mapping[entry.getKey()] = entry.getValue()
        }
        return mapping
    }

    fun target(
        function: IntFunction<out Integer?>,
        sourceCount: Int,
        targetCount: Int
    ): TargetMapping {
        val mapping = PartialFunctionImpl(sourceCount, targetCount, MappingType.FUNCTION)
        for (source in 0 until sourceCount) {
            val target: Integer = function.apply(source)
            if (target != null) {
                mapping[source] = target
            }
        }
        return mapping
    }

    fun target(
        pairs: Iterable<IntPair>, sourceCount: Int,
        targetCount: Int
    ): Mapping {
        val mapping = PartialFunctionImpl(sourceCount, targetCount, MappingType.FUNCTION)
        for (pair in pairs) {
            mapping[pair.source] = pair.target
        }
        return mapping
    }

    fun source(targets: List<Integer>, targetCount: Int): Mapping {
        val sourceCount: Int = targets.size()
        val mapping = PartialFunctionImpl(sourceCount, targetCount, MappingType.FUNCTION)
        for (source in 0 until sourceCount) {
            val target: Int = targets[source]
            mapping[source] = target
        }
        return mapping
    }

    fun target(sources: List<Integer>, sourceCount: Int): Mapping {
        val targetCount: Int = sources.size()
        val mapping = PartialFunctionImpl(sourceCount, targetCount, MappingType.FUNCTION)
        for (target in 0 until targetCount) {
            val source: Int = sources[target]
            mapping[source] = target
        }
        return mapping
    }

    /** Creates a bijection.
     *
     *
     * Throws if sources and targets are not one to one.  */
    fun bijection(targets: List<Integer?>?): Mapping {
        return Permutation(Ints.toArray(targets))
    }

    /** Creates a bijection.
     *
     *
     * Throws if sources and targets are not one to one.  */
    fun bijection(targets: Map<Integer?, Integer?>): Mapping {
        val ints = IntArray(targets.size())
        for (i in 0 until targets.size()) {
            val value: Integer = targets[i] ?: throw NullPointerException("Index $i is not mapped in $targets")
            ints[i] = value
        }
        return Permutation(ints)
    }

    /**
     * Returns whether a mapping is the identity.
     */
    fun isIdentity(mapping: TargetMapping): Boolean {
        if (mapping.sourceCount != mapping.targetCount) {
            return false
        }
        for (i in 0 until mapping.sourceCount) {
            if (mapping.getTargetOpt(i) != i) {
                return false
            }
        }
        return true
    }

    /**
     * Returns whether a mapping keeps order.
     *
     *
     * For example, {0:0, 1:1} and {0:1, 1:1} keeps order,
     * and {0:1, 1:0} breaks the initial order.
     */
    fun keepsOrdering(mapping: TargetMapping): Boolean {
        var prevTarget = -1
        for (i in 0 until mapping.sourceCount) {
            val target = mapping.getTargetOpt(i)
            if (target != -1) {
                if (target < prevTarget) {
                    return false
                }
                prevTarget = target
            }
        }
        return true
    }

    /**
     * Creates a mapping that consists of a set of contiguous ranges.
     *
     *
     * For example,
     *
     * <blockquote><pre>createShiftMapping(60,
     * 100, 0, 3,
     * 200, 50, 5);
    </pre></blockquote> *
     *
     *
     * creates
     *
     * <table>
     * <caption>Example mapping</caption>
     * <tr><th>Source</th><th>Target</th></tr>
     * <tr><td>0</td><td>100</td></tr>
     * <tr><td>1</td><td>101</td></tr>
     * <tr><td>2</td><td>102</td></tr>
     * <tr><td>3</td><td>-1</td></tr>
     * <tr><td>...</td><td>-1</td></tr>
     * <tr><td>50</td><td>200</td></tr>
     * <tr><td>51</td><td>201</td></tr>
     * <tr><td>52</td><td>202</td></tr>
     * <tr><td>53</td><td>203</td></tr>
     * <tr><td>54</td><td>204</td></tr>
     * <tr><td>55</td><td>-1</td></tr>
     * <tr><td>...</td><td>-1</td></tr>
     * <tr><td>59</td><td>-1</td></tr>
    </table> *
     *
     * @param sourceCount Maximum value of `source`
     * @param ints        Collection of ranges, each
     * `(target, source, count)`
     * @return Mapping that maps from source ranges to target ranges
     */
    fun createShiftMapping(
        sourceCount: Int, vararg ints: Int
    ): TargetMapping {
        var targetCount = 0
        assert(ints.size % 3 == 0)
        run {
            var i = 0
            while (i < ints.size) {
                val target = ints[i]
                val length = ints[i + 2]
                val top = target + length
                targetCount = Math.max(targetCount, top)
                i += 3
            }
        }
        val mapping: TargetMapping = create(
            MappingType.INVERSE_SURJECTION,
            sourceCount,  // aCount + bCount + cCount,
            targetCount
        ) // cCount + bCount
        var i = 0
        while (i < ints.size) {
            val target = ints[i]
            val source = ints[i + 1]
            val length = ints[i + 2]
            assert(source + length <= sourceCount)
            for (j in 0 until length) {
                assert(mapping.getTargetOpt(source + j) == -1)
                mapping[source + j] = target + j
            }
            i += 3
        }
        return mapping
    }

    /**
     * Creates a mapping by appending two mappings.
     *
     *
     * Sources and targets of the second mapping are shifted to the right.
     *
     *
     * For example, <pre>append({0:0, 1:1}, {0:0, 1:1, 2:2})</pre> yields
     * <pre>{0:0, 1:1, 2:2, 3:3, 4:4}</pre>.
     *
     * @see .merge
     */
    fun append(
        mapping0: TargetMapping,
        mapping1: TargetMapping
    ): TargetMapping {
        val s0 = mapping0.sourceCount
        val s1 = mapping1.sourceCount
        val t0 = mapping0.targetCount
        val t1 = mapping1.targetCount
        val mapping: TargetMapping = create(MappingType.INVERSE_SURJECTION, s0 + s1, t0 + t1)
        for (s in 0 until s0) {
            val t = mapping0.getTargetOpt(s)
            if (t >= 0) {
                mapping[s] = t
            }
        }
        for (s in 0 until s1) {
            val t = mapping1.getTargetOpt(s)
            if (t >= 0) {
                mapping[s0 + s] = t0 + t
            }
        }
        return mapping
    }

    /**
     * Creates a mapping by merging two mappings. There must be no clashes.
     *
     *
     * Unlike [.append], sources and targets are not shifted.
     *
     *
     * For example, `merge({0:0, 1:1}, {2:2, 3:3, 4:4})` yields
     * `{0:0, 1:1, 2:2, 3:3, 4:4}`.
     * `merge({0:0, 1:1}, {1:2, 2:3})` throws, because there are
     * two entries with source=1.
     */
    fun merge(
        mapping0: TargetMapping,
        mapping1: TargetMapping
    ): TargetMapping {
        val s0 = mapping0.sourceCount
        val s1 = mapping1.sourceCount
        val sMin: Int = Math.min(s0, s1)
        val sMax: Int = Math.max(s0, s1)
        val t0 = mapping0.targetCount
        val t1 = mapping1.targetCount
        val tMax: Int = Math.max(t0, t1)
        val mapping: TargetMapping = create(MappingType.INVERSE_SURJECTION, sMax, tMax)
        for (s in 0 until sMin) {
            var t = mapping0.getTargetOpt(s)
            if (t >= 0) {
                mapping[s] = t
                assert(mapping1.getTargetOpt(s) < 0)
            } else {
                t = mapping1.getTargetOpt(s)
                if (t >= 0) {
                    mapping[s] = t
                }
            }
        }
        for (s in sMin until sMax) {
            var t = if (s < s0) mapping0.getTargetOpt(s) else -1
            if (t >= 0) {
                mapping[s] = t
                assert(s >= s1 || mapping1.getTargetOpt(s) < 0)
            } else {
                t = if (s < s1) mapping1.getTargetOpt(s) else -1
                if (t >= 0) {
                    mapping[s] = t
                }
            }
        }
        return mapping
    }
    /**
     * Returns a mapping that shifts a given mapping's source by a given
     * offset.
     *
     *
     * For example, given `mapping` with sourceCount=2, targetCount=8,
     * and (source, target) entries {[0: 5], [1: 7]}, offsetSource(mapping, 3)
     * returns a mapping with sourceCount=5, targetCount=8,
     * and (source, target) entries {[3: 5], [4: 7]}.
     *
     * @param mapping     Input mapping
     * @param offset      Offset to be applied to each source
     * @param sourceCount New source count; must be at least `mapping`'s
     * source count plus `offset`
     * @return Shifted mapping
     */
    /**
     * Returns a mapping that shifts a given mapping's source by a given
     * offset, incrementing the number of sources by the minimum possible.
     *
     * @param mapping     Input mapping
     * @param offset      Offset to be applied to each source
     * @return Shifted mapping
     */
    @JvmOverloads
    fun offsetSource(
        mapping: TargetMapping, offset: Int, sourceCount: Int = mapping.sourceCount + offset
    ): TargetMapping {
        if (sourceCount < mapping.sourceCount + offset) {
            throw IllegalArgumentException("new source count too low")
        }
        return target(
            IntFunction<Integer> { source ->
                val source2: Int = source - offset
                if (source2 < 0 || source2 >= mapping.sourceCount) null else mapping.getTargetOpt(source2)
            } as IntFunction<Integer?>,
            sourceCount,
            mapping.targetCount
        )
    }
    /**
     * Returns a mapping that shifts a given mapping's target by a given
     * offset.
     *
     *
     * For example, given `mapping` with sourceCount=2, targetCount=8,
     * and (source, target) entries {[0: 5], [1: 7]}, offsetTarget(mapping, 3)
     * returns a mapping with sourceCount=2, targetCount=11,
     * and (source, target) entries {[0: 8], [1: 10]}.
     *
     * @param mapping     Input mapping
     * @param offset      Offset to be applied to each target
     * @param targetCount New target count; must be at least `mapping`'s
     * target count plus `offset`
     * @return Shifted mapping
     */
    /**
     * Returns a mapping that shifts a given mapping's target by a given
     * offset, incrementing the number of targets by the minimum possible.
     *
     * @param mapping     Input mapping
     * @param offset      Offset to be applied to each target
     * @return Shifted mapping
     */
    @JvmOverloads
    fun offsetTarget(
        mapping: TargetMapping, offset: Int, targetCount: Int = mapping.targetCount + offset
    ): TargetMapping {
        if (targetCount < mapping.targetCount + offset) {
            throw IllegalArgumentException("new target count too low")
        }
        return target(
            IntFunction<Integer> { source ->
                val target = mapping.getTargetOpt(source)
                if (target < 0) null else target + offset
            } as IntFunction<Integer?>,
            mapping.sourceCount, targetCount)
    }

    /**
     * Returns a mapping that shifts a given mapping's source and target by a
     * given offset.
     *
     *
     * For example, given `mapping` with sourceCount=2, targetCount=8,
     * and (source, target) entries {[0: 5], [1: 7]}, offsetSource(mapping, 3)
     * returns a mapping with sourceCount=5, targetCount=8,
     * and (source, target) entries {[3: 8], [4: 10]}.
     *
     * @param mapping     Input mapping
     * @param offset      Offset to be applied to each source
     * @param sourceCount New source count; must be at least `mapping`'s
     * source count plus `offset`
     * @return Shifted mapping
     */
    fun offset(
        mapping: TargetMapping, offset: Int, sourceCount: Int
    ): TargetMapping {
        if (sourceCount < mapping.sourceCount + offset) {
            throw IllegalArgumentException("new source count too low")
        }
        return target(
            label@ IntFunction<Integer> { source ->
                val source2: Int = source - offset
                if (source2 < 0 || source2 >= mapping.sourceCount) {
                    return@label null
                }
                val target = mapping.getTargetOpt(source2)
                if (target < 0) {
                    return@label null
                }
                target + offset
            } as IntFunction<Integer?>,
            sourceCount,
            mapping.targetCount + offset)
    }

    /** Returns whether a list of integers is the identity mapping
     * [0, ..., n - 1].  */
    fun isIdentity(list: List<Integer?>, count: Int): Boolean {
        if (list.size() !== count) {
            return false
        }
        for (i in 0 until count) {
            val o: Integer? = list[i]
            if (o == null || o != i) {
                return false
            }
        }
        return true
    }

    /** Inverts an [java.lang.Iterable] over
     * [org.apache.calcite.util.mapping.IntPair]s.  */
    fun invert(pairs: Iterable<IntPair>): Iterable<IntPair> {
        return Iterable<IntPair> { invert(pairs.iterator()) }
    }

    /** Inverts an [java.util.Iterator] over
     * [org.apache.calcite.util.mapping.IntPair]s.  */
    fun invert(pairs: Iterator<IntPair>): Iterator<IntPair> {
        return object : Iterator<IntPair?>() {
            @Override
            override fun hasNext(): Boolean {
                return pairs.hasNext()
            }

            @Override
            override fun next(): IntPair {
                val pair: IntPair = pairs.next()
                return IntPair.of(pair.target, pair.source)
            }

            @Override
            fun remove() {
                throw UnsupportedOperationException("remove")
            }
        }
    }

    /** Applies a mapping to an optional integer, returning an optional
     * result.  */
    fun apply(mapping: TargetMapping, i: Int): Int {
        return if (i < 0) i else mapping.getTarget(i)
    }
    //~ Inner Interfaces -------------------------------------------------------
    /**
     * Core interface of all mappings.
     */
    interface CoreMapping : Iterable<IntPair?> {
        /**
         * Returns the mapping type.
         *
         * @return Mapping type
         */
        val mappingType: org.apache.calcite.util.mapping.MappingType?

        /**
         * Returns the number of elements in the mapping.
         */
        fun size(): Int
    }

    /**
     * Mapping where every source has a target. But:
     *
     *
     *  * A target may not have a source.
     *  * May not be finite.
     *
     */
    interface FunctionMapping : CoreMapping {
        /**
         * Returns the target that a source maps to, or -1 if it is not mapped.
         */
        fun getTargetOpt(source: Int): Int

        /**
         * Returns the target that a source maps to.
         *
         * @param source source
         * @return target
         * @throws NoElementException if source is not mapped
         */
        fun getTarget(source: Int): Int

        @get:Override
        abstract override val mappingType: org.apache.calcite.util.mapping.MappingType?
        val sourceCount: Int
    }

    /**
     * Mapping suitable for sourcing columns.
     *
     *
     * Properties:
     *
     *
     *  * It has a finite number of sources and targets
     *  * Each target has exactly one source
     *  * Each source has at most one target
     *
     *
     *
     * TODO: figure out which interfaces this should extend
     */
    interface SourceMapping : CoreMapping {
        val sourceCount: Int

        /**
         * Returns the source that a target maps to.
         *
         * @param target target
         * @return source
         * @throws NoElementException if target is not mapped
         */
        fun getSource(target: Int): Int

        /**
         * Returns the source that a target maps to, or -1 if it is not mapped.
         */
        fun getSourceOpt(target: Int): Int
        val targetCount: Int

        /**
         * Returns the target that a source maps to, or -1 if it is not mapped.
         */
        fun getTargetOpt(source: Int): Int

        @get:Override
        abstract override val mappingType: org.apache.calcite.util.mapping.MappingType?
        val isIdentity: Boolean
        fun inverse(): Mapping
    }

    /**
     * Mapping suitable for mapping columns to a target.
     *
     *
     * Properties:
     *
     *
     *  * It has a finite number of sources and targets
     *  * Each target has at most one source
     *  * Each source has exactly one target
     *
     *
     *
     * TODO: figure out which interfaces this should extend
     */
    interface TargetMapping : FunctionMapping {
        @get:Override
        abstract override val sourceCount: Int

        /**
         * Returns the source that a target maps to, or -1 if it is not mapped.
         */
        fun getSourceOpt(target: Int): Int
        val targetCount: Int

        /**
         * Returns the target that a source maps to.
         *
         * @param source source
         * @return target
         * @throws NoElementException if source is not mapped
         */
        @Override
        override fun getTarget(source: Int): Int

        /**
         * Returns the target that a source maps to, or -1 if it is not mapped.
         */
        @Override
        override fun getTargetOpt(source: Int): Int
        operator fun set(source: Int, target: Int)
        fun inverse(): Mapping
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Abstract implementation of [Mapping].  */
    abstract class AbstractMapping : Mapping {
        @Override
        override operator fun set(source: Int, target: Int) {
            throw UnsupportedOperationException()
        }

        @Override
        override fun getTargetOpt(source: Int): Int {
            throw UnsupportedOperationException()
        }

        @Override
        override fun getTarget(source: Int): Int {
            val target = getTargetOpt(source)
            if (target == -1) {
                throw NoElementException(
                    "source #" + source + " has no target in mapping " + toString()
                )
            }
            return target
        }

        @Override
        override fun getSourceOpt(target: Int): Int {
            throw UnsupportedOperationException()
        }

        @Override
        override fun getSource(target: Int): Int {
            val source = getSourceOpt(target)
            if (source == -1) {
                throw NoElementException(
                    "target #" + target + " has no source in mapping " + toString()
                )
            }
            return source
        }

        @get:Override
        override val sourceCount: Int
            get() {
                throw UnsupportedOperationException()
            }

        @get:Override
        override val targetCount: Int
            get() {
                throw UnsupportedOperationException()
            }

        @get:Override
        override val isIdentity: Boolean
            get() {
                val sourceCount = sourceCount
                val targetCount = targetCount
                if (sourceCount != targetCount) {
                    return false
                }
                for (i in 0 until sourceCount) {
                    if (getSource(i) != i) {
                        return false
                    }
                }
                return true
            }

        /**
         * Returns a string representation of this mapping.
         *
         *
         * For example, the mapping
         *
         * <table border="1">
         * <caption>Example</caption>
         * <tr>
         * <th>source</th>
         * <td>0</td>
         * <td>1</td>
         * <td>2</td>
        </tr> *
         * <tr>
         * <th>target</th>
         * <td>-1</td>
         * <td>3</td>
         * <td>2</td>
        </tr> *
        </table> *
         *
         * <table border="1">
         * <caption>Example</caption>
         * <tr>
         * <th>target</th>
         * <td>0</td>
         * <td>1</td>
         * <td>2</td>
         * <td>3</td>
        </tr> *
         * <tr>
         * <th>source</th>
         * <td>-1</td>
         * <td>-1</td>
         * <td>2</td>
         * <td>1</td>
        </tr> *
        </table> *
         *
         *
         * is represented by the string "[1:3, 2:2]".
         *
         *
         * This method relies upon the optional method [.iterator].
         */
        @Override
        override fun toString(): String {
            val buf = StringBuilder()
            buf.append("[size=").append(size())
                .append(", sourceCount=").append(sourceCount)
                .append(", targetCount=").append(targetCount)
                .append(", elements=[")
            var i = 0
            for (pair in this) {
                if (i++ > 0) {
                    buf.append(", ")
                }
                buf.append(pair.source).append(':').append(pair.target)
            }
            buf.append("]]")
            return buf.toString()
        }
    }

    /** Abstract implementation of mapping where both source and target
     * domains are finite.  */
    abstract class FiniteAbstractMapping : AbstractMapping() {
        @Override
        override operator fun iterator(): Iterator<IntPair> {
            return FunctionMappingIter(this)
        }

        @Override
        override fun hashCode(): Int {
            // not very efficient
            return toString().hashCode()
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            // not very efficient
            return (obj is Mapping
                    && toString().equals(obj.toString()))
        }
    }

    /** Iterator that yields the (source, target) values in a
     * [FunctionMapping].  */
    internal class FunctionMappingIter(private val mapping: FunctionMapping) : Iterator<IntPair?> {
        private var i = 0

        @Override
        override fun hasNext(): Boolean {
            return (i < mapping.sourceCount
                    || mapping.sourceCount == -1)
        }

        @Override
        override fun next(): IntPair {
            val x = i++
            return IntPair(
                x,
                mapping.getTarget(x)
            )
        }

        @Override
        fun remove() {
            throw UnsupportedOperationException()
        }
    }

    /**
     * Thrown when a mapping is expected to return one element but returns
     * several.
     */
    class TooManyElementsException : RuntimeException()

    /**
     * Thrown when a mapping is expected to return one element but returns none.
     */
    class NoElementException
    /**
     * Creates a NoElementException.
     *
     * @param message Message
     */
        (message: String?) : RuntimeException(message)

    /**
     * A mapping where a source has at most one target, and every target has at
     * most one source.
     */
    class PartialMapping : FiniteAbstractMapping, Mapping, FunctionMapping, TargetMapping {
        protected val sources: IntArray
        protected val targets: IntArray
        private override val mappingType: MappingType

        /**
         * Creates a partial mapping.
         *
         *
         * Initially, no element is mapped to any other:
         *
         * <table border="1">
         * <caption>Example</caption>
         * <tr>
         * <th>source</th>
         * <td>0</td>
         * <td>1</td>
         * <td>2</td>
        </tr> *
         * <tr>
         * <th>target</th>
         * <td>-1</td>
         * <td>-1</td>
         * <td>-1</td>
        </tr> *
        </table> *
         *
         * <table border="1">
         * <caption>Example</caption>
         * <tr>
         * <th>target</th>
         * <td>0</td>
         * <td>1</td>
         * <td>2</td>
         * <td>3</td>
        </tr> *
         * <tr>
         * <th>source</th>
         * <td>-1</td>
         * <td>-1</td>
         * <td>-1</td>
         * <td>-1</td>
        </tr> *
        </table> *
         *
         * @param sourceCount Number of source elements
         * @param targetCount Number of target elements
         * @param mappingType Mapping type; must not allow multiple sources per
         * target or multiple targets per source
         */
        constructor(
            sourceCount: Int,
            targetCount: Int,
            mappingType: MappingType
        ) {
            this.mappingType = mappingType
            assert(mappingType.isSingleSource()) { mappingType }
            assert(mappingType.isSingleTarget()) { mappingType }
            sources = IntArray(targetCount)
            targets = IntArray(sourceCount)
            Arrays.fill(sources, -1)
            Arrays.fill(targets, -1)
        }

        /**
         * Creates a partial mapping from a list. For example, `
         * PartialMapping({1, 2, 4}, 6)` creates the mapping
         *
         * <table border="1">
         * <caption>Example</caption>
         * <tr>
         * <th>source</th>
         * <td>0</td>
         * <td>1</td>
         * <td>2</td>
         * <td>3</td>
         * <td>4</td>
         * <td>5</td>
        </tr> *
         * <tr>
         * <th>target</th>
         * <td>-1</td>
         * <td>0</td>
         * <td>1</td>
         * <td>-1</td>
         * <td>2</td>
         * <td>-1</td>
        </tr> *
        </table> *
         *
         * @param sourceList  List whose i'th element is the source of target #i
         * @param sourceCount Number of elements in the source domain
         * @param mappingType Mapping type, must be
         * [org.apache.calcite.util.mapping.MappingType.PARTIAL_SURJECTION]
         * or stronger.
         */
        constructor(
            sourceList: List<Integer>,
            sourceCount: Int,
            mappingType: MappingType
        ) {
            this.mappingType = mappingType
            assert(mappingType.isSingleSource())
            assert(mappingType.isSingleTarget())
            val targetCount: Int = sourceList.size()
            targets = IntArray(sourceCount)
            sources = IntArray(targetCount)
            Arrays.fill(sources, -1)
            for (i in 0 until sourceList.size()) {
                val source: Int = sourceList[i]
                sources[i] = source
                if (source >= 0) {
                    targets[source] = i
                } else {
                    assert(!this.mappingType.isMandatorySource())
                }
            }
        }

        private constructor(
            sources: IntArray,
            targets: IntArray,
            mappingType: MappingType
        ) {
            this.sources = sources
            this.targets = targets
            this.mappingType = mappingType
        }

        @Override
        override fun getMappingType(): MappingType {
            return mappingType
        }

        @Override
        override fun getSourceCount(): Int {
            return targets.size
        }

        @Override
        override fun getTargetCount(): Int {
            return sources.size
        }

        @Override
        override fun clear() {
            Arrays.fill(sources, -1)
            Arrays.fill(targets, -1)
        }

        @Override
        override fun size(): Int {
            var size = 0
            val a = if (sources.size < targets.size) sources else targets
            for (i1 in a) {
                if (i1 >= 0) {
                    ++size
                }
            }
            return size
        }

        @Override
        override fun inverse(): Mapping {
            return PartialMapping(
                targets.clone(),
                sources.clone(),
                mappingType.inverse()
            )
        }

        @Override
        override fun iterator(): Iterator<IntPair> {
            return MappingItr()
        }

        protected fun isValid(): Boolean {
            assertPartialValid(sources, targets)
            assertPartialValid(targets, sources)
            return true
        }

        @Override
        override fun set(source: Int, target: Int) {
            assert(isValid())
            val prevTarget = targets[source]
            targets[source] = target
            val prevSource = sources[target]
            sources[target] = source
            if (prevTarget != -1) {
                sources[prevTarget] = prevSource
            }
            if (prevSource != -1) {
                targets[prevSource] = prevTarget
            }
            assert(isValid())
        }

        /**
         * Returns the source that a target maps to, or -1 if it is not mapped.
         */
        @Override
        override fun getSourceOpt(target: Int): Int {
            return sources[target]
        }

        /**
         * Returns the target that a source maps to, or -1 if it is not mapped.
         */
        @Override
        override fun getTargetOpt(source: Int): Int {
            return targets[source]
        }

        @Override
        override fun isIdentity(): Boolean {
            if (sources.size != targets.size) {
                return false
            }
            for (i in sources.indices) {
                val source = sources[i]
                if (source != i) {
                    return false
                }
            }
            return true
        }

        /** Mapping iterator.  */
        private inner class MappingItr internal constructor() : Iterator<IntPair?> {
            var i = -1

            init {
                advance()
            }

            @Override
            override fun hasNext(): Boolean {
                return i < targets.size
            }

            private fun advance() {
                do {
                    ++i
                } while (i < targets.size && targets[i] == -1)
            }

            @Override
            override fun next(): IntPair {
                val pair = IntPair(i, targets[i])
                advance()
                return pair
            }

            @Override
            fun remove() {
                throw UnsupportedOperationException()
            }
        }

        companion object {
            private fun assertPartialValid(sources: IntArray, targets: IntArray) {
                for (i in sources.indices) {
                    val source = sources[i]
                    assert(source >= -1)
                    assert(source < targets.size)
                    assert(source == -1 || targets[source] == i)
                }
            }
        }
    }

    /**
     * A surjection with inverse has precisely one source for each target.
     * (Whereas a general surjection has at least one source for each target.)
     * Every source has at most one target.
     *
     *
     * If you call [.set] on a target, the target's previous source
     * will be lost.
     */
    internal class SurjectionWithInverse(sourceCount: Int, targetCount: Int) :
        PartialMapping(sourceCount, targetCount, MappingType.INVERSE_SURJECTION) {
        /**
         * Creates a mapping between a source and a target.
         *
         *
         * It is an error to map a target to a source which already has a
         * target.
         *
         *
         * If you map a source to a target which already has a source, the
         * old source becomes an orphan.
         *
         * @param source source
         * @param target target
         */
        @Override
        override fun set(source: Int, target: Int) {
            assert(isValid())
            val prevTarget: Int = targets.get(source)
            if (prevTarget != -1) {
                throw IllegalArgumentException(
                    "source #" + source
                            + " is already mapped to target #" + target
                )
            }
            targets.get(source) = target
            sources.get(target) = source
        }

        @Override
        override fun getSource(target: Int): Int {
            return sources.get(target)
        }
    }

    /** The identity mapping, of a given size, or infinite.  */
    class IdentityMapping
    /**
     * Creates an identity mapping.
     *
     * @param size Size, or -1 if infinite.
     */(private val size: Int) : AbstractMapping(), FunctionMapping, TargetMapping, SourceMapping {
        @Override
        override fun clear() {
            throw UnsupportedOperationException("Mapping is read-only")
        }

        @Override
        override fun size(): Int {
            return size
        }

        @Override
        override fun inverse(): Mapping {
            return this
        }

        @Override
        override fun isIdentity(): Boolean {
            return true
        }

        @Override
        override fun set(source: Int, target: Int) {
            throw UnsupportedOperationException()
        }

        @Override
        override fun getMappingType(): MappingType {
            return MappingType.BIJECTION
        }

        @Override
        override fun getSourceCount(): Int {
            return size
        }

        @Override
        override fun getTargetCount(): Int {
            return size
        }

        /**
         * Returns the target that a source maps to.
         *
         * @param source source
         * @return target
         */
        @Override
        override fun getTarget(source: Int): Int {
            if (source < 0 || size != -1 && source >= size) {
                throw IndexOutOfBoundsException(
                    "source #" + source
                            + " has no target in identity mapping of size " + size
                )
            }
            return source
        }

        /**
         * Returns the target that a source maps to, or -1 if it is not mapped.
         *
         * @param source source
         * @return target
         */
        @Override
        override fun getTargetOpt(source: Int): Int {
            if (source < 0 || size != -1 && source >= size) {
                throw IndexOutOfBoundsException(
                    "source #" + source
                            + " has no target in identity mapping of size " + size
                )
            }
            return source
        }

        /**
         * Returns the source that a target maps to.
         *
         * @param target target
         * @return source
         */
        @Override
        override fun getSource(target: Int): Int {
            if (target < 0 || size != -1 && target >= size) {
                throw IndexOutOfBoundsException(
                    "target #" + target
                            + " has no source in identity mapping of size " + size
                )
            }
            return target
        }

        /**
         * Returns the source that a target maps to, or -1 if it is not mapped.
         *
         * @param target target
         * @return source
         */
        @Override
        override fun getSourceOpt(target: Int): Int {
            if (target < 0 || size != -1 && target >= size) {
                throw IndexOutOfBoundsException(
                    "target #" + target
                            + " has no source in identity mapping of size " + size
                )
            }
            return target
        }

        @Override
        override fun iterator(): Iterator<IntPair> {
            return object : Iterator<IntPair?>() {
                var i = 0

                @Override
                override fun hasNext(): Boolean {
                    return size < 0 || i < size
                }

                @Override
                override fun next(): IntPair {
                    val x = i++
                    return IntPair(x, x)
                }

                @Override
                fun remove() {
                    throw UnsupportedOperationException()
                }
            }
        }
    }

    /** Source mapping that returns the same result as a parent
     * [SourceMapping] except for specific overriding elements.  */
    class OverridingSourceMapping(
        private val parent: SourceMapping,
        private val source: Int,
        private val target: Int
    ) : AbstractMapping(), SourceMapping {
        @Override
        override fun clear() {
            throw UnsupportedOperationException("Mapping is read-only")
        }

        @Override
        override fun size(): Int {
            return if (parent.getSourceOpt(target) >= 0) parent.size() else parent.size() + 1
        }

        @Override
        override fun inverse(): Mapping {
            return OverridingTargetMapping(
                parent.inverse() as TargetMapping,
                target,
                source
            )
        }

        @Override
        override fun getMappingType(): MappingType {
            // FIXME: Mapping type might be weaker than parent.
            return parent.mappingType!!
        }

        @Override
        override fun getSource(target: Int): Int {
            return if (target == this.target) {
                source
            } else {
                parent.getSource(target)
            }
        }

        @Override
        override fun isIdentity(): Boolean {
            // FIXME: It's possible that parent was not the identity but that
            // this overriding fixed it.
            return (source == target
                    && parent.isIdentity)
        }

        @Override
        override fun iterator(): Iterator<IntPair> {
            throw Util.needToImplement(this)
        }
    }

    /** Target mapping that returns the same result as a parent
     * [TargetMapping] except for specific overriding elements.  */
    class OverridingTargetMapping(
        private val parent: TargetMapping,
        private val target: Int,
        private val source: Int
    ) : AbstractMapping(), TargetMapping {
        @Override
        override fun clear() {
            throw UnsupportedOperationException("Mapping is read-only")
        }

        @Override
        override fun size(): Int {
            return if (parent.getTargetOpt(source) >= 0) parent.size() else parent.size() + 1
        }

        @Override
        override fun set(source: Int, target: Int) {
            parent[source] = target
        }

        @Override
        override fun inverse(): Mapping {
            return OverridingSourceMapping(
                parent.inverse(),
                source,
                target
            )
        }

        @Override
        override fun getMappingType(): MappingType {
            // FIXME: Mapping type might be weaker than parent.
            return parent.mappingType!!
        }

        @Override
        override fun isIdentity(): Boolean {
            // FIXME: Possible that parent is not identity but this overriding
            // fixes it.
            return (source == target
                    && (parent as Mapping).isIdentity())
        }

        @Override
        override fun getTarget(source: Int): Int {
            return if (source == this.source) {
                target
            } else {
                parent.getTarget(source)
            }
        }

        @Override
        override fun iterator(): Iterator<IntPair> {
            throw Util.needToImplement(this)
        }
    }

    /**
     * Implementation of [Mapping] where a source can have at most one
     * target, and a target can have any number of sources. The source count
     * must be finite, but the target count may be infinite.
     *
     *
     * The implementation uses an array for the forward-mapping, but does not
     * store the backward mapping.
     */
    private class PartialFunctionImpl internal constructor(
        sourceCount: Int,
        targetCount: Int,
        mappingType: MappingType
    ) : AbstractMapping(), TargetMapping {
        private override val sourceCount: Int
        private override val targetCount: Int
        private override val mappingType: MappingType
        private val targets: IntArray

        init {
            if (sourceCount < 0) {
                throw IllegalArgumentException("Sources must be finite")
            }
            this.sourceCount = sourceCount
            this.targetCount = targetCount
            this.mappingType = mappingType
            if (!mappingType.isSingleTarget()) {
                throw IllegalArgumentException(
                    "Must have at most one target"
                )
            }
            targets = IntArray(sourceCount)
            Arrays.fill(targets, -1)
        }

        @Override
        override fun getSourceCount(): Int {
            return sourceCount
        }

        @Override
        override fun getTargetCount(): Int {
            return targetCount
        }

        @Override
        override fun clear() {
            Arrays.fill(targets, -1)
        }

        @Override
        override fun size(): Int {
            var size = 0
            for (target in targets) {
                if (target >= 0) {
                    ++size
                }
            }
            return size
        }

        @SuppressWarnings("method.invocation.invalid")
        @Override
        override fun iterator(): Iterator<IntPair> {
            return object : Iterator<IntPair?>() {
                var i = -1

                init {
                    advance()
                }

                private fun advance() {
                    while (true) {
                        ++i
                        if (i >= sourceCount) {
                            break // end
                        }
                        if (targets[i] >= 0) {
                            break // found one
                        }
                    }
                }

                @Override
                override fun hasNext(): Boolean {
                    return i < sourceCount
                }

                @Override
                override fun next(): IntPair {
                    val pair = IntPair(i, targets[i])
                    advance()
                    return pair
                }

                @Override
                fun remove() {
                    throw UnsupportedOperationException()
                }
            }
        }

        @Override
        override fun getMappingType(): MappingType {
            return mappingType
        }

        @Override
        override fun inverse(): Mapping {
            return target(invert(this), targetCount, sourceCount)
        }

        @Override
        override fun set(source: Int, target: Int) {
            if (target < 0 && mappingType.isMandatorySource()) {
                throw IllegalArgumentException("Target is required")
            }
            if (target >= targetCount && targetCount >= 0) {
                throw IllegalArgumentException(
                    "Target must be less than target count, $targetCount"
                )
            }
            targets[source] = target
        }

        fun setAll(mapping: Mapping) {
            for (pair in mapping) {
                set(pair.source, pair.target)
            }
        }

        /**
         * Returns the target that a source maps to, or -1 if it is not mapped.
         *
         * @return target
         */
        @Override
        override fun getTargetOpt(source: Int): Int {
            return targets[source]
        }
    }

    /**
     * Decorator which converts any [Mapping] into the inverse of itself.
     *
     *
     * If the mapping does not have an inverse -- for example, if a given
     * source can have more than one target -- then the corresponding method
     * call of the underlying mapping will raise a runtime exception.
     */
    private class InverseMapping internal constructor(parent: Mapping) : Mapping {
        val parent: Mapping

        init {
            this.parent = parent
        }

        @Override
        override operator fun iterator(): Iterator<IntPair> {
            val parentIter: Iterator<IntPair> = parent.iterator()
            return object : Iterator<IntPair?>() {
                @Override
                override fun hasNext(): Boolean {
                    return parentIter.hasNext()
                }

                @Override
                override fun next(): IntPair {
                    val parentPair: IntPair = parentIter.next()
                    return IntPair(parentPair.target, parentPair.source)
                }

                @Override
                fun remove() {
                    parentIter.remove()
                }
            }
        }

        @Override
        override fun clear() {
            parent.clear()
        }

        @Override
        override fun size(): Int {
            return parent.size()
        }

        @Override
        fun getSourceCount(): Int {
            return parent.getTargetCount()
        }

        @Override
        fun getTargetCount(): Int {
            return parent.getSourceCount()
        }

        @Override
        fun getMappingType(): MappingType {
            return parent.getMappingType().inverse()
        }

        @Override
        fun isIdentity(): Boolean {
            return parent.isIdentity()
        }

        @Override
        override fun getTargetOpt(source: Int): Int {
            return parent.getSourceOpt(source)
        }

        @Override
        override fun getTarget(source: Int): Int {
            return parent.getSource(source)
        }

        @Override
        override fun getSource(target: Int): Int {
            return parent.getTarget(target)
        }

        @Override
        override fun getSourceOpt(target: Int): Int {
            return parent.getTargetOpt(target)
        }

        @Override
        override fun inverse(): Mapping {
            return parent
        }

        @Override
        override operator fun set(source: Int, target: Int) {
            parent.set(target, source)
        }
    }
}
