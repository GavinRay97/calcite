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
package org.apache.calcite.adapter.clone

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Column loader.
 *
 * @param <T> Element type of source table
</T> */
internal class ColumnLoader<T> @SuppressWarnings("method.invocation.invalid") constructor(
    typeFactory: JavaTypeFactory,
    sourceTable: Enumerable<T>,
    protoRowType: RelProtoDataType,
    @Nullable repList: List<ColumnMetaData.Rep?>?
) {
    val list: List<T> = ArrayList()
    val representationValues: List<ArrayTable.Column> = ArrayList()
    private val typeFactory: JavaTypeFactory
    val sortField: Int

    /** Creates a column loader, and performs the load.
     *
     * @param typeFactory Type factory
     * @param sourceTable Source data
     * @param protoRowType Logical row type
     * @param repList Physical row types, or null if not known
     */
    init {
        var repList: List<ColumnMetaData.Rep?>? = repList
        this.typeFactory = typeFactory
        val rowType: RelDataType = protoRowType.apply(typeFactory)
        if (repList == null) {
            repList = Collections.nCopies(
                rowType.getFieldCount(),
                ColumnMetaData.Rep.OBJECT
            )
        }
        sourceTable.into(list)
        val sorts = intArrayOf(-1)
        load(rowType, repList, sorts)
        sortField = sorts[0]
    }

    fun size(): Int {
        return list.size()
    }

    private fun load(
        elementType: RelDataType,
        repList: List<ColumnMetaData.Rep?>, sort: IntArray?
    ) {
        val types: List<Type> = object : AbstractList<Type?>() {
            val fields: List<RelDataTypeField> = elementType.getFieldList()

            @Override
            operator fun get(index: Int): Type {
                return typeFactory.getJavaClass(
                    fields[index].getType()
                )
            }

            @Override
            fun size(): Int {
                return fields.size()
            }
        }
        var sources: IntArray? = null
        for (pair in Ord.zip(types)) {
            @SuppressWarnings("unchecked") val sliceList: List<*> =
                if (types.size() === 1) list else object : AbstractList<Object?>() {
                    val slice: Int = pair.i

                    @Override
                    operator fun get(index: Int): Object? {
                        val row: T = requireNonNull(list[index]) { "null value at index $index" }
                        return (row as Array<Object?>)[slice]
                    }

                    @Override
                    fun size(): Int {
                        return list.size()
                    }
                }
            val list2: List<*> = wrap(
                repList[pair.i],
                sliceList,
                elementType.getFieldList().get(pair.i).getType()
            )
            val clazz: Class = if (pair.e is Class) pair.e as Class else Object::class.java
            val valueSet = ValueSet(clazz)
            for (o in list2) {
                valueSet.add(o as Comparable)
            }
            if (sort != null && sort[0] < 0 && valueSet.map.keySet().size() === list.size()) {
                // We have discovered a the first unique key in the table.
                sort[0] = pair.i
                // map.keySet().size() == list.size() above implies list contains only non-null elements
                @SuppressWarnings("assignment.type.incompatible") val values: Array<Comparable> =
                    valueSet.values.toArray(arrayOfNulls<Comparable>(0))
                val kevs = arrayOfNulls<Kev>(list.size())
                for (i in kevs.indices) {
                    kevs[i] = Kev(i, values[i])
                }
                Arrays.sort(kevs)
                sources = IntArray(list.size())
                for (i in sources.indices) {
                    sources[i] = kevs[i]!!.source
                }
                if (isIdentity(sources)) {
                    // Table was already sorted. Clear the permutation.
                    // We've already set sort[0], so we won't check for another
                    // sorted column.
                    sources = null
                } else {
                    // Re-sort all previous columns.
                    for (i in 0 until pair.i) {
                        representationValues.set(
                            i, representationValues[i].permute(sources)
                        )
                    }
                }
            }
            representationValues.add(valueSet.freeze(pair.i, sources))
        }
    }

    /**
     * Set of values of a column, created during the load process, and converted
     * to a serializable (and more compact) form before load completes.
     */
    internal class ValueSet(clazz: Class) {
        val clazz: Class
        val map: Map<Comparable, Comparable> = HashMap()
        val values: List<Comparable> = ArrayList()

        @Nullable
        var min: Comparable? = null

        @Nullable
        var max: Comparable? = null
        var containsNull = false

        init {
            this.clazz = clazz
        }

        fun add(@Nullable e: Comparable?) {
            var e: Comparable? = e
            if (e != null) {
                val old: Comparable = e
                e = map[e]
                if (e == null) {
                    e = old
                    map.put(e, e)
                    if (min == null || min.compareTo(e) > 0) {
                        min = e
                    }
                    if (max == null || max.compareTo(e) < 0) {
                        max = e
                    }
                }
            } else {
                containsNull = true
            }
            values.add(e)
        }

        /** Freezes the contents of this value set into a column, optionally
         * re-ordering if `sources` is specified.  */
        fun freeze(ordinal: Int, sources: @Nullable IntArray?): ArrayTable.Column {
            val representation: ArrayTable.Representation = chooseRep(ordinal)
            val cardinality: Int = map.size() + if (containsNull) 1 else 0
            val data: Object = representation.freeze(this, sources)
            return Column(representation, data, cardinality)
        }

        fun chooseRep(ordinal: Int): ArrayTable.Representation {
            val primitive: Primitive = Primitive.of(clazz)
            val boxPrimitive: Primitive = Primitive.ofBox(clazz)
            val p: Primitive = if (primitive != null) primitive else boxPrimitive
            if (!containsNull && p != null) {
                when (p) {
                    FLOAT, DOUBLE -> return PrimitiveArray(ordinal, p, p)
                    OTHER, VOID -> throw AssertionError("wtf?!")
                    else -> {}
                }
                val min: Comparable? = min
                val max: Comparable? = max
                if (canBeLong(min) && canBeLong(max)) {
                    return chooseFixedRep(
                        ordinal, p, toLong(min), toLong(max)
                    )
                }
            }

            // We don't want to use a dictionary if:
            // (a) there are so many values that an object pointer (with one
            //     indirection) has about as many bits as a code (with two
            //     indirections); or
            // (b) if there are very few copies of each value.
            // The condition kind of captures this, but needs to be tuned.
            val codeCount: Int = map.size() + if (containsNull) 1 else 0
            val codeBitCount = log2(nextPowerOf2(codeCount))
            if (codeBitCount < 10 && values.size() > 2000) {
                val representation: ArrayTable.Representation =
                    chooseFixedRep(-1, Primitive.INT, 0, (codeCount - 1).toLong())
                return ObjectDictionary(ordinal, representation)
            }
            return ObjectArray(ordinal)
        }

        companion object {
            private fun toLong(o: Object): Long {
                // We treat Boolean and Character as if they were subclasses of
                // Number but actually they are not.
                return if (o is Boolean) {
                    if (o) 1 else 0
                } else if (o is Character) {
                    o as Character
                } else {
                    (o as Number).longValue()
                }
            }

            @EnsuresNonNullIf(result = true, expression = "#1")
            private fun canBeLong(@Nullable o: Object): Boolean {
                return (o is Boolean
                        || o is Character
                        || o is Number)
            }

            /** Chooses a representation for a fixed-precision primitive type
             * (boolean, byte, char, short, int, long).
             *
             * @param ordinal Ordinal of this column in table
             * @param p Type that values are to be returned as (not necessarily the
             * same as they will be stored)
             * @param min Minimum value to be encoded
             * @param max Maximum value to be encoded (inclusive)
             */
            private fun chooseFixedRep(
                ordinal: Int, p: Primitive, min: Long, max: Long
            ): ArrayTable.Representation {
                if (min == max) {
                    return Constant(ordinal)
                }
                val bitCountMax = log2(nextPowerOf2(abs2(max) + 1))
                var bitCount: Int // 1 for sign
                var signed: Boolean
                if (min >= 0) {
                    signed = false
                    bitCount = bitCountMax
                } else {
                    signed = true
                    val bitCountMin = log2(nextPowerOf2(abs2(min) + 1))
                    bitCount = Math.max(bitCountMin, bitCountMax) + 1
                }

                // Must be a fixed point primitive.
                if (bitCount > 21 && bitCount < 32) {
                    // Can't get more than 2 into a word.
                    signed = true
                    bitCount = 32
                }
                if (bitCount >= 33 && bitCount < 64) {
                    // Can't get more than one into a word.
                    signed = true
                    bitCount = 64
                }
                if (signed) {
                    when (bitCount) {
                        8 -> return PrimitiveArray(
                            ordinal, Primitive.BYTE, p
                        )
                        16 -> return PrimitiveArray(
                            ordinal, Primitive.SHORT, p
                        )
                        32 -> return PrimitiveArray(
                            ordinal, Primitive.INT, p
                        )
                        64 -> return PrimitiveArray(
                            ordinal, Primitive.LONG, p
                        )
                        else -> {}
                    }
                }
                return BitSlicedPrimitiveArray(
                    ordinal, bitCount, p, signed
                )
            }

            /** Two's complement absolute on int value.  */
            @SuppressWarnings("unused")
            private fun abs2(v: Int): Int {
                // -128 becomes +127
                return if (v < 0) v.inv() else v
            }

            /** Two's complement absolute on long value.  */
            private fun abs2(v: Long): Long {
                // -128 becomes +127
                return if (v < 0) v.inv() else v
            }
        }
    }

    /** Key-value pair.  */
    private class Kev internal constructor(val source: Int, key: Comparable) : Comparable<Kev?> {
        private val key: Comparable

        init {
            this.key = key
        }

        @Override
        operator fun compareTo(o: Kev): Int {
            return key.compareTo(o.key)
        }
    }

    companion object {
        val INT_B = intArrayOf(0x2, 0xC, 0xF0, 0xFF00, -0x10000)
        val INT_S = intArrayOf(1, 2, 4, 8, 16)
        val LONG_B = longArrayOf(
            0x2, 0xC, 0xF0, 0xFF00, -0x10000, -0x100000000L
        )
        val LONG_S = intArrayOf(1, 2, 4, 8, 16, 32)
        fun nextPowerOf2(v: Int): Int {
            var v = v
            v--
            v = v or (v ushr 1)
            v = v or (v ushr 2)
            v = v or (v ushr 4)
            v = v or (v ushr 8)
            v = v or (v ushr 16)
            v++
            return v
        }

        fun nextPowerOf2(v: Long): Long {
            var v = v
            v--
            v = v or (v ushr 1)
            v = v or (v ushr 2)
            v = v or (v ushr 4)
            v = v or (v ushr 8)
            v = v or (v ushr 16)
            v = v or (v ushr 32)
            v++
            return v
        }

        fun log2(v: Int): Int {
            var v = v
            var r = 0
            for (i in 4 downTo 0) {
                if (v and INT_B[i] != 0) {
                    v = v shr INT_S[i]
                    r = r or INT_S[i]
                }
            }
            return r
        }

        fun log2(v: Long): Int {
            var v = v
            var r = 0
            for (i in 5 downTo 0) {
                if (v and LONG_B[i] != 0L) {
                    v = v shr LONG_S[i]
                    r = r or LONG_S[i]
                }
            }
            return r
        }

        fun invert(targets: IntArray): IntArray {
            val sources = IntArray(targets.size)
            for (i in targets.indices) {
                sources[targets[i]] = i
            }
            return sources
        }

        fun isIdentity(sources: IntArray): Boolean {
            for (i in sources.indices) {
                if (sources[i] != i) {
                    return false
                }
            }
            return true
        }

        /** Adapt for some types that we represent differently internally than their
         * JDBC types. [java.sql.Timestamp] values that are not null are
         * converted to `long`, but nullable timestamps are acquired using
         * [java.sql.ResultSet.getObject] and therefore the Timestamp
         * value needs to be converted to a [Long]. Similarly
         * [java.sql.Date] and [java.sql.Time] values to
         * [Integer].  */
        private fun wrap(
            rep: ColumnMetaData.Rep?, list: List<*>,
            type: RelDataType
        ): List<Object?> {
            if (true) {
                return list
            }
            when (type.getSqlTypeName()) {
                TIMESTAMP -> when (rep) {
                    OBJECT, JAVA_SQL_TIMESTAMP -> {
                        return Util.transform(
                            list as List<Timestamp?>
                        ) { t: Timestamp? -> if (t == null) null else t.getTime() }
                    }
                    else -> {}
                }
                TIME -> when (rep) {
                    OBJECT, JAVA_SQL_TIME -> return Util.< Time
                        ,Integer > transform<Time, Integer>(
                        list as List<Time?>
                    ) { t: Time? -> if (t == null) null else (t.getTime() % DateTimeUtils.MILLIS_PER_DAY) }
                    else -> {}
                }
                DATE -> when (rep) {
                    OBJECT, JAVA_SQL_DATE -> return Util.< Date
                        ,Integer > transform<Date, Integer>(
                        list as List<Date?>
                    ) { d: Date? -> if (d == null) null else (d.getTime() / DateTimeUtils.MILLIS_PER_DAY) }
                    else -> {}
                }
                else -> {}
            }
            return list
        }
    }
}
