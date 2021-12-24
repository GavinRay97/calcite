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

import org.apache.calcite.DataContext

/**
 * Implementation of table that reads rows from column stores, one per column.
 * Column store formats are chosen based on the type and distribution of the
 * values in the column; see [Representation] and
 * [RepresentationType].
 */
internal class ArrayTable(
    elementType: Type?, protoRowType: RelProtoDataType,
    supplier: Supplier<Content?>
) : AbstractQueryableTable(elementType), ScannableTable {
    private val protoRowType: RelProtoDataType
    private val supplier: Supplier<Content>

    /** Creates an ArrayTable.  */
    init {
        this.protoRowType = protoRowType
        this.supplier = supplier
    }

    @Override
    fun getRowType(typeFactory: RelDataTypeFactory?): RelDataType {
        return protoRowType.apply(typeFactory)
    }

    @get:Override
    val statistic: Statistic
        get() {
            val keys: List<ImmutableBitSet> = ArrayList()
            val content: Content = supplier.get()
            for (ord in Ord.zip(content.columns)) {
                if (ord.e.cardinality === content.size) {
                    keys.add(ImmutableBitSet.of(ord.i))
                }
            }
            return Statistics.of(content.size, keys, content.collations)
        }

    @Override
    fun scan(root: DataContext?): Enumerable<Array<Object>> {
        return object : AbstractEnumerable<Array<Object?>?>() {
            @Override
            fun enumerator(): Enumerator<Array<Object>> {
                val content: Content = supplier.get()
                return content.arrayEnumerator()
            }
        }
    }

    @Override
    fun <T> asQueryable(
        queryProvider: QueryProvider?,
        schema: SchemaPlus?, tableName: String?
    ): Queryable<T> {
        return object : AbstractTableQueryable<T>(
            queryProvider, schema, this,
            tableName
        ) {
            @SuppressWarnings("unchecked")
            @Override
            fun enumerator(): Enumerator<T> {
                val content: Content = supplier.get()
                return content.enumerator<Any>()
            }
        }
    }

    /** How a column's values are represented.  */
    internal enum class RepresentationType {
        /** Constant. Contains only one value.
         *
         *
         * We can't store 0-bit values in
         * an array: we'd have no way of knowing how many there were.
         *
         * @see Constant
         */
        CONSTANT,

        /** Object array. Null values are represented by null. Values may or may
         * not be canonized; if canonized, = and != can be implemented using
         * pointer.
         *
         * @see ObjectArray
         */
        OBJECT_ARRAY,

        /**
         * Array of primitives. Null values not possible. Only for primitive
         * types (and not optimal for boolean).
         *
         * @see PrimitiveArray
         */
        PRIMITIVE_ARRAY,

        /** Bit-sliced primitive array. Values are `bitCount` bits each,
         * and interpreted as signed. Stored as an array of long values.
         *
         *
         * If gcd(bitCount, 64) != 0, some values will cross boundaries.
         * bits each. But for all of those values except 4, there is a primitive
         * type (8 byte, 16 short, 32 int) which is more efficient.
         *
         * @see BitSlicedPrimitiveArray
         */
        BIT_SLICED_PRIMITIVE_ARRAY,

        /**
         * Dictionary of primitives. Use one of the previous methods to store
         * unsigned offsets into the dictionary. Dictionary is canonized and
         * sorted, so v1 &lt; v2 if and only if code(v1) &lt; code(v2). The
         * dictionary may or may not contain a null value.
         *
         *
         * The dictionary is not beneficial unless the codes are
         * significantly shorter than the values. A column of `long`
         * values with many duplicates is a win; a column of mostly distinct
         * `short` values is likely a loss. The other win is if there are
         * null values; otherwise the best option would be an
         * [.OBJECT_ARRAY].
         *
         * @see PrimitiveDictionary
         */
        PRIMITIVE_DICTIONARY,

        /**
         * Dictionary of objects. Use one of the previous methods to store
         * unsigned offsets into the dictionary.
         *
         * @see ObjectDictionary
         */
        OBJECT_DICTIONARY,

        /**
         * Compressed string table. Block of char data. Strings represented
         * using an unsigned offset into the table (stored using one of the
         * previous methods).
         *
         *
         * First 2 bytes are unsigned length; subsequent bytes are string
         * contents. The null value, strings longer than 64k and strings that
         * occur very commonly are held in an 'exceptions' array and are
         * recognized by their high offsets. Other strings are created on demand
         * (this reduces the number of objects that need to be created during
         * deserialization from cache.
         *
         * @see StringDictionary
         */
        STRING_DICTIONARY,

        /**
         * Compressed byte array table. Similar to compressed string table.
         *
         * @see ByteStringDictionary
         */
        BYTE_STRING_DICTIONARY
    }

    /** Column definition and value set.  */
    class Column internal constructor(val representation: Representation, data: Object, cardinality: Int) {
        val dataSet: Object
        val cardinality: Int

        init {
            dataSet = data
            this.cardinality = cardinality
        }

        fun permute(sources: IntArray?): Column {
            return Column(
                representation,
                representation.permute(dataSet, sources),
                cardinality
            )
        }

        @Override
        override fun toString(): String {
            return ("Column(representation=" + representation
                    + ", value=" + representation.toString(dataSet) + ")")
        }

        companion object {
            /** Returns a list view onto a data set.  */
            fun asList(
                representation: Representation,
                dataSet: Object?
            ): List {
                // Cache size. It might be expensive to compute.
                val size = representation.size(dataSet)
                return object : AbstractList() {
                    @Override
                    @Nullable
                    operator fun get(index: Int): Object {
                        return representation.getObject(dataSet, index)
                    }

                    @Override
                    fun size(): Int {
                        return size
                    }
                }
            }
        }
    }

    /** Representation of the values of a column.  */
    interface Representation {
        /** Returns the representation type.  */
        val type: RepresentationType?

        /** Converts a value set into a compact representation. If
         * `sources` is not null, permutes.  */
        fun freeze(valueSet: ColumnLoader.ValueSet?, sources: @Nullable IntArray?): Object

        @Nullable
        fun getObject(dataSet: Object?, ordinal: Int): Object
        fun getInt(dataSet: Object?, ordinal: Int): Int

        /** Creates a data set that is the same as a given data set
         * but re-ordered.  */
        fun permute(dataSet: Object?, sources: IntArray?): Object

        /** Returns the number of elements in a data set. (Some representations
         * return the capacity, which may be slightly larger than the actual
         * size.)  */
        fun size(dataSet: Object?): Int

        /** Converts a data set to a string.  */
        fun toString(dataSet: Object?): String
    }

    /** Representation that stores the column values in an array.  */
    class ObjectArray internal constructor(val ordinal: Int) : Representation {
        @Override
        override fun toString(): String {
            return "ObjectArray(ordinal=$ordinal)"
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.OBJECT_ARRAY

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet, sources: @Nullable IntArray?): Object {
            // We assume the values have been canonized.
            val list: List<Comparable> = permuteList<Any>(valueSet.values, sources)
            return list.toArray(arrayOfNulls<Comparable>(0))
        }

        @Override
        override fun permute(dataSet: Object, sources: IntArray): Object {
            @Nullable val list: Array<Comparable> = dataSet
            val size = list.size
            @Nullable val comparables: Array<Comparable?> = arrayOfNulls<Comparable>(size)
            for (i in 0 until size) {
                comparables[i] = list[sources[i]]
            }
            return comparables
        }

        @Override
        @Nullable
        override fun getObject(dataSet: Object, ordinal: Int): Object? {
            return dataSet[ordinal]
        }

        @Override
        override fun getInt(dataSet: Object, ordinal: Int): Int {
            val value = getObject(dataSet, ordinal) as Number?
            return requireNonNull(value, "value").intValue()
        }

        @Override
        override fun size(dataSet: Object): Int {
            return (dataSet as Array<Comparable?>).size
        }

        @Override
        override fun toString(dataSet: Object?): String {
            return Arrays.toString(dataSet as Array<Comparable?>?)
        }
    }

    /** Representation that stores the values of a column in an array of
     * primitive values.  */
    class PrimitiveArray internal constructor(val ordinal: Int, primitive: Primitive, p: Primitive) : Representation {
        private val primitive: Primitive
        private val p: Primitive

        init {
            this.primitive = primitive
            this.p = p
        }

        @Override
        override fun toString(): String {
            return ("PrimitiveArray(ordinal=" + ordinal
                    + ", primitive=" + primitive
                    + ", p=" + p
                    + ")")
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.PRIMITIVE_ARRAY

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet, sources: @Nullable IntArray?): Object {
            return primitive.toArray2(
                permuteList(valueSet.values, sources)
            )
        }

        @Override
        override fun permute(dataSet: Object?, sources: IntArray?): Object {
            return primitive.permute(dataSet, sources)
        }

        @Override
        @Nullable
        override fun getObject(dataSet: Object?, ordinal: Int): Object {
            return p.arrayItem(dataSet, ordinal)
        }

        @Override
        override fun getInt(dataSet: Object?, ordinal: Int): Int {
            return Array.getInt(dataSet, ordinal)
        }

        @Override
        override fun size(dataSet: Object?): Int {
            return Array.getLength(dataSet)
        }

        @Override
        override fun toString(dataSet: Object?): String {
            return p.arrayToString(dataSet)
        }
    }

    /** Representation that stores column values in a dictionary of
     * primitive values, then uses a short code for each row.  */
    class PrimitiveDictionary internal constructor() : Representation {
        @Override
        override fun toString(): String {
            return "PrimitiveDictionary()"
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.PRIMITIVE_DICTIONARY

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet?, sources: @Nullable IntArray?): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun permute(dataSet: Object?, sources: IntArray?): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun getObject(dataSet: Object?, ordinal: Int): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun getInt(dataSet: Object?, ordinal: Int): Int {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun size(dataSet: Object?): Int {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun toString(dataSet: Object?): String {
            throw UnsupportedOperationException() // TODO:
        }
    }

    /** Representation that stores the values of a column as a
     * dictionary of objects.  */
    class ObjectDictionary internal constructor(
        val ordinal: Int,
        val representation: Representation
    ) : Representation {
        @Override
        override fun toString(): String {
            return ("ObjectDictionary(ordinal=" + ordinal
                    + ", representation=" + representation
                    + ")")
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.OBJECT_DICTIONARY

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet, sources: @Nullable IntArray?): Object {
            val n: Int = valueSet.map.keySet().size()
            val extra = if (valueSet.containsNull) 1 else 0
            @SuppressWarnings("all") @Nullable val codeValues: Array<Comparable> =
                valueSet.map.keySet().toArray(arrayOfNulls<Comparable>(n + extra))
            // codeValues[0..n] is non-null since valueSet.map.keySet is non-null
            // There might be null at the very end, however, it won't participate in Arrays.sort
            @SuppressWarnings("assignment.type.incompatible") val nonNullCodeValues: Array<Comparable> = codeValues
            Arrays.sort(nonNullCodeValues, 0, n)
            val codeValueSet: ColumnLoader.ValueSet = ValueSet(Int::class.javaPrimitiveType)
            val list: List<Comparable> = permuteList<Any>(valueSet.values, sources)
            for (value in list) {
                var code: Int
                if (value == null) {
                    code = n
                } else {
                    code = Arrays.binarySearch(codeValues, value)
                    assert(code >= 0) { "$code, $value" }
                }
                codeValueSet.add(code)
            }
            val codes: Object = representation.freeze(codeValueSet, null)
            return Pair.of(codes, codeValues)
        }

        @Override
        override fun permute(dataSet: Object, sources: IntArray?): Object {
            val pair: Pair<Object, Array<Comparable>> = unfreeze(dataSet)
            val codes: Object = pair.left
            @Nullable val codeValues: Array<Comparable> = pair.right
            return Pair.of(representation.permute(codes, sources), codeValues)
        }

        @Override
        @Nullable
        override fun getObject(dataSet: Object, ordinal: Int): Object {
            val pair: Pair<Object, Array<Comparable>> = unfreeze(dataSet)
            val code = representation.getInt(pair.left, ordinal)
            return pair.right.get(code)
        }

        @Override
        override fun getInt(dataSet: Object, ordinal: Int): Int {
            val value = getObject(dataSet, ordinal) as Number
            return requireNonNull(value, "value").intValue()
        }

        @Override
        override fun size(dataSet: Object): Int {
            val pair: Pair<Object, Array<Comparable>> = unfreeze(dataSet)
            return representation.size(pair.left)
        }

        @Override
        override fun toString(dataSet: Object?): String {
            return Column.asList(this, dataSet).toString()
        }

        companion object {
            private fun unfreeze(value: Object): Pair<Object, Array<Comparable>> {
                return value as Pair<Object, Array<Comparable>>
            }
        }
    }

    /** Representation that stores string column values.  */
    class StringDictionary internal constructor() : Representation {
        @Override
        override fun toString(): String {
            return "StringDictionary()"
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.STRING_DICTIONARY

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet?, sources: @Nullable IntArray?): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun permute(dataSet: Object?, sources: IntArray?): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun getObject(dataSet: Object?, ordinal: Int): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun getInt(dataSet: Object?, ordinal: Int): Int {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun size(dataSet: Object?): Int {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun toString(dataSet: Object?): String {
            return Column.asList(this, dataSet).toString()
        }
    }

    /** Representation that stores byte-string column values.  */
    class ByteStringDictionary internal constructor() : Representation {
        @Override
        override fun toString(): String {
            return "ByteStringDictionary()"
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.BYTE_STRING_DICTIONARY

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet?, sources: @Nullable IntArray?): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun permute(dataSet: Object?, sources: IntArray?): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun getObject(dataSet: Object?, ordinal: Int): Object {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun getInt(dataSet: Object?, ordinal: Int): Int {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun size(dataSet: Object?): Int {
            throw UnsupportedOperationException() // TODO:
        }

        @Override
        override fun toString(dataSet: Object?): String {
            return Column.asList(this, dataSet).toString()
        }
    }

    /** Representation of a column that has the same value for every row.  */
    class Constant internal constructor(val ordinal: Int) : Representation {
        @Override
        override fun toString(): String {
            return "Constant(ordinal=$ordinal)"
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.CONSTANT

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet, sources: @Nullable IntArray?): Object {
            val size: Int = valueSet.values.size()
            return Pair.of(if (size == 0) null else valueSet.values.get(0), size)
        }

        @Override
        override fun permute(dataSet: Object, sources: IntArray?): Object {
            return dataSet
        }

        @Override
        @Nullable
        override fun getObject(dataSet: Object, ordinal: Int): Object {
            val pair: Pair<Object, Integer> = unfreeze(dataSet)
            return pair.left
        }

        @Override
        override fun getInt(dataSet: Object, ordinal: Int): Int {
            @Nullable val value = getObject(dataSet, ordinal) as Number
            return requireNonNull(value, "value").intValue()
        }

        @Override
        override fun size(dataSet: Object): Int {
            val pair: Pair<Object, Integer> = unfreeze(dataSet)
            return pair.right
        }

        @Override
        override fun toString(dataSet: Object): String {
            val pair: Pair<Object, Integer> = unfreeze(dataSet)
            return Collections.nCopies(pair.right, pair.left).toString()
        }

        companion object {
            private fun unfreeze(value: Object): Pair<Object, Integer> {
                return value as Pair<Object, Integer>
            }
        }
    }

    /** Representation that stores numeric values in a bit-sliced
     * array. Each value does not necessarily occupy 8, 16, 32 or 64
     * bits (the number of bits used by the built-in types). This
     * representation is often used to store the value codes for a
     * dictionary-based representation.  */
    class BitSlicedPrimitiveArray internal constructor(
        ordinal: Int, bitCount: Int, primitive: Primitive, signed: Boolean
    ) : Representation {
        val ordinal: Int
        val bitCount: Int
        val primitive: Primitive
        val signed: Boolean

        init {
            assert(bitCount > 0)
            this.ordinal = ordinal
            this.bitCount = bitCount
            this.primitive = primitive
            this.signed = signed
        }

        @Override
        override fun toString(): String {
            return ("BitSlicedPrimitiveArray(ordinal=" + ordinal
                    + ", bitCount=" + bitCount
                    + ", primitive=" + primitive
                    + ", signed=" + signed + ")")
        }

        @get:Override
        override val type: RepresentationType?
            get() = RepresentationType.BIT_SLICED_PRIMITIVE_ARRAY

        @Override
        override fun freeze(valueSet: ColumnLoader.ValueSet, sources: @Nullable IntArray?): Object {
            val chunksPerWord = 64 / bitCount
            val valueList: List<Comparable> = permuteList<Any>(valueSet.values, sources)
            val valueCount: Int = valueList.size()
            val wordCount = (valueCount + (chunksPerWord - 1)) / chunksPerWord
            val remainingChunkCount = valueCount % chunksPerWord
            val longs = LongArray(wordCount)
            val n = valueCount / chunksPerWord
            var i: Int
            var k = 0
            if (valueCount > 0
                && valueList[0] is Boolean
            ) {
                @SuppressWarnings("unchecked") val booleans: List<Boolean> = valueList
                i = 0
                while (i < n) {
                    var v: Long = 0
                    for (j in 0 until chunksPerWord) {
                        v = v or (if (booleans[k++]) 1 shl bitCount * j else 0).toLong()
                    }
                    longs[i] = v
                    i++
                }
                if (remainingChunkCount > 0) {
                    var v: Long = 0
                    for (j in 0 until remainingChunkCount) {
                        v = v or (if (booleans[k++]) 1 shl bitCount * j else 0).toLong()
                    }
                    longs[i] = v
                }
            } else {
                @SuppressWarnings("unchecked") val numbers: List<Number> = valueList
                i = 0
                while (i < n) {
                    var v: Long = 0
                    for (j in 0 until chunksPerWord) {
                        v = v or (numbers[k++].longValue() shl bitCount * j)
                    }
                    longs[i] = v
                    i++
                }
                if (remainingChunkCount > 0) {
                    var v: Long = 0
                    for (j in 0 until remainingChunkCount) {
                        v = v or (numbers[k++].longValue() shl bitCount * j)
                    }
                    longs[i] = v
                }
            }
            return longs
        }

        @Override
        override fun permute(dataSet: Object, sources: IntArray): Object {
            val longs0 = dataSet as LongArray
            val n = sources.size
            val longs = LongArray(longs0.size)
            for (i in 0 until n) {
                orLong(
                    bitCount, longs, i,
                    getLong(bitCount, longs0, sources[i])
                )
            }
            return longs
        }

        @Override
        override fun getObject(dataSet: Object, ordinal: Int): Object {
            val longs = dataSet as LongArray
            val chunksPerWord = 64 / bitCount
            val word = ordinal / chunksPerWord
            val v = longs[word]
            val chunk = ordinal % chunksPerWord
            val mask = (1 shl bitCount) - 1
            val signMask = 1 shl bitCount - 1
            val shift = chunk * bitCount
            val w = v shr shift
            var x = w and mask.toLong()
            if (signed && x and signMask.toLong() != 0L) {
                x = -x
            }
            return when (primitive) {
                BOOLEAN -> x != 0L
                BYTE -> x.toByte()
                CHAR -> Char(x.toUShort())
                SHORT -> x.toShort()
                INT -> x.toInt()
                LONG -> x
                else -> throw AssertionError(primitive.toString() + " unexpected")
            }
        }

        @Override
        override fun getInt(dataSet: Object, ordinal: Int): Int {
            val longs = dataSet as LongArray
            val chunksPerWord = 64 / bitCount
            val word = ordinal / chunksPerWord
            val v = longs[word]
            val chunk = ordinal % chunksPerWord
            val mask = (1 shl bitCount) - 1
            val signMask = 1 shl bitCount - 1
            val shift = chunk * bitCount
            val w = v shr shift
            var x = w and mask.toLong()
            if (signed && x and signMask.toLong() != 0L) {
                x = -x
            }
            return x.toInt()
        }

        @Override
        override fun size(dataSet: Object): Int {
            val longs = dataSet as LongArray
            val chunksPerWord = 64 / bitCount
            return longs.size * chunksPerWord // may be slightly too high
        }

        @Override
        override fun toString(dataSet: Object?): String {
            return Column.asList(this, dataSet).toString()
        }

        companion object {
            fun getLong(bitCount: Int, values: LongArray, ordinal: Int): Long {
                return getLong(
                    bitCount, 64 / bitCount, (1L shl bitCount) - 1L,
                    values, ordinal
                )
            }

            fun getLong(
                bitCount: Int,
                chunksPerWord: Int,
                mask: Long,
                values: LongArray,
                ordinal: Int
            ): Long {
                val word = ordinal / chunksPerWord
                val chunk = ordinal % chunksPerWord
                val value = values[word]
                val shift = chunk * bitCount
                return value shr shift and mask
            }

            fun orLong(
                bitCount: Int, values: LongArray, ordinal: Int, value: Long
            ) {
                orLong(bitCount, 64 / bitCount, values, ordinal, value)
            }

            fun orLong(
                bitCount: Int, chunksPerWord: Int, values: LongArray, ordinal: Int,
                value: Long
            ) {
                val word = ordinal / chunksPerWord
                val chunk = ordinal % chunksPerWord
                val shift = chunk * bitCount
                values[word] = values[word] or (value shl shift)
            }
        }
    }

    /** Contents of a table.  */
    class Content internal constructor(
        columns: List<Column?>?, size: Int,
        collations: Iterable<RelCollation?>?
    ) {
        val columns: List<Column>
        val size: Int
        val collations: ImmutableList<RelCollation>

        init {
            this.columns = ImmutableList.copyOf(columns)
            this.size = size
            this.collations = ImmutableList.copyOf(collations)
        }

        @Deprecated
        internal constructor(columns: List<Column?>?, size: Int, sortField: Int) : this(
            columns, size,
            if (sortField >= 0) RelCollations.createSingleton(sortField) else ImmutableList.of()
        ) {
        }

        @SuppressWarnings("unchecked")
        fun <T> enumerator(): Enumerator<T> {
            return if (columns.size() === 1) {
                ObjectEnumerator(size, columns[0]) as Enumerator<T>
            } else {
                ArrayEnumerator(size, columns) as Enumerator<T>
            }
        }

        fun arrayEnumerator(): Enumerator<Array<Object>> {
            return ArrayEnumerator(size, columns)
        }

        /** Enumerator over a table with a single column; each element
         * returned is an object.  */
        private class ObjectEnumerator internal constructor(val rowCount: Int, column: Column) : Enumerator<Object?> {
            val dataSet: Object
            val representation: Representation
            var i = -1

            init {
                dataSet = column.dataSet
                representation = column.representation
            }

            @Override
            @Nullable
            fun current(): Object {
                return representation.getObject(dataSet, i)
            }

            @Override
            fun moveNext(): Boolean {
                return ++i < rowCount
            }

            @Override
            fun reset() {
                i = -1
            }

            @Override
            fun close() {
            }
        }

        /** Enumerator over a table with more than one column; each element
         * returned is an array.  */
        private class ArrayEnumerator internal constructor(val rowCount: Int, val columns: List<Column>) :
            Enumerator<Array<Object?>?> {
            var i = -1
            @Override
            @Nullable
            fun current(): Array<Object?> {
                @Nullable val objects: Array<Object?> = arrayOfNulls<Object>(columns.size())
                for (j in objects.indices) {
                    val pair = columns[j]
                    objects[j] = pair.representation.getObject(pair.dataSet, i)
                }
                return objects
            }

            @Override
            fun moveNext(): Boolean {
                return ++i < rowCount
            }

            @Override
            fun reset() {
                i = -1
            }

            @Override
            fun close() {
            }
        }
    }

    companion object {
        private fun <E> permuteList(
            list: List<E>, sources: @Nullable IntArray?
        ): List<E> {
            return if (sources == null) {
                list
            } else object : AbstractList<E>() {
                @Override
                operator fun get(index: Int): E {
                    return list[sources[index]]
                }

                @Override
                fun size(): Int {
                    return list.size()
                }
            }
        }
    }
}
