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

import java.util.AbstractSequentialList

/**
 * Implementation of list similar to [LinkedList], but stores elements
 * in chunks of 32 elements.
 *
 *
 * ArrayList has O(n) insertion and deletion into the middle of the list.
 * ChunkList insertion and deletion are O(1).
 *
 * @param <E> element type
</E> */
class ChunkList<E> : AbstractSequentialList<E> {
    private var size = 0
    private var first: @Nullable Array<E?>?
    private var last: @Nullable Array<E?>?

    /**
     * Creates an empty ChunkList.
     */
    constructor() {}

    /**
     * Creates a ChunkList whose contents are a given Collection.
     */
    constructor(collection: Collection<E>?) {
        @SuppressWarnings(["method.invocation.invalid", "unused"]) val ignore: Boolean = addAll(collection)
    }

    /**
     * For debugging and testing.
     */
    fun isValid(fail: Boolean): Boolean {
        if (first == null != (last == null)) {
            assert(!fail)
            return false
        }
        if (first == null != (size == 0)) {
            assert(!fail)
            return false
        }
        var n = 0
        for (@SuppressWarnings("unused") e in this) {
            if (n++ > size) {
                assert(!fail)
                return false
            }
        }
        if (n != size) {
            assert(!fail)
            return false
        }
        var prev: Array<E?>? = null
        var chunk = first
        while (chunk != null) {
            if (prev(chunk) != prev) {
                assert(!fail)
                return false
            }
            prev = chunk
            if (occupied(chunk) == 0) {
                assert(!fail)
                return false
            }
            chunk = next(chunk)
        }
        return true
    }

    @Override
    fun listIterator(index: Int): ListIterator<E> {
        return locate(index)
    }

    @Override
    fun size(): Int {
        return size
    }

    @Override
    fun clear() {
        // base class method works, but let's optimize
        size = 0
        last = null
        first = last
    }

    @Override
    fun add(element: E): Boolean {
        var chunk = last
        var occupied: Int
        if (chunk == null) {
            last = arrayOfNulls<Object>(CHUNK_SIZE + HEADER_SIZE) as Array<E?>?
            first = last
            chunk = first
            occupied = 0
        } else {
            occupied = occupied(chunk)
            if (occupied == CHUNK_SIZE) {
                chunk = arrayOfNulls<Object>(CHUNK_SIZE + HEADER_SIZE) as Array<E?>
                setNext(requireNonNull(last, "last"), chunk)
                setPrev<E?>(chunk, last)
                occupied = 0
                last = chunk
            }
        }
        setOccupied<E?>(chunk, occupied + 1)
        setElement(chunk, HEADER_SIZE + occupied, element)
        ++size
        return true
    }

    @Override
    fun add(index: Int, element: E) {
        if (index == size) {
            add(element)
        } else {
            super.add(index, element)
        }
    }

    private fun locate(index: Int): ChunkListIterator {
        if (index < 0 || index > size) {
            throw IndexOutOfBoundsException()
        }
        if (first == null) {
            // Create an iterator positioned before the first element.
            return ChunkListIterator(null, 0, 0, -1, 0)
        }
        var n = 0
        var chunk: Array<E?> = first
        while (true) {
            val occupied = occupied(chunk)
            val nextN = n + occupied
            val next = next(chunk)
            if (nextN >= index || next == null) {
                return ChunkListIterator(chunk, n, index, -1, n + occupied)
            }
            n = nextN
            chunk = next
        }
    }

    /** Iterator over a [ChunkList].  */
    private inner class ChunkListIterator internal constructor(
        chunk: @Nullable Array<E>?, start: Int, cursor: Int, lastRet: Int,
        end: Int
    ) : ListIterator<E> {
        private var chunk: @Nullable Array<E?>?

        /** Offset in the list of the first element of this chunk.  */
        private var start: Int

        /** Offset within current chunk of the next element to return.  */
        private var cursor: Int

        /** Offset within the current chunk of the last element returned. -1 if
         * [.next] or [.previous] has not been called.  */
        private var lastRet: Int

        /** Offset of the first unoccupied location in the current chunk.  */
        private var end: Int

        init {
            this.chunk = chunk
            this.start = start
            this.cursor = cursor
            this.lastRet = lastRet
            this.end = end
        }

        private fun currentChunk(): Array<E?> {
            return castNonNull(chunk)
        }

        @Override
        override fun hasNext(): Boolean {
            return cursor < size
        }

        @Override
        override fun next(): E? {
            if (cursor >= size) {
                throw NoSuchElementException()
            }
            if (cursor == end) {
                chunk = if (chunk == null) {
                    first
                } else {
                    next(chunk!!)
                }
                start = end
                end = if (chunk == null) {
                    start
                } else {
                    start + occupied(chunk!!)
                }
            }
            return element(
                currentChunk(),
                HEADER_SIZE + cursor++.also { lastRet = it } - start
            )
        }

        @Override
        override fun hasPrevious(): Boolean {
            return cursor > 0
        }

        @Override
        override fun previous(): E? {
            lastRet = cursor--
            if (cursor < start) {
                chunk = if (chunk == null) last else prev(chunk)
                if (chunk == null) {
                    throw NoSuchElementException()
                }
                val o = occupied(chunk!!)
                end = start
                start -= o
                assert(cursor == end - 1)
            }
            return element(currentChunk(), cursor - start)
        }

        @Override
        override fun nextIndex(): Int {
            return cursor
        }

        @Override
        override fun previousIndex(): Int {
            return cursor - 1
        }

        @Override
        fun remove() {
            if (lastRet < 0) {
                throw IllegalStateException()
            }
            --size
            --cursor
            if (end == start + 1) {
                // Chunk is now empty.
                val prev = prev(currentChunk())
                val next = next(currentChunk())
                if (next == null) {
                    last = prev
                    if (prev == null) {
                        first = null
                    } else {
                        setNext<E?>(prev, null)
                    }
                    chunk = null
                    end = HEADER_SIZE
                } else {
                    if (prev == null) {
                        first = next
                        chunk = first
                        setPrev<E?>(next, null)
                        end = occupied(requireNonNull(chunk, "chunk"))
                    } else {
                        setNext<E?>(prev, next)
                        setPrev<E?>(next, prev)
                        chunk = prev
                        end = start
                        start -= occupied(requireNonNull(chunk, "chunk"))
                    }
                }
                lastRet = -1
                return
            }
            val r = lastRet
            lastRet = -1
            if (r < start) {
                // Element we wish to eliminate is the last element in the previous
                // block.
                var c = chunk
                if (c == null) {
                    c = last
                }
                var o = occupied(castNonNull(c))
                if (o == 1) {
                    // Block is now empty; remove it
                    val prev = prev(c)
                    if (prev == null) {
                        if (chunk == null) {
                            last = null
                            first = last
                        } else {
                            first = chunk
                            setPrev(requireNonNull(chunk, "chunk"), null)
                        }
                    } else {
                        setNext(requireNonNull(prev, "prev"), chunk)
                        setPrev(requireNonNull(chunk, "chunk"), prev)
                    }
                } else {
                    --o
                    setElement(c, HEADER_SIZE + o, null) // allow gc
                    setOccupied<E?>(c, o)
                }
            } else {
                // Move existing contents down one.
                System.arraycopy(
                    currentChunk(), HEADER_SIZE + r - start + 1,
                    currentChunk(), HEADER_SIZE + r - start, end - r - 1
                )
                --end
                val o = end - start
                setElement(currentChunk(), HEADER_SIZE + o, null) // allow gc
                setOccupied<E?>(currentChunk(), o)
            }
        }

        @Override
        fun set(e: E) {
            if (lastRet < 0) {
                throw IllegalStateException()
            }
            var c: Array<E?>? = currentChunk()
            val p = lastRet
            var s = start
            if (p < start) {
                // The element is at the end of the previous chunk
                c = prev(c)
                s -= occupied(castNonNull(c))
            }
            setElement(c, HEADER_SIZE + p - s, e)
        }

        @Override
        fun add(e: E) {
            if (chunk == null) {
                val newChunk = arrayOfNulls<Object>(CHUNK_SIZE + HEADER_SIZE) as Array<E?>
                if (first != null) {
                    setNext<E?>(newChunk, first)
                    setPrev(requireNonNull(first, "first"), newChunk)
                }
                first = newChunk
                if (last == null) {
                    last = newChunk
                }
                chunk = newChunk
                end = start
            } else if (end == start + CHUNK_SIZE) {
                // FIXME We create a new chunk, but the next chunk might be
                // less than half full. We should consider using it.
                val newChunk = arrayOfNulls<Object>(CHUNK_SIZE + HEADER_SIZE) as Array<E?>
                val next = next(chunk!!)
                setPrev<E?>(newChunk, chunk)
                setNext(requireNonNull(chunk, "chunk"), newChunk)
                if (next == null) {
                    last = newChunk
                } else {
                    setPrev<E?>(next, newChunk)
                    setNext<E?>(newChunk, next)
                }
                setOccupied(requireNonNull(chunk, "chunk"), CHUNK_SIZE / 2)
                setOccupied<E?>(newChunk, CHUNK_SIZE / 2)
                System.arraycopy(
                    requireNonNull(chunk, "chunk"), HEADER_SIZE + CHUNK_SIZE / 2,
                    newChunk, HEADER_SIZE, CHUNK_SIZE / 2
                )
                Arrays.fill(
                    chunk, HEADER_SIZE + CHUNK_SIZE / 2,
                    HEADER_SIZE + CHUNK_SIZE, null
                )
                if (cursor - start < CHUNK_SIZE / 2) {
                    end -= CHUNK_SIZE / 2
                } else {
                    start += CHUNK_SIZE / 2
                    chunk = newChunk
                }
            }
            // Move existing contents up one.
            System.arraycopy(
                chunk, HEADER_SIZE + cursor - start,
                chunk, HEADER_SIZE + cursor - start + 1, end - cursor
            )
            ++end
            setElement(chunk, HEADER_SIZE + cursor - start, e)
            setOccupied<E?>(chunk, end - start)
            ++size
        }
    }

    companion object {
        private const val HEADER_SIZE = 3
        private const val CHUNK_SIZE = 64
        private val INTEGERS: Array<Integer?> = arrayOfNulls<Integer>(CHUNK_SIZE + 3)

        init {
            for (i in INTEGERS.indices) {
                INTEGERS[i] = i
            }
        }

        private fun <E> prev(chunk: Array<E>?): @Nullable Array<E>? {
            return chunk!![0] as Array<E>
        }

        private fun <E> setPrev(chunk: Array<E?>?, prev: @Nullable Array<E?>?) {
            chunk!![0] = prev as E?
        }

        private fun <E> next(chunk: Array<E>): @Nullable Array<E>? {
            return chunk[1] as Array<E>
        }

        private fun <E> setNext(chunk: Array<E?>, next: @Nullable Array<E?>?) {
            assert(chunk != next)
            chunk[1] = next as E?
        }

        private fun <E> occupied(chunk: Array<E>): Int {
            return requireNonNull(chunk[2], "chunk[2] (number of occupied entries)") as Integer
        }

        @SuppressWarnings("unchecked")
        private fun <E> setOccupied(chunk: Array<E?>?, size: Int) {
            chunk!![2] = INTEGERS[size] as E?
        }

        private fun <E> element(chunk: Array<E>, index: Int): E {
            return chunk[index]
        }

        private fun <E> setElement(chunk: Array<E>?, index: Int, @Nullable element: E) {
            chunk!![index] = castNonNull(element)
        }
    }
}
