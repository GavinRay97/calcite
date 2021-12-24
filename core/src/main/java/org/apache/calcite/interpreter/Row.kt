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
package org.apache.calcite.interpreter

import java.util.Arrays

/**
 * Row.
 */
class Row internal constructor(@Nullable values: Array<Object?>) {
    @Nullable
    private val values: Array<Object?>

    /** Creates a Row.  */ // must stay package-protected, because does not copy
    init {
        this.values = values
    }

    @Override
    override fun hashCode(): Int {
        return Arrays.hashCode(values)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || obj is Row
                && Arrays.equals(values, (obj as Row).values))
    }

    @Override
    override fun toString(): String {
        return Arrays.toString(values)
    }

    @Nullable
    fun getObject(index: Int): Object? {
        return values[index]
    }

    // must stay package-protected
    @Nullable
    fun getValues(): Array<Object?> {
        return values
    }

    /** Returns a copy of the values.  */
    @Nullable
    fun copyValues(): Array<Object> {
        return values.clone()
    }

    fun size(): Int {
        return values.size
    }

    /**
     * Utility class to build row objects.
     */
    class RowBuilder(size: Int) {
        @Nullable
        var values: Array<Object?>

        init {
            values = arrayOfNulls<Object>(size)
        }

        /**
         * Sets the value of a particular column.
         *
         * @param index Zero-indexed position of value.
         * @param value Desired column value.
         */
        operator fun set(index: Int, @Nullable value: Object?) {
            values[index] = value
        }

        /** Returns a Row.  */
        fun build(): Row {
            return Row(values)
        }

        /** Allocates a new internal array.  */
        fun reset() {
            values = arrayOfNulls<Object>(values.size)
        }

        fun size(): Int {
            return values.size
        }
    }

    companion object {
        /** Creates a Row.
         *
         *
         * Makes a defensive copy of the array, so the Row is immutable.
         * (If you're worried about the extra copy, call [.of].
         * But the JIT probably avoids the copy.)
         */
        fun asCopy(@Nullable vararg values: Object?): Row {
            return Row(values.clone())
        }

        /** Creates a Row with one column value.  */
        fun of(@Nullable value0: Object?): Row {
            return Row(arrayOf<Object?>(value0))
        }

        /** Creates a Row with two column values.  */
        fun of(@Nullable value0: Object?, @Nullable value1: Object?): Row {
            return Row(arrayOf<Object?>(value0, value1))
        }

        /** Creates a Row with three column values.  */
        fun of(@Nullable value0: Object?, @Nullable value1: Object?, @Nullable value2: Object?): Row {
            return Row(arrayOf<Object?>(value0, value1, value2))
        }

        /** Creates a Row with variable number of values.  */
        fun of(@Nullable vararg values: Object?): Row {
            return Row(values)
        }

        /**
         * Create a RowBuilder object that eases creation of a new row.
         *
         * @param size Number of columns in output data.
         * @return New RowBuilder object.
         */
        fun newBuilder(size: Int): RowBuilder {
            return RowBuilder(size)
        }
    }
}
