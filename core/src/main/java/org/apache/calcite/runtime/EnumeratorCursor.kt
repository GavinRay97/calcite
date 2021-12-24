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
package org.apache.calcite.runtime

import org.apache.calcite.avatica.util.PositionedCursor
import org.apache.calcite.linq4j.Enumerator

/**
 * Implementation of [org.apache.calcite.avatica.util.Cursor] on top of an
 * [org.apache.calcite.linq4j.Enumerator] that
 * returns a record for each row. The returned record is cached to avoid
 * multiple computations of current row.
 * For instance,
 * [org.apache.calcite.adapter.enumerable.EnumerableCalc]
 * computes result just in `current()` method, thus it makes sense to
 * cache the result and make it available for all the accessors.
 *
 * @param <T> Element type
</T> */
abstract class EnumeratorCursor<T> protected constructor(enumerator: Enumerator<T>) : PositionedCursor<T>() {
    private val enumerator: Enumerator<T>

    /**
     * Creates an `EnumeratorCursor`.
     *
     * @param enumerator input enumerator
     */
    init {
        this.enumerator = enumerator
    }

    @Override
    protected fun current(): T {
        return enumerator.current()
    }

    @Override
    operator fun next(): Boolean {
        return enumerator.moveNext()
    }

    @Override
    fun close() {
        enumerator.close()
    }
}
