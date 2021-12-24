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

import org.apache.calcite.linq4j.Enumerator

/**
 * Implementation of [org.apache.calcite.avatica.util.Cursor] on top of an
 * [org.apache.calcite.linq4j.Enumerator] that
 * returns a record for each row. The record is a synthetic class whose fields
 * are all public.
 *
 * @param <E> Element type
</E> */
class RecordEnumeratorCursor<E>(
    enumerator: Enumerator<E>?,
    clazz: Class<E>
) : EnumeratorCursor<E>(enumerator) {
    private val clazz: Class<E>

    /**
     * Creates a RecordEnumeratorCursor.
     *
     * @param enumerator Enumerator
     * @param clazz Element type
     */
    init {
        this.clazz = clazz
    }

    @Override
    protected fun createGetter(ordinal: Int): Getter {
        return FieldGetter(clazz.getFields().get(ordinal))
    }
}
