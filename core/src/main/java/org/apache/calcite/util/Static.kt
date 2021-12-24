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

import org.apache.calcite.runtime.CalciteResource

/**
 * Definitions of objects to be statically imported.
 *
 * <h2>Note to developers</h2>
 *
 *
 * Please give careful consideration before including an object in this
 * class. Pros:
 *
 *  * Code that uses these objects will be terser.
 *
 *
 *
 * Cons:
 *
 *  * Namespace pollution,
 *  * code that is difficult to understand (a general problem with static
 * imports),
 *  * potential cyclic initialization.
 *
 */
object Static {
    /** Resources.  */
    val RESOURCE: CalciteResource = Resources.create(CalciteResource::class.java)

    /** Builds a list.  */
    fun <E> cons(first: E, rest: List<E>?): List<E> {
        return ConsList.of(first, rest)
    }
}
