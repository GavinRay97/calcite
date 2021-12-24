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

import org.apache.calcite.linq4j.function.Deterministic
import org.apache.calcite.linq4j.function.Parameter
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import java.util.Random

/**
 * Function object for `RAND` and `RAND_INTEGER`, with and without
 * seed.
 */
@SuppressWarnings("unused")
class RandomFunction
/** Creates a RandomFunction.
 *
 *
 * Marked deterministic so that the code generator instantiates one once
 * per query, not once per row.  */
@Deterministic constructor() {
    @MonotonicNonNull
    private var random: Random? = null

    /** Implements the `RAND()` SQL function.  */
    fun rand(): Double {
        if (random == null) {
            random = Random()
        }
        return random.nextDouble()
    }

    /** Implements the `RAND(seed)` SQL function.  */
    fun randSeed(@Parameter(name = "seed") seed: Int): Double {
        if (random == null) {
            random = Random(seed xor (seed shl 16))
        }
        return random.nextDouble()
    }

    /** Implements the `RAND_INTEGER(bound)` SQL function.  */
    fun randInteger(@Parameter(name = "bound") bound: Int): Int {
        if (random == null) {
            random = Random()
        }
        return random.nextInt(bound)
    }

    /** Implements the `RAND_INTEGER(seed, bound)` SQL function.  */
    fun randIntegerSeed(
        @Parameter(name = "seed") seed: Int,
        @Parameter(name = "bound") bound: Int
    ): Int {
        if (random == null) {
            random = Random(seed)
        }
        return random.nextInt(bound)
    }
}
