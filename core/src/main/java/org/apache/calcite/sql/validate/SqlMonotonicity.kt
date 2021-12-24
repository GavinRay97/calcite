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
package org.apache.calcite.sql.validate

import Prepare.CatalogReader
import org.apache.calcite.sql.validate.SqlValidatorImpl.Expander
import SqlValidatorScope.Resolve
import kotlin.jvm.JvmOverloads
import org.apache.calcite.sql.validate.SqlValidatorScope.Path
import org.apache.calcite.sql.validate.SqlValidatorScope.EmptyPath
import org.apache.calcite.sql.validate.SqlValidatorScope.Resolve

/**
 * Enumeration of types of monotonicity.
 */
enum class SqlMonotonicity {
    STRICTLY_INCREASING, INCREASING, STRICTLY_DECREASING, DECREASING, CONSTANT,

    /**
     * Catch-all value for expressions that have some monotonic properties.
     * Maybe it isn't known whether the expression is increasing or decreasing;
     * or maybe the value is neither increasing nor decreasing but the value
     * never repeats.
     */
    MONOTONIC, NOT_MONOTONIC;

    /**
     * If this is a strict monotonicity (StrictlyIncreasing, StrictlyDecreasing)
     * returns the non-strict equivalent (Increasing, Decreasing).
     *
     * @return non-strict equivalent monotonicity
     */
    fun unstrict(): SqlMonotonicity {
        return when (this) {
            STRICTLY_INCREASING -> INCREASING
            STRICTLY_DECREASING -> DECREASING
            else -> this
        }
    }

    /**
     * Returns the reverse monotonicity.
     *
     * @return reverse monotonicity
     */
    fun reverse(): SqlMonotonicity {
        return when (this) {
            STRICTLY_INCREASING -> STRICTLY_DECREASING
            INCREASING -> DECREASING
            STRICTLY_DECREASING -> STRICTLY_INCREASING
            DECREASING -> INCREASING
            else -> this
        }
    }

    /**
     * Whether values of this monotonicity are decreasing. That is, if a value
     * at a given point in a sequence is X, no point later in the sequence will
     * have a value greater than X.
     *
     * @return whether values are decreasing
     */
    val isDecreasing: Boolean
        get() = when (this) {
            STRICTLY_DECREASING, DECREASING -> true
            else -> false
        }

    /**
     * Returns whether values of this monotonicity may ever repeat after moving
     * to another value: true for [.NOT_MONOTONIC] and [.CONSTANT],
     * false otherwise.
     *
     *
     * If a column is known not to repeat, a sort on that column can make
     * progress before all of the input has been seen.
     *
     * @return whether values repeat
     */
    fun mayRepeat(): Boolean {
        return when (this) {
            NOT_MONOTONIC, CONSTANT -> true
            else -> false
        }
    }
}
