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
package org.apache.calcite.rel.hint

import org.apache.calcite.rel.RelNode

/**
 * A [HintPredicate] to combine multiple hint predicates into one.
 *
 *
 * The composition can be `AND` or `OR`.
 */
class CompositeHintPredicate internal constructor(composition: Composition, vararg predicates: HintPredicate?) :
    HintPredicate {
    //~ Enums ------------------------------------------------------------------
    /** How hint predicates are composed.  */
    enum class Composition {
        AND, OR
    }

    //~ Instance fields --------------------------------------------------------
    private val predicates: ImmutableList<HintPredicate>
    private val composition: Composition

    /**
     * Creates a [CompositeHintPredicate] with a [Composition]
     * and an array of hint predicates.
     *
     *
     * Make this constructor package-protected intentionally.
     * Use utility methods in [HintPredicates]
     * to create a [CompositeHintPredicate].
     */
    init {
        assert(predicates != null)
        assert(predicates.size > 1)
        for (predicate in predicates) {
            assert(predicate != null)
        }
        this.predicates = ImmutableList.copyOf(predicates)
        this.composition = composition
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun apply(hint: RelHint, rel: RelNode): Boolean {
        return apply(composition, hint, rel)
    }

    private fun apply(composition: Composition, hint: RelHint, rel: RelNode): Boolean {
        return when (composition) {
            Composition.AND -> {
                for (predicate in predicates) {
                    if (!predicate.apply(hint, rel)) {
                        return false
                    }
                }
                true
            }
            Composition.OR -> {
                for (predicate in predicates) {
                    if (predicate.apply(hint, rel)) {
                        return true
                    }
                }
                false
            }
            else -> {
                for (predicate in predicates) {
                    if (predicate.apply(hint, rel)) {
                        return true
                    }
                }
                false
            }
        }
    }
}
