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
package org.apache.calcite.rel.core

import com.google.common.collect.ImmutableSet

/**
 * Describes the necessary parameters for an implementation in order to
 * identify and set dynamic variables.
 */
class CorrelationId
/**
 * Creates a correlation identifier.
 */ private constructor(
    /**
     * Returns the identifier.
     *
     * @return identifier
     */
    val id: Int,
    /**
     * Returns the preferred name of the variable.
     *
     * @return name
     */
    val name: String
) : Cloneable, Comparable<CorrelationId?> {

    /**
     * Creates a correlation identifier.
     * This is a type-safe wrapper over int.
     *
     * @param id     Identifier
     */
    constructor(id: Int) : this(id, CORREL_PREFIX + id) {}

    /**
     * Creates a correlation identifier from a name.
     *
     * @param name     variable name
     */
    constructor(name: String) : this(Integer.parseInt(name.substring(CORREL_PREFIX.length())), name) {
        assert(name.startsWith(CORREL_PREFIX)) {
            ("Correlation name should start with " + CORREL_PREFIX
                    + " actual name is " + name)
        }
    }

    @Override
    override fun toString(): String {
        return name
    }

    @Override
    operator fun compareTo(other: CorrelationId): Int {
        return id - other.id
    }

    @Override
    override fun hashCode(): Int {
        return id
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || obj is CorrelationId
                && id == (obj as CorrelationId).id)
    }

    companion object {
        /**
         * Prefix to the name of correlating variables.
         */
        const val CORREL_PREFIX = "\$cor"

        /** Converts a set of correlation ids to a set of names.  */
        fun setOf(set: Set<String>): ImmutableSet<CorrelationId> {
            if (set.isEmpty()) {
                return ImmutableSet.of()
            }
            val builder: ImmutableSet.Builder<CorrelationId> = ImmutableSet.builder()
            for (s in set) {
                builder.add(CorrelationId(s))
            }
            return builder.build()
        }

        /** Converts a set of names to a set of correlation ids.  */
        fun names(set: Set<CorrelationId>): Set<String> {
            if (set.isEmpty()) {
                return ImmutableSet.of()
            }
            val builder: ImmutableSet.Builder<String> = ImmutableSet.builder()
            for (s in set) {
                builder.add(s.name)
            }
            return builder.build()
        }
    }
}
