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

import org.apache.calcite.rel.type.RelDataType

/**
 * Helpers for [SqlNameMatcher].
 */
object SqlNameMatchers {
    private val CASE_SENSITIVE = BaseMatcher(true)
    private val CASE_INSENSITIVE = BaseMatcher(false)

    /** Returns a name matcher with the given case sensitivity.  */
    fun withCaseSensitive(caseSensitive: Boolean): SqlNameMatcher {
        return if (caseSensitive) CASE_SENSITIVE else CASE_INSENSITIVE
    }

    /** Creates a name matcher that can suggest corrections to what the user
     * typed. It matches liberally (case-insensitively) and also records the last
     * match.  */
    fun liberal(): SqlNameMatcher {
        return LiberalNameMatcher()
    }

    /** Partial implementation of [SqlNameMatcher].  */
    private class BaseMatcher internal constructor(caseSensitive: Boolean) : SqlNameMatcher {
        @get:Override
        override val isCaseSensitive: Boolean

        init {
            caseSensitive = caseSensitive
        }

        @Override
        fun matches(string: String, name: String?): Boolean {
            return if (caseSensitive) string.equals(name) else string.equalsIgnoreCase(name)
        }

        protected fun listMatches(list0: List<String>, list1: List<String?>): Boolean {
            if (list0.size() !== list1.size()) {
                return false
            }
            for (i in 0 until list0.size()) {
                val s0 = list0[i]
                val s1 = list1[i]
                if (!matches(s0, s1)) {
                    return false
                }
            }
            return true
        }

        @Override
        operator fun <K : List<String?>?, V> get(
            map: Map<K, V>,
            prefixNames: List<String>, names: List<String>
        ): @Nullable V? {
            val key = concat(prefixNames, names)
            if (caseSensitive) {
                return map.get(key)
            }
            for (entry in map.entrySet()) {
                if (listMatches(key, entry.getKey())) {
                    matched(prefixNames, entry.getKey())
                    return entry.getValue()
                }
            }
            return null
        }

        protected fun matched(prefixNames: List<String?>?, names: List<String?>?) {}
        protected fun bestMatch(): List<String> {
            throw UnsupportedOperationException()
        }

        @Override
        override fun bestString(): String {
            return SqlIdentifier.getString(bestMatch())
        }

        @Override
        @Nullable
        override fun field(rowType: RelDataType, fieldName: String?): RelDataTypeField {
            return rowType.getField(fieldName, caseSensitive, false)
        }

        @Override
        fun frequency(names: Iterable<String>, name: String?): Int {
            var n = 0
            for (s in names) {
                if (matches(s, name)) {
                    ++n
                }
            }
            return n
        }

        @Override
        override fun createSet(): Set<String> {
            return if (isCaseSensitive()) LinkedHashSet() else TreeSet(String.CASE_INSENSITIVE_ORDER)
        }

        companion object {
            private fun concat(prefixNames: List<String>, names: List<String>): List<String> {
                return if (prefixNames.isEmpty()) {
                    names
                } else {
                    ImmutableList.< String > builder < String ? > ().addAll(prefixNames).addAll(names)
                        .build()
                }
            }
        }
    }

    /** Matcher that remembers the requests that were made of it.  */
    private class LiberalNameMatcher internal constructor() : BaseMatcher(false) {
        @Nullable
        var matchedNames: List<String>? = null
        @Override
        override fun listMatches(
            list0: List<String>,
            list1: List<String?>
        ): Boolean {
            val b = super.listMatches(list0, list1)
            if (b) {
                matchedNames = ImmutableList.copyOf(list1)
            }
            return b
        }

        @Override
        protected override fun matched(
            prefixNames: List<String?>,
            names: List<String?>?
        ) {
            matchedNames = ImmutableList.copyOf(
                if (Util.startsWith(names, prefixNames)) Util.skip(names, prefixNames.size()) else names
            )
        }

        @Override
        public override fun bestMatch(): List<String> {
            return requireNonNull(matchedNames, "matchedNames")
        }
    }
}
