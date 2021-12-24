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
package org.apache.calcite.jdbc

import org.apache.calcite.sql.SqlCollation

/**
 * Collation that uses a specific [Collator] for comparison.
 */
class JavaCollation(coercibility: Coercibility?, locale: Locale?, charset: Charset?, strength: Int) :
    SqlCollation(coercibility, locale, charset, getStrengthString(strength)) {
    private val collator: Collator

    init {
        collator = Collator.getInstance(locale)
        collator.setStrength(strength)
    }

    @Override
    protected fun generateCollationName(
        charset: Charset?
    ): String {
        return super.generateCollationName(charset) + "\$JAVA_COLLATOR"
    }

    @Override
    @Nullable
    fun getCollator(): Collator {
        return collator
    }

    companion object {
        // Strength values
        private const val STRENGTH_PRIMARY = "primary"
        private const val STRENGTH_SECONDARY = "secondary"
        private const val STRENGTH_TERTIARY = "tertiary"
        private const val STRENGTH_IDENTICAL = "identical"
        private fun getStrengthString(strengthValue: Int): String {
            return when (strengthValue) {
                Collator.PRIMARY -> STRENGTH_PRIMARY
                Collator.SECONDARY -> STRENGTH_SECONDARY
                Collator.TERTIARY -> STRENGTH_TERTIARY
                Collator.IDENTICAL -> STRENGTH_IDENTICAL
                else -> throw IllegalArgumentException("Incorrect strength value.")
            }
        }
    }
}
