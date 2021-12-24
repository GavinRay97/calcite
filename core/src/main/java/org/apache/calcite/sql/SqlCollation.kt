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
package org.apache.calcite.sql

import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.sql.parser.SqlParserUtil
import org.apache.calcite.util.Glossary
import org.apache.calcite.util.SerializableCharset
import org.apache.calcite.util.Util
import org.checkerframework.checker.initialization.qual.UnderInitialization
import org.checkerframework.dataflow.qual.Pure
import java.io.Serializable
import java.nio.charset.Charset
import java.text.Collator
import java.util.Locale
import org.apache.calcite.util.Static.RESOURCE

/**
 * A `SqlCollation` is an object representing a `Collate`
 * statement. It is immutable.
 */
class SqlCollation : Serializable {
    //~ Enums ------------------------------------------------------------------
    /**
     * <blockquote>A &lt;character value expression&gt; consisting of a column
     * reference has the coercibility characteristic Implicit, with collating
     * sequence as defined when the column was created. A &lt;character value
     * expression&gt; consisting of a value other than a column (e.g., a host
     * variable or a literal) has the coercibility characteristic Coercible,
     * with the default collation for its character repertoire. A &lt;character
     * value expression&gt; simply containing a &lt;collate clause&gt; has the
     * coercibility characteristic Explicit, with the collating sequence
     * specified in the &lt;collate clause&gt;.</blockquote>
     *
     * @see Glossary.SQL99 SQL:1999 Part 2 Section 4.2.3
     */
    enum class Coercibility {
        /** Strongest coercibility.  */
        EXPLICIT, IMPLICIT, COERCIBLE,

        /** Weakest coercibility.  */
        NONE
    }

    //~ Instance fields --------------------------------------------------------
    val collationName: String
    protected val wrappedCharset: SerializableCharset
    protected val locale: Locale
    protected val strength: String
    val coercibility: Coercibility
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlCollation with the default collation name and the given
     * coercibility.
     *
     * @param coercibility Coercibility
     */
    constructor(coercibility: Coercibility) : this(
        CalciteSystemProperty.DEFAULT_COLLATION.value(),
        coercibility
    ) {
    }

    /**
     * Creates a Collation by its name and its coercibility.
     *
     * @param collation    Collation specification
     * @param coercibility Coercibility
     */
    constructor(
        collation: String?,
        coercibility: Coercibility
    ) {
        this.coercibility = coercibility
        val parseValues: SqlParserUtil.ParsedCollation = SqlParserUtil.parseCollation(collation)
        val charset: Charset = parseValues.getCharset()
        wrappedCharset = SerializableCharset.forCharset(charset)
        locale = parseValues.getLocale()
        strength = parseValues.getStrength().toLowerCase(Locale.ROOT)
        collationName = generateCollationName(charset)
    }

    /**
     * Creates a Collation by its coercibility, locale, charset and strength.
     */
    constructor(
        coercibility: Coercibility,
        locale: Locale,
        charset: Charset,
        strength: String
    ) {
        var charset: Charset = charset
        this.coercibility = coercibility
        charset = SqlUtil.getCharset(charset.name())
        wrappedCharset = SerializableCharset.forCharset(charset)
        this.locale = locale
        this.strength = strength.toLowerCase(Locale.ROOT)
        collationName = generateCollationName(charset)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable o: Object): Boolean {
        return (this === o
                || o is SqlCollation
                && collationName.equals((o as SqlCollation).collationName))
    }

    @Override
    override fun hashCode(): Int {
        return collationName.hashCode()
    }

    protected fun generateCollationName(
        charset: Charset
    ): String {
        return charset.name().toUpperCase(Locale.ROOT) + "$" + String.valueOf(locale) + "$" + strength
    }

    @Override
    override fun toString(): String {
        return "COLLATE $collationName"
    }

    fun unparse(
        writer: SqlWriter
    ) {
        writer.keyword("COLLATE")
        writer.identifier(collationName, false)
    }

    val charset: Charset
        get() = wrappedCharset.getCharset()

    fun getLocale(): Locale {
        return locale
    }

    /**
     * Returns the [Collator] to compare values having the current
     * collation, or `null` if no specific [Collator] is needed, in
     * which case [String.compareTo] will be used.
     */
    val collator: Collator?
        @Pure @Nullable get() = null

    companion object {
        val COERCIBLE = SqlCollation(Coercibility.COERCIBLE)
        val IMPLICIT = SqlCollation(Coercibility.IMPLICIT)

        /**
         * Returns the collating sequence (the collation name) and the coercibility
         * for the resulting value of a dyadic operator.
         *
         * @param col1 first operand for the dyadic operation
         * @param col2 second operand for the dyadic operation
         * @return the resulting collation sequence. The "no collating sequence"
         * result is returned as null.
         *
         * @see Glossary.SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
         */
        @Nullable
        fun getCoercibilityDyadicOperator(
            col1: SqlCollation?,
            col2: SqlCollation?
        ): SqlCollation? {
            return getCoercibilityDyadic(col1, col2)
        }

        /**
         * Returns the collating sequence (the collation name) and the coercibility
         * for the resulting value of a dyadic operator.
         *
         * @param col1 first operand for the dyadic operation
         * @param col2 second operand for the dyadic operation
         * @return the resulting collation sequence
         *
         * @throws org.apache.calcite.runtime.CalciteException from
         * [org.apache.calcite.runtime.CalciteResource.invalidCompare] or
         * [org.apache.calcite.runtime.CalciteResource.differentCollations]
         * if no collating sequence can be deduced
         *
         * @see Glossary.SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
         */
        fun getCoercibilityDyadicOperatorThrows(
            col1: SqlCollation,
            col2: SqlCollation
        ): SqlCollation {
            return getCoercibilityDyadic(col1, col2)
                ?: throw RESOURCE.invalidCompare(
                    col1.collationName,
                    "" + col1.coercibility,
                    col2.collationName,
                    "" + col2.coercibility
                ).ex()
        }

        /**
         * Returns the collating sequence (the collation name) to use for the
         * resulting value of a comparison.
         *
         * @param col1 first operand for the dyadic operation
         * @param col2 second operand for the dyadic operation
         *
         * @return the resulting collation sequence. If no collating
         * sequence could be deduced throws a
         * [org.apache.calcite.runtime.CalciteResource.invalidCompare]
         *
         * @see Glossary.SQL99 SQL:1999 Part 2 Section 4.2.3 Table 3
         */
        fun getCoercibilityDyadicComparison(
            col1: SqlCollation,
            col2: SqlCollation
        ): String {
            return getCoercibilityDyadicOperatorThrows(col1, col2).collationName
        }

        /**
         * Returns the result for [.getCoercibilityDyadicComparison] and
         * [.getCoercibilityDyadicOperator].
         */
        @Nullable
        protected fun getCoercibilityDyadic(
            col1: SqlCollation?,
            col2: SqlCollation?
        ): SqlCollation? {
            assert(null != col1)
            assert(null != col2)
            val coercibility1 = col1!!.coercibility
            val coercibility2 = col2!!.coercibility
            return when (coercibility1) {
                Coercibility.COERCIBLE -> when (coercibility2) {
                    Coercibility.COERCIBLE -> col2
                    Coercibility.IMPLICIT -> col2
                    Coercibility.NONE -> null
                    Coercibility.EXPLICIT -> col2
                    else -> throw Util.unexpected(coercibility2)
                }
                Coercibility.IMPLICIT -> when (coercibility2) {
                    Coercibility.COERCIBLE -> col1
                    Coercibility.IMPLICIT -> {
                        if (col1.collationName.equals(col2.collationName)) {
                            col2
                        } else null
                    }
                    Coercibility.NONE -> null
                    Coercibility.EXPLICIT -> col2
                    else -> throw Util.unexpected(coercibility2)
                }
                Coercibility.NONE -> when (coercibility2) {
                    Coercibility.COERCIBLE, Coercibility.IMPLICIT, Coercibility.NONE -> null
                    Coercibility.EXPLICIT -> col2
                    else -> throw Util.unexpected(coercibility2)
                }
                Coercibility.EXPLICIT -> when (coercibility2) {
                    Coercibility.COERCIBLE, Coercibility.IMPLICIT, Coercibility.NONE -> col1
                    Coercibility.EXPLICIT -> {
                        if (col1.collationName.equals(col2.collationName)) {
                            return col2
                        }
                        throw RESOURCE.differentCollations(
                            col1.collationName,
                            col2.collationName
                        ).ex()
                    }
                    else -> throw Util.unexpected(coercibility2)
                }
                else -> throw Util.unexpected(coercibility1)
            }
        }
    }
}
