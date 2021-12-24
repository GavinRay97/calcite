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
package org.apache.calcite.sql.util

import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.util.UnmodifiableArrayList
import java.sql.Timestamp
import java.util.List

/**
 * Extension to [StringBuilder] for the purposes of creating SQL queries
 * and expressions.
 *
 *
 * Using this class helps to prevent SQL injection attacks, incorrectly
 * quoted identifiers and strings. These problems occur when you build SQL by
 * concatenating strings, and you forget to treat identifers and string literals
 * correctly. SqlBuilder has special methods for appending identifiers and
 * literals.
 */
class SqlBuilder {
    private val buf: StringBuilder
    private val dialect: SqlDialect?

    /**
     * Creates a SqlBuilder.
     *
     * @param dialect Dialect
     */
    constructor(dialect: SqlDialect?) {
        assert(dialect != null)
        this.dialect = dialect
        buf = StringBuilder()
    }

    /**
     * Creates a SqlBuilder with a given string.
     *
     * @param dialect Dialect
     * @param s       Initial contents of the buffer
     */
    constructor(dialect: SqlDialect?, s: String?) {
        assert(dialect != null)
        this.dialect = dialect
        buf = StringBuilder(s)
    }

    /**
     * Returns the dialect.
     *
     * @return dialect
     */
    fun getDialect(): SqlDialect? {
        return dialect
    }

    /**
     * Returns the length (character count).
     *
     * @return the length of the sequence of characters currently
     * represented by this object
     */
    fun length(): Int {
        return buf.length()
    }

    /**
     * Clears the contents of the buffer.
     */
    fun clear() {
        buf.setLength(0)
    }

    /**
     * {@inheritDoc}
     *
     *
     * Returns the SQL string.
     *
     * @return SQL string
     * @see .getSql
     */
    @Override
    override fun toString(): String {
        return sql
    }

    /**
     * Returns the SQL.
     */
    val sql: String
        get() = buf.toString()

    /**
     * Returns the SQL and clears the buffer.
     *
     *
     * Convenient if you are reusing the same SQL builder in a loop.
     */
    val sqlAndClear: String
        get() {
            val str: String = buf.toString()
            clear()
            return str
        }

    /**
     * Appends a hygienic SQL string.
     *
     * @param s SQL string to append
     * @return This builder
     */
    fun append(s: SqlString): SqlBuilder {
        buf.append(s.getSql())
        return this
    }

    /**
     * Appends a string, without any quoting.
     *
     *
     * Calls to this method are dubious.
     *
     * @param s String to append
     * @return This builder
     */
    fun append(s: String?): SqlBuilder {
        buf.append(s)
        return this
    }

    /**
     * Appends a character, without any quoting.
     *
     * @param c Character to append
     * @return This builder
     */
    fun append(c: Char): SqlBuilder {
        buf.append(c)
        return this
    }

    /**
     * Appends a number, per [StringBuilder.append].
     */
    fun append(n: Long): SqlBuilder {
        buf.append(n)
        return this
    }

    /**
     * Appends an identifier to this buffer, quoting accordingly.
     *
     * @param name Identifier
     * @return This builder
     */
    fun identifier(name: String?): SqlBuilder {
        dialect.quoteIdentifier(buf, name)
        return this
    }

    /**
     * Appends one or more identifiers to this buffer, quoting accordingly.
     *
     * @param names Varargs array of identifiers
     * @return This builder
     */
    fun identifier(vararg names: String?): SqlBuilder {
        dialect.quoteIdentifier(buf, UnmodifiableArrayList.of(names))
        return this
    }

    /**
     * Appends a compound identifier to this buffer, quoting accordingly.
     *
     * @param names Parts of a compound identifier
     * @return This builder
     */
    fun identifier(names: List<String?>?): SqlBuilder {
        dialect.quoteIdentifier(buf, names)
        return this
    }

    /**
     * Returns the contents of this SQL buffer as a 'certified kocher' SQL
     * string.
     *
     *
     * Use this method in preference to [.toString]. It indicates
     * that the SQL string has been constructed using good hygiene, and is
     * therefore less likely to contain SQL injection or badly quoted
     * identifiers or strings.
     *
     * @return Contents of this builder as a SQL string.
     */
    fun toSqlString(): SqlString {
        return SqlString(dialect, buf.toString())
    }

    /**
     * Appends a string literal to this buffer.
     *
     *
     * For example, calling `literal("can't")`
     * would convert the buffer
     * <blockquote>`SELECT `</blockquote>
     * to
     * <blockquote>`SELECT 'can''t'`</blockquote>
     *
     * @param s String to append
     * @return This buffer
     */
    fun literal(s: String?): SqlBuilder {
        buf.append(
            if (s == null) "null" else dialect.quoteStringLiteral(s)
        )
        return this
    }

    /**
     * Appends a timestamp literal to this buffer.
     *
     * @param timestamp Timestamp to append
     * @return This buffer
     */
    fun literal(timestamp: Timestamp?): SqlBuilder {
        buf.append(
            if (timestamp == null) "null" else dialect.quoteTimestampLiteral(timestamp)
        )
        return this
    }

    /**
     * Returns the index within this string of the first occurrence of the
     * specified substring.
     *
     * @see StringBuilder.indexOf
     */
    fun indexOf(str: String?): Int {
        return buf.indexOf(str)
    }

    /**
     * Returns the index within this string of the first occurrence of the
     * specified substring, starting at the specified index.
     *
     * @see StringBuilder.indexOf
     */
    fun indexOf(str: String?, fromIndex: Int): Int {
        return buf.indexOf(str, fromIndex)
    }

    /**
     * Inserts the string into this character sequence.
     *
     * @see StringBuilder.insert
     */
    fun insert(offset: Int, str: String?): SqlBuilder {
        buf.insert(offset, str)
        return this
    }
}
