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

import org.apache.calcite.avatica.util.ByteString

/**
 * A string, optionally with [character set][Charset] and
 * [SqlCollation]. It is immutable.
 */
class NlsString private constructor(
    @Nullable stringValue: String?, @Nullable bytesValue: ByteString?,
    @Nullable charsetName: String?, @Nullable collation: SqlCollation
) : Comparable<NlsString?>, Cloneable, Cloneable {
    @Nullable
    private val stringValue: String?

    @Nullable
    private val bytesValue: ByteString?

    @get:Nullable
    @get:Pure
    @Nullable
    val charsetName: String? = null

    @Nullable
    private var charset: Charset? = null

    @Nullable
    private val collation: SqlCollation?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a string in a specified character set.
     *
     * @param bytesValue  Byte array constant, must not be null
     * @param charsetName Name of the character set, must not be null
     * @param collation   Collation, may be null
     *
     * @throws IllegalCharsetNameException If the given charset name is illegal
     * @throws UnsupportedCharsetException If no support for the named charset
     * is available in this instance of the Java virtual machine
     * @throws RuntimeException If the given value cannot be represented in the
     * given charset
     */
    constructor(
        bytesValue: ByteString?, charsetName: String?,
        @Nullable collation: SqlCollation
    ) : this(
        null, Objects.requireNonNull(bytesValue, "bytesValue"),
        Objects.requireNonNull(charsetName, "charsetName"), collation
    ) {
    }

    /**
     * Easy constructor for Java string.
     *
     * @param stringValue String constant, must not be null
     * @param charsetName Name of the character set, may be null
     * @param collation Collation, may be null
     *
     * @throws IllegalCharsetNameException If the given charset name is illegal
     * @throws UnsupportedCharsetException If no support for the named charset
     * is available in this instance of the Java virtual machine
     * @throws RuntimeException If the given value cannot be represented in the
     * given charset
     */
    constructor(
        stringValue: String?, @Nullable charsetName: String?,
        @Nullable collation: SqlCollation
    ) : this(Objects.requireNonNull(stringValue, "stringValue"), null, charsetName, collation) {
    }

    /** Internal constructor; other constructors must call it.  */
    init {
        if (charsetName != null) {
            this.charsetName = charsetName.toUpperCase(Locale.ROOT)
            charset = SqlUtil.getCharset(charsetName)
        } else {
            this.charsetName = null
            charset = null
        }
        if (stringValue != null == (bytesValue != null)) {
            throw IllegalArgumentException("Specify stringValue or bytesValue")
        }
        if (bytesValue != null) {
            if (charset == null) {
                throw IllegalArgumentException("Bytes value requires charset")
            }
            SqlUtil.validateCharset(bytesValue, charset)
        } else {
            assert(stringValue != null) { "stringValue must not be null" }
            // Java string can be malformed if LATIN1 is required.
            if (this.charsetName != null
                && (this.charsetName!!.equals("LATIN1")
                        || this.charsetName!!.equals("ISO-8859-1"))
            ) {
                assert(charset != null) { "charset must not be null" }
                if (!charset.newEncoder().canEncode(stringValue)) {
                    throw RESOURCE.charsetEncoding(stringValue, charset.name()).ex()
                }
            }
        }
        this.collation = collation
        this.stringValue = stringValue
        this.bytesValue = bytesValue
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun clone(): Object {
        return try {
            super.clone()
        } catch (e: CloneNotSupportedException) {
            throw AssertionError()
        }
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(stringValue, bytesValue, charsetName, collation)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is NlsString
                && Objects.equals(stringValue, (obj as NlsString).stringValue)
                && Objects.equals(bytesValue, (obj as NlsString).bytesValue)
                && Objects.equals(charsetName, (obj as NlsString).charsetName)
                && Objects.equals(collation, (obj as NlsString).collation)))
    }

    @Override
    operator fun compareTo(other: NlsString): Int {
        return if (collation != null && collation.getCollator() != null) {
            collation.getCollator().compare(value, other.value)
        } else value.compareTo(other.value)
    }

    @Pure
    @Nullable
    fun getCharset(): Charset? {
        return charset
    }

    @Pure
    @Nullable
    fun getCollation(): SqlCollation? {
        return collation
    }

    val value: String
        get() {
            if (stringValue == null) {
                assert(bytesValue != null) { "bytesValue must not be null" }
                assert(charset != null) { "charset must not be null" }
                return DECODE_MAP.getUnchecked(Pair.of(bytesValue, charset))
            }
            return stringValue
        }

    /**
     * Returns a string the same as this but with spaces trimmed from the
     * right.
     */
    fun rtrim(): NlsString {
        val trimmed: String = SqlFunctions.rtrim(value)
        return if (!trimmed.equals(value)) {
            NlsString(trimmed, charsetName, collation)
        } else this
    }

    /** As [.asSql] but with SQL standard
     * dialect.  */
    fun asSql(prefix: Boolean, suffix: Boolean): String {
        return asSql(prefix, suffix, AnsiSqlDialect.DEFAULT)
    }

    /**
     * Returns the string quoted for SQL, for example `_ISO-8859-1'is it a
     * plane? no it''s superman!'`.
     *
     * @param prefix if true, prefix the character set name
     * @param suffix if true, suffix the collation clause
     * @param dialect Dialect
     * @return the quoted string
     */
    fun asSql(
        prefix: Boolean,
        suffix: Boolean,
        dialect: SqlDialect
    ): String {
        val ret = StringBuilder()
        dialect.quoteStringLiteral(ret, if (prefix) charsetName else null, value)

        // NOTE jvs 3-Feb-2005:  see FRG-78 for why this should go away
        if (false) {
            if (suffix && null != collation) {
                ret.append(" ")
                ret.append(collation.toString())
            }
        }
        return ret.toString()
    }

    /**
     * Returns the string quoted for SQL, for example `_ISO-8859-1'is it a
     * plane? no it''s superman!'`.
     */
    @Override
    override fun toString(): String {
        return asSql(true, true)
    }

    /** Creates a copy of this `NlsString` with different content but same
     * charset and collation.  */
    fun copy(value: String?): NlsString {
        return NlsString(value, charsetName, collation)
    }

    /** Returns the value as a [ByteString].  */
    @get:Nullable
    @get:Pure
    val valueBytes: ByteString?
        get() = bytesValue

    companion object {
        //~ Instance fields --------------------------------------------------------
        private val DECODE_MAP: LoadingCache<Pair<ByteString, Charset>, String> = CacheBuilder.newBuilder()
            .softValues()
            .build(
                object : CacheLoader<Pair<ByteString?, Charset?>?, String?>() {
                    @Override
                    fun load(key: Pair<ByteString, Charset>): String {
                        val charset: Charset = key.right
                        val decoder: CharsetDecoder = charset.newDecoder()
                        val bytes: ByteArray = key.left.getBytes()
                        val buffer: ByteBuffer = ByteBuffer.wrap(bytes)
                        return try {
                            decoder.decode(buffer).toString()
                        } catch (ex: CharacterCodingException) {
                            throw RESOURCE.charsetEncoding( //CHECKSTYLE: IGNORE 1
                                String(bytes, Charset.defaultCharset()),
                                charset.name()
                            ).ex()
                        }
                    }
                })

        /**
         * Concatenates some [NlsString] objects. The result has the charset
         * and collation of the first element. The other elements must have matching
         * (or null) charset and collation. Concatenates all at once, not pairwise,
         * to avoid string copies.
         *
         * @param args array of [NlsString] to be concatenated
         */
        fun concat(args: List<NlsString>): NlsString {
            if (args.size() < 2) {
                return args[0]
            }
            val charSetName = args[0].charsetName
            val collation: SqlCollation? = args[0].collation
            var length: Int = args[0].value.length()

            // sum string lengths and validate
            for (i in 1 until args.size()) {
                val arg = args[i]
                length += arg.value.length()
                if (!(arg.charsetName == null
                            || arg.charsetName!!.equals(charSetName))
                ) {
                    throw IllegalArgumentException("mismatched charsets")
                }
                if (!(arg.collation == null
                            || arg.collation.equals(collation))
                ) {
                    throw IllegalArgumentException("mismatched collations")
                }
            }
            val sb = StringBuilder(length)
            for (arg in args) {
                sb.append(arg.value)
            }
            return NlsString(
                sb.toString(),
                charSetName,
                collation
            )
        }
    }
}
