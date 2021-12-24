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

import java.nio.ByteOrder

/**
 * Utility functions for converting from one type to another.
 */
object ConversionUtil {
    //~ Static fields/initializers ---------------------------------------------
    val NATIVE_UTF16_CHARSET_NAME = if (ByteOrder.nativeOrder() === ByteOrder.BIG_ENDIAN) "UTF-16BE" else "UTF-16LE"

    /**
     * A constant string which can be used wherever a Java string containing
     * Unicode characters is needed in a test. It spells 'anthropos' in Greek.
     */
    const val TEST_UNICODE_STRING = "\u03B1\u03BD\u03B8\u03C1\u03C9\u03C0\u03BF\u03C2"

    /**
     * A constant string which can be used wherever a SQL literal containing
     * Unicode escape characters is needed in a test. It spells 'anthropos' in
     * Greek. The escape character is the SQL default (backslash); note that the
     * backslash-doubling here is for Java only, so by the time the SQL parser
     * gets it, there is only one backslash.
     */
    const val TEST_UNICODE_SQL_ESCAPED_LITERAL = "\\03B1\\03BD\\03B8\\03C1\\03C9\\03C0\\03BF\\03C2"
    //~ Methods ----------------------------------------------------------------
    /**
     * Converts a byte array into a bit string or a hex string.
     *
     *
     * For example, `toStringFromByteArray(new byte[] {0xAB, 0xCD},
     * 16)` returns `ABCD`.
     */
    fun toStringFromByteArray(
        value: ByteArray,
        radix: Int
    ): String {
        assert(2 == radix || 16 == radix) { "Make sure that the algorithm below works for your radix" }
        if (0 == value.size) {
            return ""
        }
        val trick = radix * radix
        val ret = StringBuilder()
        for (b in value) {
            ret.append(Integer.toString(trick or (0x0ff and b.toInt()), radix).substring(1))
        }
        return ret.toString().toUpperCase(Locale.ROOT)
    }

    /**
     * Converts a string into a byte array. The inverse of
     * [.toStringFromByteArray].
     */
    fun toByteArrayFromString(
        value: String,
        radix: Int
    ): ByteArray {
        assert(16 == radix) { "Specified string to byte array conversion not supported yet" }
        assert(value.length() % 2 === 0) { "Hex binary string must contain even number of characters" }
        val ret = ByteArray(value.length() / 2)
        for (i in ret.indices) {
            val digit1: Int = Character.digit(
                value.charAt(i * 2),
                radix
            )
            val digit2: Int = Character.digit(
                value.charAt(i * 2 + 1),
                radix
            )
            assert(digit1 != -1 && digit2 != -1) { "String could not be converted to byte array" }
            ret[i] = (digit1 * radix + digit2).toByte()
        }
        return ret
    }

    /**
     * Converts an approximate value into a string, following the SQL 2003
     * standard.
     */
    fun toStringFromApprox(d: Double, isFloat: Boolean): String {
        val nf: NumberFormat = NumberUtil.getApproxFormatter(isFloat)
        return nf.format(d)
    }

    /**
     * Converts a string into a BOOLEAN.
     */
    @Nullable
    fun toBoolean(@Nullable str: String?): Boolean? {
        var str: String? = str ?: return null
        str = str.trim()
        return if (str.equalsIgnoreCase("TRUE")) {
            Boolean.TRUE
        } else if (str.equalsIgnoreCase("FALSE")) {
            Boolean.FALSE
        } else if (str.equalsIgnoreCase("UNKNOWN")) {
            null
        } else {
            throw RESOURCE.invalidBoolean(str).ex()
        }
    }
}
