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

import java.util.Arrays
import java.util.Locale

/**
 * Utilities for converting SQL `LIKE` and `SIMILAR` operators
 * to regular expressions.
 */
object Like {
    private const val JAVA_REGEX_SPECIALS = "[]()|^-+*?{}$\\."
    private const val SQL_SIMILAR_SPECIALS = "[]()|^-+*_%?{}"
    private val REG_CHAR_CLASSES = arrayOf(
        "[:ALPHA:]", "\\p{Alpha}",
        "[:alpha:]", "\\p{Alpha}",
        "[:UPPER:]", "\\p{Upper}",
        "[:upper:]", "\\p{Upper}",
        "[:LOWER:]", "\\p{Lower}",
        "[:lower:]", "\\p{Lower}",
        "[:DIGIT:]", "\\d",
        "[:digit:]", "\\d",
        "[:SPACE:]", " ",
        "[:space:]", " ",
        "[:WHITESPACE:]", "\\s",
        "[:whitespace:]", "\\s",
        "[:ALNUM:]", "\\p{Alnum}",
        "[:alnum:]", "\\p{Alnum}"
    )

    // It's important to have XDigit before Digit to match XDigit first
    // (i.e. see the posixRegexToPattern method)
    private val POSIX_CHARACTER_CLASSES = arrayOf(
        "Lower", "Upper", "ASCII",
        "Alpha", "XDigit", "Digit", "Alnum", "Punct", "Graph", "Print", "Blank", "Cntrl", "Space"
    )

    /**
     * Translates a SQL LIKE pattern to Java regex pattern, with optional
     * escape string.
     */
    fun sqlToRegexLike(
        sqlPattern: String,
        @Nullable escapeStr: CharSequence?
    ): String {
        val escapeChar: Char
        if (escapeStr != null) {
            if (escapeStr.length() !== 1) {
                throw invalidEscapeCharacter(escapeStr.toString())
            }
            escapeChar = escapeStr.charAt(0)
        } else {
            escapeChar = 0.toChar()
        }
        return sqlToRegexLike(sqlPattern, escapeChar)
    }

    /**
     * Translates a SQL LIKE pattern to Java regex pattern.
     */
    fun sqlToRegexLike(
        sqlPattern: String,
        escapeChar: Char
    ): String {
        var i: Int
        val len: Int = sqlPattern.length()
        val javaPattern = StringBuilder(len + len)
        i = 0
        while (i < len) {
            val c: Char = sqlPattern.charAt(i)
            if (JAVA_REGEX_SPECIALS.indexOf(c) >= 0) {
                javaPattern.append('\\')
            }
            if (c == escapeChar) {
                if (i == sqlPattern.length() - 1) {
                    throw invalidEscapeSequence(sqlPattern, i)
                }
                val nextChar: Char = sqlPattern.charAt(i + 1)
                if (nextChar == '_'
                    || nextChar == '%'
                    || nextChar == escapeChar
                ) {
                    javaPattern.append(nextChar)
                    i++
                } else {
                    throw invalidEscapeSequence(sqlPattern, i)
                }
            } else if (c == '_') {
                javaPattern.append('.')
            } else if (c == '%') {
                javaPattern.append("(?s:.*)")
            } else {
                javaPattern.append(c)
            }
            i++
        }
        return javaPattern.toString()
    }

    private fun invalidEscapeCharacter(s: String): RuntimeException {
        return RuntimeException(
            "Invalid escape character '$s'"
        )
    }

    private fun invalidEscapeSequence(s: String, i: Int): RuntimeException {
        return RuntimeException(
            "Invalid escape sequence '$s', $i"
        )
    }

    private fun similarEscapeRuleChecking(
        sqlPattern: String,
        escapeChar: Char
    ) {
        if (escapeChar.code == 0) {
            return
        }
        if (SQL_SIMILAR_SPECIALS.indexOf(escapeChar) >= 0) {
            // The the escape character is a special character
            // SQL 2003 Part 2 Section 8.6 General Rule 3.b
            for (i in 0 until sqlPattern.length()) {
                if (sqlPattern.charAt(i) === escapeChar) {
                    if (i == sqlPattern.length() - 1) {
                        throw invalidEscapeSequence(sqlPattern, i)
                    }
                    val c: Char = sqlPattern.charAt(i + 1)
                    if (SQL_SIMILAR_SPECIALS.indexOf(c) < 0
                        && c != escapeChar
                    ) {
                        throw invalidEscapeSequence(sqlPattern, i)
                    }
                }
            }
        }

        // SQL 2003 Part 2 Section 8.6 General Rule 3.c
        if (escapeChar == ':') {
            var position: Int
            position = sqlPattern.indexOf("[:")
            if (position >= 0) {
                position = sqlPattern.indexOf(":]")
            }
            if (position < 0) {
                throw invalidEscapeSequence(sqlPattern, position)
            }
        }
    }

    private fun invalidRegularExpression(
        pattern: String, i: Int
    ): RuntimeException {
        return RuntimeException(
            "Invalid regular expression '$pattern', index $i"
        )
    }

    private fun sqlSimilarRewriteCharEnumeration(
        sqlPattern: String,
        javaPattern: StringBuilder,
        pos: Int,
        escapeChar: Char
    ): Int {
        var i: Int
        i = pos + 1
        while (i < sqlPattern.length()) {
            val c: Char = sqlPattern.charAt(i)
            if (c == ']') {
                return i - 1
            } else if (c == escapeChar) {
                i++
                val nextChar: Char = sqlPattern.charAt(i)
                if (SQL_SIMILAR_SPECIALS.indexOf(nextChar) >= 0) {
                    if (JAVA_REGEX_SPECIALS.indexOf(nextChar) >= 0) {
                        javaPattern.append('\\')
                    }
                    javaPattern.append(nextChar)
                } else if (escapeChar == nextChar) {
                    javaPattern.append(nextChar)
                } else {
                    throw invalidRegularExpression(sqlPattern, i)
                }
            } else if (c == '-') {
                javaPattern.append('-')
            } else if (c == '^') {
                javaPattern.append('^')
            } else if (sqlPattern.startsWith("[:", i)) {
                val numOfRegCharSets = REG_CHAR_CLASSES.size / 2
                var found = false
                for (j in 0 until numOfRegCharSets) {
                    if (sqlPattern.startsWith(REG_CHAR_CLASSES[j + j], i)) {
                        javaPattern.append(REG_CHAR_CLASSES[j + j + 1])
                        i += REG_CHAR_CLASSES[j + j].length() - 1
                        found = true
                        break
                    }
                }
                if (!found) {
                    throw invalidRegularExpression(sqlPattern, i)
                }
            } else if (SQL_SIMILAR_SPECIALS.indexOf(c) >= 0) {
                throw invalidRegularExpression(sqlPattern, i)
            } else {
                javaPattern.append(c)
            }
            i++
        }
        return i - 1
    }

    /**
     * Translates a SQL SIMILAR pattern to Java regex pattern, with optional
     * escape string.
     */
    fun sqlToRegexSimilar(
        sqlPattern: String,
        @Nullable escapeStr: CharSequence?
    ): String {
        val escapeChar: Char
        if (escapeStr != null) {
            if (escapeStr.length() !== 1) {
                throw invalidEscapeCharacter(escapeStr.toString())
            }
            escapeChar = escapeStr.charAt(0)
        } else {
            escapeChar = 0.toChar()
        }
        return sqlToRegexSimilar(sqlPattern, escapeChar)
    }

    /**
     * Translates SQL SIMILAR pattern to Java regex pattern.
     */
    fun sqlToRegexSimilar(
        sqlPattern: String,
        escapeChar: Char
    ): String {
        similarEscapeRuleChecking(sqlPattern, escapeChar)
        var insideCharacterEnumeration = false
        val javaPattern = StringBuilder(sqlPattern.length() * 2)
        val len: Int = sqlPattern.length()
        var i = 0
        while (i < len) {
            val c: Char = sqlPattern.charAt(i)
            if (c == escapeChar) {
                if (i == len - 1) {
                    // It should never reach here after the escape rule
                    // checking.
                    throw invalidEscapeSequence(sqlPattern, i)
                }
                val nextChar: Char = sqlPattern.charAt(i + 1)
                if (SQL_SIMILAR_SPECIALS.indexOf(nextChar) >= 0) {
                    // special character, use \ to replace the escape char.
                    if (JAVA_REGEX_SPECIALS.indexOf(nextChar) >= 0) {
                        javaPattern.append('\\')
                    }
                    javaPattern.append(nextChar)
                } else if (nextChar == escapeChar) {
                    javaPattern.append(nextChar)
                } else {
                    // It should never reach here after the escape rule
                    // checking.
                    throw invalidEscapeSequence(sqlPattern, i)
                }
                i++ // we already process the next char.
            } else {
                when (c) {
                    '_' -> javaPattern.append('.')
                    '%' -> javaPattern.append("(?s:.*)")
                    '[' -> {
                        javaPattern.append('[')
                        insideCharacterEnumeration = true
                        i = sqlSimilarRewriteCharEnumeration(
                            sqlPattern,
                            javaPattern,
                            i,
                            escapeChar
                        )
                    }
                    ']' -> {
                        if (!insideCharacterEnumeration) {
                            throw invalidRegularExpression(sqlPattern, i)
                        }
                        insideCharacterEnumeration = false
                        javaPattern.append(']')
                    }
                    '\\' -> javaPattern.append("\\\\")
                    '$' ->
                        // $ is special character in java regex, but regular in
                        // SQL regex.
                        javaPattern.append("\\$")
                    else -> javaPattern.append(c)
                }
            }
            i++
        }
        if (insideCharacterEnumeration) {
            throw invalidRegularExpression(sqlPattern, len)
        }
        return javaPattern.toString()
    }

    fun posixRegexToPattern(regex: String, caseSensitive: Boolean): java.util.regex.Pattern {
        // Replace existing character classes with java equivalent ones
        var regex = regex
        val originalRegex = regex
        val existingExpressions: Array<String> = Arrays.stream(POSIX_CHARACTER_CLASSES)
            .filter { v -> originalRegex.contains(v.toLowerCase(Locale.ROOT)) }.toArray { _Dummy_.__Array__() }
        for (v in existingExpressions) {
            regex = regex.replace(v.toLowerCase(Locale.ROOT), "\\p{$v}")
        }
        val flags = if (caseSensitive) 0 else java.util.regex.Pattern.CASE_INSENSITIVE
        return java.util.regex.Pattern.compile(regex, flags)
    }
}
