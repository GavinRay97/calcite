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

import com.google.common.collect.ImmutableList
import java.text.MessageFormat
import java.util.ArrayList
import java.util.List
import java.util.Locale
import java.util.Map

/**
 * String template.
 *
 *
 * It is extended from [java.text.MessageFormat] to allow parameters
 * to be substituted by name as well as by position.
 *
 *
 * The following example, using MessageFormat, yields "Happy 64th birthday,
 * Ringo!":
 *
 * <blockquote>MessageFormat f =
 * new MessageFormat("Happy {0,number}th birthday, {1}!");<br></br>
 * Object[] args = {64, "Ringo"};<br></br>
 * System.out.println(f.format(args);</blockquote>
 *
 *
 * Here is the same example using a Template and named parameters:
 *
 * <blockquote>Template f =
 * new Template("Happy {age,number}th birthday, {name}!");<br></br>
 * Map&lt;Object, Object&gt; args = new HashMap&lt;Object, Object&gt;();<br></br>
 * args.put("age", 64);<br></br>
 * args.put("name", "Ringo");<br></br>
 * System.out.println(f.format(args);</blockquote>
 *
 *
 * Using a Template you can also use positional parameters:
 *
 * <blockquote>Template f =
 * new Template("Happy {age,number}th birthday, {name}!");<br></br>
 * Object[] args = {64, "Ringo"};<br></br>
 * System.out.println(f.format(args);</blockquote>
 *
 *
 * Or a hybrid; here, one argument is specified by name, another by position:
 *
 * <blockquote>Template f =
 * new Template("Happy {age,number}th birthday, {name}!");<br></br>
 * Map&lt;Object, Object&gt; args = new HashMap&lt;Object, Object&gt;();<br></br>
 * args.put(0, 64);<br></br>
 * args.put("name", "Ringo");<br></br>
 * System.out.println(f.format(args);</blockquote>
 */
class Template private constructor(
    pattern: String, parameterNames: List<String>, locale: Locale
) : MessageFormat(pattern, locale) {
    /**
     * Returns the names of the parameters, in the order that they appeared in
     * the template string.
     *
     * @return List of parameter names
     */
    val parameterNames: List<String>

    init {
        this.parameterNames = ImmutableList.copyOf(parameterNames)
    }

    /**
     * Formats a set of arguments to produce a string.
     *
     *
     * Arguments may appear in the map using named keys (of type String), or
     * positional keys (0-based ordinals represented either as type String or
     * Integer).
     *
     * @param argMap A map containing the arguments as (key, value) pairs
     * @return Formatted string.
     * @throws IllegalArgumentException if the Format cannot format the given
     * object
     */
    fun format(argMap: Map<Object?, Object>): String {
        @Nullable val args: Array<Object?> = arrayOfNulls<Object>(parameterNames.size())
        for (i in 0 until parameterNames.size()) {
            args[i] = getArg(argMap, i)
        }
        return format(args)
    }

    /**
     * Returns the value of the `ordinal`th argument.
     *
     * @param argMap  Map of argument values
     * @param ordinal Ordinal of argument
     * @return Value of argument
     */
    @Nullable
    private fun getArg(argMap: Map<Object?, Object>, ordinal: Int): Object? {
        // First get by name.
        val parameterName = parameterNames[ordinal]
        var arg: Object? = argMap[parameterName]
        if (arg != null) {
            return arg
        }
        // Next by integer ordinal.
        arg = argMap[ordinal]
        return if (arg != null) {
            arg
        } else argMap[ordinal.toString() + ""]
        // Next by string ordinal.
    }

    companion object {
        /**
         * Creates a Template for the default locale and the
         * specified pattern.
         *
         * @param pattern the pattern for this message format
         * @throws IllegalArgumentException if the pattern is invalid
         */
        fun of(pattern: String): Template {
            return of(pattern, Locale.getDefault())
        }

        /**
         * Creates a Template for the specified locale and
         * pattern.
         *
         * @param pattern the pattern for this message format
         * @param locale  the locale for this message format
         * @throws IllegalArgumentException if the pattern is invalid
         */
        fun of(pattern: String, locale: Locale): Template {
            val parameterNames: List<String> = ArrayList()
            val processedPattern = process(pattern, parameterNames)
            return Template(processedPattern, parameterNames, locale)
        }

        /**
         * Parses the pattern, populates the parameter names, and returns the
         * pattern with parameter names converted to parameter ordinals.
         *
         *
         * To ensure that the same parsing rules apply, this code is copied from
         * [java.text.MessageFormat.applyPattern] but with different
         * actions when a parameter is recognized.
         *
         * @param pattern        Pattern
         * @param parameterNames Names of parameters (output)
         * @return Pattern with named parameters substituted with ordinals
         */
        private fun process(pattern: String, parameterNames: List<String>): String {
            val segments: Array<StringBuilder?> = arrayOfNulls<StringBuilder>(4)
            for (i in segments.indices) {
                segments[i] = StringBuilder()
            }
            var part = 0
            var inQuote = false
            var braceStack = 0
            var i = 0
            while (i < pattern.length()) {
                val ch: Char = pattern.charAt(i)
                if (part == 0) {
                    if (ch == '\'') {
                        segments[part].append(ch) // jhyde: don't lose orig quote
                        if (i + 1 < pattern.length()
                            && pattern.charAt(i + 1) === '\''
                        ) {
                            segments[part].append(ch) // handle doubles
                            ++i
                        } else {
                            inQuote = !inQuote
                        }
                    } else if (ch == '{' && !inQuote) {
                        part = 1
                    } else {
                        segments[part].append(ch)
                    }
                } else if (inQuote) {              // just copy quotes in parts
                    segments[part].append(ch)
                    if (ch == '\'') {
                        inQuote = false
                    }
                } else {
                    when (ch) {
                        ',' -> if (part < 3) {
                            part += 1
                        } else {
                            segments[part].append(ch)
                        }
                        '{' -> {
                            ++braceStack
                            segments[part].append(ch)
                        }
                        '}' -> if (braceStack == 0) {
                            part = 0
                            makeFormat(segments, parameterNames)
                        } else {
                            --braceStack
                            segments[part].append(ch)
                        }
                        '\'' -> {
                            inQuote = true
                            segments[part].append(ch)
                        }
                        else -> segments[part].append(ch)
                    }
                }
                ++i
            }
            if (braceStack == 0 && part != 0) {
                throw IllegalArgumentException(
                    "Unmatched braces in the pattern."
                )
            }
            return segments[0].toString()
        }

        /**
         * Called when a complete parameter has been seen.
         *
         * @param segments       Comma-separated segments of the parameter definition
         * @param parameterNames List of parameter names seen so far
         */
        private fun makeFormat(
            segments: Array<StringBuilder?>,
            parameterNames: List<String>
        ) {
            val parameterName: String = segments[1].toString()
            val parameterOrdinal: Int = parameterNames.size()
            parameterNames.add(parameterName)
            segments[0].append("{")
            segments[0].append(parameterOrdinal)
            val two: String = segments[2].toString()
            if (two.length() > 0) {
                segments[0].append(",").append(two)
            }
            val three: String = segments[3].toString()
            if (three.length() > 0) {
                segments[0].append(",").append(three)
            }
            segments[0].append("}")
            segments[1].setLength(0) // throw away other segments
            segments[2].setLength(0)
            segments[3].setLength(0)
        }

        /**
         * Creates a Template with the given pattern and uses it
         * to format the given arguments. This is equivalent to
         * <blockquote>
         * `[Template][.of](pattern).[.format](args)`
        </blockquote> *
         *
         * @throws IllegalArgumentException if the pattern is invalid,
         * or if an argument in the
         * `arguments` array is not of the
         * type expected by the format element(s)
         * that use it.
         */
        fun formatByName(
            pattern: String,
            argMap: Map<Object?, Object>
        ): String {
            return of(pattern).format(argMap)
        }
    }
}
