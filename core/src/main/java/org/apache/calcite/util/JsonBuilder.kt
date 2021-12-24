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

import org.apache.calcite.avatica.util.Spaces

/**
 * Builder for JSON documents (represented as [List], [Map],
 * [String], [Boolean], [Long]).
 */
class JsonBuilder {
    /**
     * Creates a JSON object (represented by a [Map]).
     */
    fun map(): Map<String, Object> {
        // Use LinkedHashMap to preserve order.
        return LinkedHashMap()
    }

    /**
     * Creates a JSON object (represented by a [List]).
     */
    fun list(): List<Object> {
        return ArrayList()
    }

    /**
     * Adds a key/value pair to a JSON object.
     */
    fun put(map: Map<String?, Object?>, name: String?, @Nullable value: Object?): JsonBuilder {
        map.put(name, value)
        return this
    }

    /**
     * Adds a key/value pair to a JSON object if the value is not null.
     */
    fun putIf(
        map: Map<String?, Object?>, name: String?, @Nullable value: Object?
    ): JsonBuilder {
        if (value != null) {
            map.put(name, value)
        }
        return this
    }

    /**
     * Serializes an object consisting of maps, lists and atoms into a JSON
     * string.
     *
     *
     * We should use a JSON library such as Jackson when Mondrian needs
     * one elsewhere.
     */
    fun toJsonString(o: Object?): String {
        val buf = StringBuilder()
        append(buf, 0, o)
        return buf.toString()
    }

    /**
     * Appends a JSON object to a string builder.
     */
    fun append(buf: StringBuilder, indent: Int, @Nullable o: Object?) {
        if (o == null) {
            buf.append("null")
        } else if (o is Map) {
            appendMap(buf, indent, o as Map)
        } else if (o is List) {
            appendList(buf, indent, o as List<*>)
        } else if (o is String) {
            buf.append('"')
                .append(
                    (o as String).replace("\"", "\\\"")
                        .replace("\n", "\\n")
                )
                .append('"')
        } else {
            assert(o is Number || o is Boolean)
            buf.append(o)
        }
    }

    private fun appendMap(
        buf: StringBuilder, indent: Int, map: Map<String, Object>
    ) {
        if (map.isEmpty()) {
            buf.append("{}")
            return
        }
        buf.append("{")
        newline(buf, indent + 1)
        var n = 0
        for (entry in map.entrySet()) {
            if (n++ > 0) {
                buf.append(",")
                newline(buf, indent + 1)
            }
            append(buf, 0, entry.getKey())
            buf.append(": ")
            append(buf, indent + 1, entry.getValue())
        }
        newline(buf, indent)
        buf.append("}")
    }

    private fun appendList(
        buf: StringBuilder, indent: Int, list: List<*>
    ) {
        if (list.isEmpty()) {
            buf.append("[]")
            return
        }
        buf.append("[")
        newline(buf, indent + 1)
        var n = 0
        for (o in list) {
            if (n++ > 0) {
                buf.append(",")
                newline(buf, indent + 1)
            }
            append(buf, indent + 1, o)
        }
        newline(buf, indent)
        buf.append("]")
    }

    companion object {
        private fun newline(buf: StringBuilder, indent: Int) {
            Spaces.append(buf.append('\n'), indent * 2)
        }
    }
}
