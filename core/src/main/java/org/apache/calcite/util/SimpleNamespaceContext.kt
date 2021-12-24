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

import java.util.Collections

/**
 * Simple [javax.xml.namespace.NamespaceContext] implementation. Follows the standard
 * NamespaceContext contract, and is loadable via a [java.util.Map]
 */
class SimpleNamespaceContext @SuppressWarnings(["method.invocation.invalid", "methodref.receiver.bound.invalid"]) constructor(
    bindings: Map<String?, String?>
) : NamespaceContext {
    private val prefixToNamespaceUri: Map<String, String> = HashMap()
    private val namespaceUriToPrefixes: Map<String, Set<String>> = HashMap()

    init {
        bindNamespaceUri(XMLConstants.XML_NS_PREFIX, XMLConstants.XML_NS_URI)
        bindNamespaceUri(XMLConstants.XMLNS_ATTRIBUTE, XMLConstants.XMLNS_ATTRIBUTE_NS_URI)
        bindNamespaceUri(XMLConstants.DEFAULT_NS_PREFIX, "")
        bindings.forEach { prefix: String, namespaceUri: String -> bindNamespaceUri(prefix, namespaceUri) }
    }

    @Override
    fun getNamespaceURI(prefix: String): String? {
        return if (prefixToNamespaceUri.containsKey(prefix)) {
            prefixToNamespaceUri[prefix]
        } else ""
    }

    @Override
    @Nullable
    fun getPrefix(namespaceUri: String): String? {
        val prefixes = getPrefixesSet(namespaceUri)
        return if (!prefixes.isEmpty()) prefixes.iterator().next() else null
    }

    @Override
    fun getPrefixes(namespaceUri: String): Iterator<String> {
        return getPrefixesSet(namespaceUri).iterator()
    }

    private fun getPrefixesSet(namespaceUri: String): Set<String> {
        val prefixes = namespaceUriToPrefixes[namespaceUri]
        return if (prefixes != null) Collections.unmodifiableSet(prefixes) else Collections.emptySet()
    }

    private fun bindNamespaceUri(prefix: String, namespaceUri: String) {
        prefixToNamespaceUri.put(prefix, namespaceUri)
        val prefixes: Set<String> = namespaceUriToPrefixes
            .computeIfAbsent(namespaceUri) { k -> LinkedHashSet() }
        prefixes.add(prefix)
    }
}
