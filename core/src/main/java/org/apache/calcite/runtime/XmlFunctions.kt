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

import org.apache.calcite.util.SimpleNamespaceContext

/**
 * A collection of functions used in Xml processing.
 */
object XmlFunctions {
    private val XPATH_FACTORY: ThreadLocal<XPathFactory> = ThreadLocal.withInitial(XPathFactory::newInstance)
    private val TRANSFORMER_FACTORY: ThreadLocal<TransformerFactory> = ThreadLocal.withInitial {
        val transformerFactory: TransformerFactory = TransformerFactory.newInstance()
        transformerFactory.setErrorListener(InternalErrorListener())
        transformerFactory
    }
    private val VALID_NAMESPACE_PATTERN: Pattern = Pattern
        .compile("^(([0-9a-zA-Z:_-]+=\"[^\"]*\")( [0-9a-zA-Z:_-]+=\"[^\"]*\")*)$")
    private val EXTRACT_NAMESPACE_PATTERN: Pattern = Pattern
        .compile("([0-9a-zA-Z:_-]+)=(['\"])((?!\\2).+?)\\2")

    @Nullable
    fun extractValue(@Nullable input: String?, @Nullable xpath: String?): String? {
        return if (input == null || xpath == null) {
            null
        } else try {
            val xpathExpression: XPathExpression = castNonNull(XPATH_FACTORY.get()).newXPath().compile(xpath)
            try {
                val nodes: NodeList = xpathExpression
                    .evaluate(InputSource(StringReader(input)), XPathConstants.NODESET) as NodeList
                val result: List<String> = ArrayList()
                for (i in 0 until nodes.getLength()) {
                    val item: Node = castNonNull(nodes.item(i))
                    val firstChild: Node = requireNonNull(
                        item.getFirstChild()
                    ) { "firstChild of node $item" }
                    result.add(firstChild.getTextContent())
                }
                StringUtils.join(result, " ")
            } catch (e: XPathExpressionException) {
                xpathExpression.evaluate(InputSource(StringReader(input)))
            }
        } catch (ex: XPathExpressionException) {
            throw RESOURCE.invalidInputForExtractValue(input, xpath).ex()
        }
    }

    @Nullable
    fun xmlTransform(@Nullable xml: String?, @Nullable xslt: String?): String? {
        return if (xml == null || xslt == null) {
            null
        } else try {
            val xsltSource: Source = StreamSource(StringReader(xslt))
            val xmlSource: Source = StreamSource(StringReader(xml))
            val transformer: Transformer = castNonNull(TRANSFORMER_FACTORY.get())
                .newTransformer(xsltSource)
            val writer = StringWriter()
            val result = StreamResult(writer)
            transformer.setErrorListener(InternalErrorListener())
            transformer.transform(xmlSource, result)
            writer.toString()
        } catch (e: TransformerConfigurationException) {
            throw RESOURCE.illegalXslt(xslt).ex()
        } catch (e: TransformerException) {
            throw RESOURCE.invalidInputForXmlTransform(xml).ex()
        }
    }

    @Nullable
    fun extractXml(@Nullable xml: String?, @Nullable xpath: String?): String? {
        return extractXml(xml, xpath, null)
    }

    @Nullable
    fun extractXml(
        @Nullable xml: String?, @Nullable xpath: String?,
        @Nullable namespace: String?
    ): String? {
        return if (xml == null || xpath == null) {
            null
        } else try {
            val xPath: XPath = castNonNull(XPATH_FACTORY.get()).newXPath()
            if (namespace != null) {
                xPath.setNamespaceContext(extractNamespaceContext(namespace))
            }
            val xpathExpression: XPathExpression = xPath.compile(xpath)
            try {
                val result: List<String> = ArrayList()
                val nodes: NodeList = xpathExpression
                    .evaluate(InputSource(StringReader(xml)), XPathConstants.NODESET) as NodeList
                for (i in 0 until nodes.getLength()) {
                    result.add(convertNodeToString(castNonNull(nodes.item(i))))
                }
                StringUtils.join(result, "")
            } catch (e: XPathExpressionException) {
                val node: Node = xpathExpression
                    .evaluate(InputSource(StringReader(xml)), XPathConstants.NODE) as Node
                convertNodeToString(node)
            }
        } catch (ex: IllegalArgumentException) {
            throw RESOURCE.invalidInputForExtractXml(xpath, namespace).ex()
        } catch (ex: XPathExpressionException) {
            throw RESOURCE.invalidInputForExtractXml(xpath, namespace).ex()
        } catch (ex: TransformerException) {
            throw RESOURCE.invalidInputForExtractXml(xpath, namespace).ex()
        }
    }

    @Nullable
    fun existsNode(@Nullable xml: String?, @Nullable xpath: String?): Integer? {
        return existsNode(xml, xpath, null)
    }

    @Nullable
    fun existsNode(
        @Nullable xml: String?, @Nullable xpath: String?,
        @Nullable namespace: String?
    ): Integer? {
        return if (xml == null || xpath == null) {
            null
        } else try {
            val xPath: XPath = castNonNull(XPATH_FACTORY.get()).newXPath()
            if (namespace != null) {
                xPath.setNamespaceContext(extractNamespaceContext(namespace))
            }
            val xpathExpression: XPathExpression = xPath.compile(xpath)
            try {
                val nodes: NodeList = xpathExpression
                    .evaluate(InputSource(StringReader(xml)), XPathConstants.NODESET) as NodeList
                if (nodes != null && nodes.getLength() > 0) {
                    1
                } else 0
            } catch (e: XPathExpressionException) {
                val node: Node = xpathExpression
                    .evaluate(InputSource(StringReader(xml)), XPathConstants.NODE) as Node
                if (node != null) {
                    1
                } else 0
            }
        } catch (ex: IllegalArgumentException) {
            throw RESOURCE.invalidInputForExistsNode(xpath, namespace).ex()
        } catch (ex: XPathExpressionException) {
            throw RESOURCE.invalidInputForExistsNode(xpath, namespace).ex()
        }
    }

    private fun extractNamespaceContext(namespace: String): SimpleNamespaceContext {
        if (!VALID_NAMESPACE_PATTERN.matcher(namespace).find()) {
            throw IllegalArgumentException("Invalid namespace $namespace")
        }
        val namespaceMap: Map<String, String> = HashMap()
        val matcher: Matcher = EXTRACT_NAMESPACE_PATTERN.matcher(namespace)
        while (matcher.find()) {
            namespaceMap.put(castNonNull(matcher.group(1)), castNonNull(matcher.group(3)))
        }
        return SimpleNamespaceContext(namespaceMap)
    }

    @Throws(TransformerException::class)
    private fun convertNodeToString(node: Node): String {
        val writer = StringWriter()
        val transformer: Transformer = castNonNull(TRANSFORMER_FACTORY.get()).newTransformer()
        transformer.setErrorListener(InternalErrorListener())
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes")
        transformer.transform(DOMSource(node), StreamResult(writer))
        return writer.toString()
    }

    /** The internal default ErrorListener for Transformer. Just rethrows errors to
     * discontinue the XML transformation.  */
    private class InternalErrorListener : ErrorListener {
        @Override
        @Throws(TransformerException::class)
        fun warning(exception: TransformerException?) {
            throw exception
        }

        @Override
        @Throws(TransformerException::class)
        fun error(exception: TransformerException?) {
            throw exception
        }

        @Override
        @Throws(TransformerException::class)
        fun fatalError(exception: TransformerException?) {
            throw exception
        }
    }
}
