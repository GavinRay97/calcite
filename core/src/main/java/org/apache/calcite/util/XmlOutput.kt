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

import com.google.common.collect.Lists
import java.io.PrintWriter
import java.io.Writer
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Collections
import java.util.Deque
import java.util.List
import java.util.Objects.requireNonNull

/**
 * Streaming XML output.
 *
 *
 * Use this class to write XML to any streaming source.
 * While the class itself is unstructured and doesn't enforce any DTD
 * specification, use of the class
 * does ensure that the output is syntactically valid XML.
 */
class XmlOutput(out: PrintWriter) {
    // This Writer is the underlying output stream to which all XML is
    // written.
    private val out: PrintWriter

    // The tagStack is maintained to check that tags are balanced.
    private val tagStack: Deque<String> = ArrayDeque()

    // The class maintains an indentation level to improve output quality.
    private var indent: Int

    // The class also maintains the total number of tags written.  This
    // is used to monitor changes to the output
    private var tagsWritten: Int
    /**
     * Sets or unsets the compact mode.  Compact mode causes the generated
     * XML to be free of extraneous whitespace and other unnecessary
     * characters.
     *
     * @param compact true to turn on compact mode, or false to turn it off.
     */
    /** Whehter output should be compacted.  Compacted output is free of
     * extraneous whitespace and is designed for easier transport.  */
    var compact = false

    /** String to write for each indent level; see [.setIndentString].  */
    private var indentString = "\t"

    /** Whether to detect that tags are empty; see [.setGlob].  */
    private var glob = false

    /**
     * Whether we have started but not finished a start tag. This only happens
     * if `glob` is true. The start tag is automatically closed
     * when we start a child node. If there are no child nodes, [.endTag]
     * creates an empty tag.
     */
    private var inTag = false

    /** Whether to always quote CDATA segments (even if they don't contain
     * special characters); see [.setAlwaysQuoteCData].  */
    private var alwaysQuoteCData = false
    /**
     * Sets whether to ignore unquoted text, such as whitespace.
     */
    /** Whether to ignore unquoted text, such as whitespace; see
     * [.setIgnorePcdata].  */
    var ignorePcdata = false

    /**
     * Private helper function to display a degree of indentation.
     *
     * @param out the PrintWriter to which to display output.
     * @param indent the degree of indentation.
     */
    private fun displayIndent(out: PrintWriter, indent: Int) {
        if (!compact) {
            for (i in 0 until indent) {
                out.print(indentString)
            }
        }
    }

    /**
     * Constructs a new XmlOutput based on any [Writer].
     *
     * @param out the writer to which this XmlOutput generates results.
     */
    constructor(out: Writer?) : this(PrintWriter(out, true)) {}

    /**
     * Constructs a new XmlOutput based on a [PrintWriter].
     *
     * @param out the writer to which this XmlOutput generates results.
     */
    init {
        this.out = out
        indent = 0
        tagsWritten = 0
    }

    /**
     * Sets the string to print for each level of indentation. The default is a
     * tab. The value must not be `null`. Set this to the empty
     * string to achieve no indentation (note that
     * `[.setCompact](true)` removes indentation *and*
     * newlines).
     */
    fun setIndentString(indentString: String) {
        this.indentString = indentString
    }

    /**
     * Sets whether to detect that tags are empty.
     */
    fun setGlob(glob: Boolean) {
        this.glob = glob
    }

    /**
     * Sets whether to always quote cdata segments (even if they don't contain
     * special characters).
     */
    fun setAlwaysQuoteCData(alwaysQuoteCData: Boolean) {
        this.alwaysQuoteCData = alwaysQuoteCData
    }

    /**
     * Sends a string directly to the output stream, without escaping any
     * characters.  Use with caution!
     */
    fun print(s: String?) {
        out.print(s)
    }

    /**
     * Starts writing a new tag to the stream.  The tag's name must be given and
     * its attributes should be specified by a fully constructed AttrVector
     * object.
     *
     * @param tagName the name of the tag to write.
     * @param attributes an XMLAttrVector containing the attributes to include
     * in the tag.
     */
    fun beginTag(tagName: String?, @Nullable attributes: XMLAttrVector?) {
        beginBeginTag(tagName)
        attributes?.display(out, indent)
        endBeginTag(tagName)
    }

    fun beginBeginTag(tagName: String?) {
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">")
            } else {
                out.println(">")
            }
            inTag = false
        }
        displayIndent(out, indent)
        out.print("<")
        out.print(tagName)
    }

    fun endBeginTag(tagName: String?) {
        if (glob) {
            inTag = true
        } else if (compact) {
            out.print(">")
        } else {
            out.println(">")
        }
        out.flush()
        tagStack.push(tagName)
        indent++
        tagsWritten++
    }

    /**
     * Writes an attribute.
     */
    fun attribute(name: String, value: String?) {
        printAtt(out, name, value)
    }

    /**
     * If we are currently inside the start tag, finishes it off.
     */
    fun beginNode() {
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">")
            } else {
                out.println(">")
            }
            inTag = false
        }
    }

    /**
     * Completes a tag.  This outputs the end tag corresponding to the
     * last exposed beginTag.  The tag name must match the name of the
     * corresponding beginTag.
     * @param tagName the name of the end tag to write.
     */
    fun endTag(tagName: String?) {
        // Check that the end tag matches the corresponding start tag
        val x: String = tagStack.pop()
        assert(x.equals(tagName))

        // Lower the indent and display the end tag
        indent--
        if (inTag) {
            // we're still in the start tag -- this element had no children
            if (compact) {
                out.print("/>")
            } else {
                out.println("/>")
            }
            inTag = false
        } else {
            displayIndent(out, indent)
            out.print("</")
            out.print(tagName)
            if (compact) {
                out.print(">")
            } else {
                out.println(">")
            }
        }
        out.flush()
    }

    /**
     * Writes an empty tag to the stream.  An empty tag is one with no
     * tags inside it, although it may still have attributes.
     *
     * @param tagName the name of the empty tag.
     * @param attributes an XMLAttrVector containing the attributes to
     * include in the tag.
     */
    fun emptyTag(tagName: String?, attributes: XMLAttrVector?) {
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">")
            } else {
                out.println(">")
            }
            inTag = false
        }
        displayIndent(out, indent)
        out.print("<")
        out.print(tagName)
        if (attributes != null) {
            out.print(" ")
            attributes.display(out, indent)
        }
        if (compact) {
            out.print("/>")
        } else {
            out.println("/>")
        }
        out.flush()
        tagsWritten++
    }

    /**
     * Writes a CDATA section.  Such sections always appear on their own line.
     * The nature in which the CDATA section is written depends on the actual
     * string content with respect to these special characters/sequences:
     *
     *  * `&`
     *  * `"`
     *  * `'`
     *  * `<`
     *  * `>`
     *
     * Additionally, the sequence `]]>` is special.
     *
     *  * Content containing no special characters will be left as-is.
     *  * Content containing one or more special characters but not the
     * sequence `]]>` will be enclosed in a CDATA section.
     *  * Content containing special characters AND at least one
     * `]]>` sequence will be left as-is but have all of its
     * special characters encoded as entities.
     *
     * These special treatment rules are required to allow cdata sections
     * to contain XML strings which may themselves contain cdata sections.
     * Traditional CDATA sections **do not nest**.
     */
    fun cdata(data: String?) {
        cdata(data, false)
    }

    /**
     * Writes a CDATA section (as [.cdata]).
     *
     * @param data string to write
     * @param quote if true, quote in a `<![CDATA[`
     * ... `]]>` regardless of the content of
     * `data`; if false, quote only if the content needs it
     */
    fun cdata(@Nullable data: String?, quote: Boolean) {
        var data = data
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">")
            } else {
                out.println(">")
            }
            inTag = false
        }
        if (data == null) {
            data = ""
        }
        var specials = false
        @SuppressWarnings("unused") var cdataEnd = false

        // Scan the string for special characters
        // If special characters are found, scan the string for ']]>'
        if (stringHasXMLSpecials(data)) {
            specials = true
            if (data.contains("]]>")) {
                // TODO: support string that contains cdataEnd literal values
                cdataEnd = true
            }
        }

        // Display the result
        displayIndent(out, indent)
        if (quote || alwaysQuoteCData) {
            out.print("<![CDATA[")
            out.print(data)
            out.println("]]>")
        } else if (!specials) {
            out.println(data)
        } else {
            stringEncodeXML(data, out)
            out.println()
        }
        out.flush()
        tagsWritten++
    }

    /**
     * Writes a String tag; a tag containing nothing but a CDATA section.
     */
    fun stringTag(name: String?, data: String?) {
        beginTag(name, null)
        cdata(data)
        endTag(name)
    }

    /**
     * Writes content.
     */
    fun content(@Nullable content: String?) {
        // This method previously used a LineNumberReader, but that class is
        // susceptible to a form of DoS attack. It uses lots of memory and CPU if a
        // malicious client gives it input with very long lines.
        if (content != null) {
            indent++
            val chars: CharArray = content.toCharArray()
            var prev = 0
            var i = 0
            while (i < chars.size) {
                if (chars[i] == '\n'
                    || chars[i] == '\r' && i + 1 < chars.size && chars[i + 1] == '\n'
                ) {
                    displayIndent(out, indent)
                    out.println(content.substring(prev, i))
                    if (chars[i] == '\r') {
                        ++i
                    }
                    prev = i + 1
                }
                i++
            }
            displayIndent(out, indent)
            out.println(content.substring(prev, chars.size))
            indent--
            out.flush()
        }
        tagsWritten++
    }

    /**
     * Write header. Use default version 1.0.
     */
    fun header() {
        out.println("<?xml version=\"1.0\" ?>")
        out.flush()
        tagsWritten++
    }

    /**
     * Write header, take version as input.
     */
    fun header(version: String?) {
        out.print("<?xml version=\"")
        out.print(version)
        out.println("\" ?>")
        out.flush()
        tagsWritten++
    }

    /**
     * Returns the total number of tags written.
     *
     * @return the total number of tags written to the XML stream.
     */
    fun numTagsWritten(): Int {
        return tagsWritten
    }

    /**
     * Utility for replacing special characters
     * with escape sequences in strings.
     *
     *
     * A StringEscaper starts out as an identity transform in the "mutable"
     * state.  Call [.defineEscape] as many times as necessary to set up
     * mappings, and then call [.makeImmutable] before
     * actually applying the defined transform.  Or,
     * use one of the global mappings pre-defined here.
     */
    internal class StringEscaper : Cloneable {
        @Nullable
        private var translationVector: List<String>?
        private var translationTable: @Nullable Array<String>?

        /**
         * Map character "from" to escape sequence "to".
         */
        fun defineEscape(from: Char, to: String?) {
            val i: Int = from.code
            val translationVector: List<String> = requireNonNull(
                translationVector,
                "translationVector"
            )
            if (i >= translationVector.size()) {
                // Extend list by adding the requisite number of nulls.
                val count: Int = i + 1 - translationVector.size()
                translationVector.addAll(Collections.nCopies(count, null))
            }
            translationVector.set(i, to)
        }

        /**
         * Call this before attempting to escape strings; after this,
         * defineEscape may not be called again.
         */
        @SuppressWarnings("assignment.type.incompatible")
        fun makeImmutable() {
            translationTable = requireNonNull(translationVector, "translationVector").toArray(arrayOfNulls<String>(0))
            translationVector = null
        }

        /**
         * Apply an immutable transformation to the given string.
         */
        fun escapeString(s: String): String {
            var sb: StringBuilder? = null
            val n: Int = s.length()
            for (i in 0 until n) {
                val c: Char = s.charAt(i)
                var escape: String?
                // codes >= 128 (e.g. Euro sign) are always escaped
                escape = if (c.code > 127) {
                    "&#" + Integer.toString(c).toString() + ";"
                } else if (c >= requireNonNull(translationTable, "translationTable").length) {
                    null
                } else {
                    translationTable!![c.code]
                }
                if (escape == null) {
                    if (sb != null) {
                        sb.append(c)
                    }
                } else {
                    if (sb == null) {
                        sb = StringBuilder(n * 2)
                        sb.append(s, 0, i)
                    }
                    sb.append(escape)
                }
            }
            return if (sb == null) {
                s
            } else {
                sb.toString()
            }
        }

        @Override
        protected fun clone(): StringEscaper {
            val clone = StringEscaper()
            if (translationVector != null) {
                clone.translationVector = ArrayList(translationVector)
            }
            if (translationTable != null) {
                clone.translationTable = translationTable.clone()
            }
            return clone
        }

        /**
         * Create a mutable escaper from an existing escaper, which may
         * already be immutable.
         */
        val mutableClone: StringEscaper
            get() {
                val clone = clone()
                if (clone.translationVector == null) {
                    clone.translationVector = Lists.newArrayList(
                        requireNonNull(clone.translationTable, "clone.translationTable")
                    )
                    clone.translationTable = null
                }
                return clone
            }

        /** Identity transform.  */
        init {
            translationVector = ArrayList()
        }

        companion object {
            val XML_ESCAPER: StringEscaper? = null
            val XML_NUMERIC_ESCAPER: StringEscaper? = null
            val HTML_ESCAPER: StringEscaper? = null
            val URL_ARG_ESCAPER: StringEscaper? = null
            val URL_ESCAPER: StringEscaper? = null

            init {
                HTML_ESCAPER = StringEscaper()
                HTML_ESCAPER.defineEscape('&', "&amp;")
                HTML_ESCAPER.defineEscape('"', "&quot;")
                //      htmlEscaper.defineEscape('\'',"&apos;");
                HTML_ESCAPER.defineEscape('\'', "&#39;")
                HTML_ESCAPER.defineEscape('<', "&lt;")
                HTML_ESCAPER.defineEscape('>', "&gt;")
                XML_NUMERIC_ESCAPER = StringEscaper()
                XML_NUMERIC_ESCAPER.defineEscape('&', "&#38;")
                XML_NUMERIC_ESCAPER.defineEscape('"', "&#34;")
                XML_NUMERIC_ESCAPER.defineEscape('\'', "&#39;")
                XML_NUMERIC_ESCAPER.defineEscape('<', "&#60;")
                XML_NUMERIC_ESCAPER.defineEscape('>', "&#62;")
                URL_ARG_ESCAPER = StringEscaper()
                URL_ARG_ESCAPER.defineEscape('?', "%3f")
                URL_ARG_ESCAPER.defineEscape('&', "%26")
                URL_ESCAPER = URL_ARG_ESCAPER.mutableClone
                URL_ESCAPER.defineEscape('%', "%%")
                URL_ESCAPER.defineEscape('"', "%22")
                URL_ESCAPER.defineEscape('\r', "+")
                URL_ESCAPER.defineEscape('\n', "+")
                URL_ESCAPER.defineEscape(' ', "+")
                URL_ESCAPER.defineEscape('#', "%23")
                HTML_ESCAPER.makeImmutable()
                XML_ESCAPER = HTML_ESCAPER
                XML_NUMERIC_ESCAPER.makeImmutable()
                URL_ARG_ESCAPER.makeImmutable()
                URL_ESCAPER.makeImmutable()
            }
        }
    }

    /** List of attribute names and values.  */
    class XMLAttrVector {
        fun display(out: PrintWriter?, indent: Int) {
            throw UnsupportedOperationException()
        }
    }

    companion object {
        /** Prints an XML attribute name and value for string `val`.  */
        private fun printAtt(pw: PrintWriter, name: String, @Nullable `val`: String?) {
            if (`val` != null /* && !val.equals("") */) {
                pw.print(" ")
                pw.print(name)
                pw.print("=\"")
                pw.print(escapeForQuoting(`val`))
                pw.print("\"")
            }
        }

        /**
         * Encode a String for XML output, displaying it to a PrintWriter.
         * The String to be encoded is displayed, except that
         * special characters are converted into entities.
         * @param input a String to convert.
         * @param out a PrintWriter to which to write the results.
         */
        private fun stringEncodeXML(input: String, out: PrintWriter) {
            for (i in 0 until input.length()) {
                val c: Char = input.charAt(i)
                when (c) {
                    '<', '>', '"', '\'', '&', '\t', '\n', '\r' -> out.print("&#" + c.code + ";")
                    else -> out.print(c)
                }
            }
        }

        private fun escapeForQuoting(`val`: String): String {
            return StringEscaper.XML_NUMERIC_ESCAPER!!.escapeString(`val`)
        }

        /**
         * Returns whether a string contains any XML special characters.
         *
         *
         * If this function returns true, the string will need to be
         * encoded either using the stringEncodeXML function above or using a
         * CDATA section.  Note that MSXML has a nasty bug whereby whitespace
         * characters outside of a CDATA section are lost when parsing.  To
         * avoid hitting this bug, this method treats many whitespace characters
         * as "special".
         *
         * @param input the String to scan for XML special characters.
         * @return true if the String contains any such characters.
         */
        private fun stringHasXMLSpecials(input: String): Boolean {
            for (i in 0 until input.length()) {
                val c: Char = input.charAt(i)
                when (c) {
                    '<', '>', '"', '\'', '&', '\t', '\n', '\r' -> return true
                    else -> {}
                }
            }
            return false
        }
    }
}
