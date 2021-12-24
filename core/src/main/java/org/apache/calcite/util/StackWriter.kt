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

import java.io.FilterWriter

/**
 * A helper class for generating formatted text. StackWriter keeps track of
 * nested formatting state like indentation level and quote escaping. Typically,
 * it is inserted between a PrintWriter and the real Writer; directives are
 * passed straight through the PrintWriter via the write method, as in the
 * following example:
 *
 * <blockquote><pre>`
 * StringWriter sw = new StringWriter();
 * StackWriter stackw = new StackWriter(sw, StackWriter.INDENT_SPACE4);
 * PrintWriter pw = new PrintWriter(stackw);
 * pw.write(StackWriter.INDENT);
 * pw.print("execute remote(link_name,");
 * pw.write(StackWriter.OPEN_SQL_STRING_LITERAL);
 * pw.println();
 * pw.write(StackWriter.INDENT);
 * pw.println("select * from t where c > 'alabama'");
 * pw.write(StackWriter.OUTDENT);
 * pw.write(StackWriter.CLOSE_SQL_STRING_LITERAL);
 * pw.println(");");
 * pw.write(StackWriter.OUTDENT);
 * pw.close();
 * System.out.println(sw.toString());
`</pre></blockquote> *
 *
 *
 * which produces the following output:
 *
 * <blockquote><pre>`
 * execute remote(link_name,'
 * select * from t where c > ''alabama''
 * ');
`</pre></blockquote> *
 */
class StackWriter
/**
 * Creates a new StackWriter on top of an existing Writer, with the
 * specified string to be used for each level of indentation.
 *
 * @param writer      underlying writer
 * @param indentation indentation unit such as [.INDENT_TAB] or
 * [.INDENT_SPACE4]
 */(writer: Writer?, private val indentation: String) : FilterWriter(writer) {
    //~ Instance fields --------------------------------------------------------
    private var indentationDepth = 0
    private var needIndent = false
    private val quoteStack: Deque<Character> = ArrayDeque()

    //~ Constructors -----------------------------------------------------------
    //~ Methods ----------------------------------------------------------------
    @Throws(IOException::class)
    private fun indentIfNeeded() {
        if (needIndent) {
            for (i in 0 until indentationDepth) {
                out.write(indentation)
            }
            needIndent = false
        }
    }

    @Throws(IOException::class)
    private fun writeQuote(quoteChar: Character) {
        indentIfNeeded()
        var n = 1
        for (quote in quoteStack) {
            if (quote.equals(quoteChar)) {
                n *= 2
            }
        }
        for (i in 0 until n) {
            out.write(quoteChar)
        }
    }

    @Throws(IOException::class)
    private fun pushQuote(quoteChar: Character) {
        writeQuote(quoteChar)
        quoteStack.push(quoteChar)
    }

    @Throws(IOException::class)
    private fun popQuote(quoteChar: Character) {
        val pop: Character = quoteStack.pop()
        assert(pop.equals(quoteChar))
        writeQuote(quoteChar)
    }

    // implement Writer
    @Override
    @Throws(IOException::class)
    fun write(c: Int) {
        when (c) {
            INDENT -> indentationDepth++
            OUTDENT -> indentationDepth--
            OPEN_SQL_STRING_LITERAL -> pushQuote(SINGLE_QUOTE)
            CLOSE_SQL_STRING_LITERAL -> popQuote(SINGLE_QUOTE)
            OPEN_SQL_IDENTIFIER -> pushQuote(DOUBLE_QUOTE)
            CLOSE_SQL_IDENTIFIER -> popQuote(DOUBLE_QUOTE)
            '\n' -> {
                out.write(c)
                needIndent = true
            }
            '\r' ->
                // NOTE jvs 3-Jan-2006:  suppress indentIfNeeded() in this case
                // so that we don't get spurious diffs on Windows vs. Linux
                out.write(c)
            '\'' -> writeQuote(SINGLE_QUOTE)
            '"' -> writeQuote(DOUBLE_QUOTE)
            else -> {
                indentIfNeeded()
                out.write(c)
            }
        }
    }

    // implement Writer
    @Override
    @Throws(IOException::class)
    fun write(cbuf: CharArray, off: Int, len: Int) {
        // TODO: something more efficient using searches for
        // special characters
        for (i in off until off + len) {
            write(cbuf[i].code)
        }
    }

    // implement Writer
    @Override
    @Throws(IOException::class)
    fun write(str: String, off: Int, len: Int) {
        // TODO: something more efficient using searches for
        // special characters
        for (i in off until off + len) {
            write(str.charAt(i))
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /**
         * Directive for increasing the indentation level.
         */
        const val INDENT = -0xfffffff

        /**
         * Directive for decreasing the indentation level.
         */
        const val OUTDENT = -0xffffffe

        /**
         * Directive for beginning an SQL string literal.
         */
        const val OPEN_SQL_STRING_LITERAL = -0xffffffd

        /**
         * Directive for ending an SQL string literal.
         */
        const val CLOSE_SQL_STRING_LITERAL = -0xffffffc

        /**
         * Directive for beginning an SQL identifier.
         */
        const val OPEN_SQL_IDENTIFIER = -0xffffffb

        /**
         * Directive for ending an SQL identifier.
         */
        const val CLOSE_SQL_IDENTIFIER = -0xffffffa

        /**
         * Tab indentation.
         */
        const val INDENT_TAB = "\t"

        /**
         * Four-space indentation.
         */
        const val INDENT_SPACE4 = "    "
        private val SINGLE_QUOTE: Character = '\''
        private val DOUBLE_QUOTE: Character = '"'

        /**
         * Writes an SQL string literal.
         *
         * @param pw PrintWriter on which to write
         * @param s  text of literal
         */
        fun printSqlStringLiteral(pw: PrintWriter, s: String?) {
            pw.write(OPEN_SQL_STRING_LITERAL)
            pw.print(s)
            pw.write(CLOSE_SQL_STRING_LITERAL)
        }

        /**
         * Writes an SQL identifier.
         *
         * @param pw PrintWriter on which to write
         * @param s  identifier
         */
        fun printSqlIdentifier(pw: PrintWriter, s: String?) {
            pw.write(OPEN_SQL_IDENTIFIER)
            pw.print(s)
            pw.write(CLOSE_SQL_IDENTIFIER)
        }
    }
}
