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

import org.checkerframework.checker.nullness.qual.PolyNull

/**
 * Serializable wrapper around a [Charset].
 *
 *
 * It serializes itself by writing out the name of the character set, for
 * example "ISO-8859-1". On the other side, it deserializes itself by looking
 * for a charset with the same name.
 *
 *
 * A SerializableCharset is immutable.
 */
class SerializableCharset private constructor(charset: Charset?) : Serializable {
    //~ Instance fields --------------------------------------------------------
    private var charset: Charset?
    private var charsetName: String
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SerializableCharset. External users should call
     * [.forCharset].
     *
     * @param charset Character set; must not be null
     */
    init {
        assert(charset != null)
        this.charset = charset
        charsetName = charset.name()
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Per [Serializable].
     */
    @Throws(IOException::class)
    private fun writeObject(out: ObjectOutputStream) {
        out.writeObject(charset.name())
    }

    /**
     * Per [Serializable].
     */
    @SuppressWarnings("JdkObsolete")
    @Throws(IOException::class, ClassNotFoundException::class)
    private fun readObject(`in`: ObjectInputStream) {
        charsetName = `in`.readObject()
        charset = requireNonNull(
            Charset.availableCharsets().get(charsetName)
        ) { "charset is not found: $charsetName" }
    }

    /**
     * Returns the wrapped [Charset].
     *
     * @return the wrapped Charset
     */
    fun getCharset(): Charset? {
        return charset
    }

    companion object {
        /**
         * Returns a SerializableCharset wrapping the given Charset, or null if the
         * `charset` is null.
         *
         * @param charset Character set to wrap, or null
         * @return Wrapped charset
         */
        @PolyNull
        fun forCharset(@PolyNull charset: Charset?): SerializableCharset? {
            return if (charset == null) {
                null
            } else SerializableCharset(charset)
        }
    }
}
