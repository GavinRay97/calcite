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

import org.apache.calcite.avatica.util.ByteString
import org.apache.commons.lang3.StringUtils
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.util.zip.DeflaterOutputStream

/**
 * A collection of functions used in compression and decompression.
 */
object CompressionFunctions {
    /**
     * MySql Compression is based on zlib.
     * [Deflater](https://docs.oracle.com/javase/8/docs/api/java/util/zip/Deflater.html)
     * is used to implement compression.
     */
    @Nullable
    fun compress(@Nullable data: String?): ByteString? {
        return try {
            if (data == null) {
                return null
            }
            if (StringUtils.isEmpty(data)) {
                return ByteString(ByteArray(0))
            }
            val outputStream = ByteArrayOutputStream()
            val dataLength: ByteBuffer = ByteBuffer.allocate(4)
            dataLength.order(ByteOrder.LITTLE_ENDIAN)
            dataLength.putInt(data.length())
            outputStream.write(dataLength.array())
            val inflaterStream = DeflaterOutputStream(outputStream)
            inflaterStream.write(data.getBytes(Charset.defaultCharset()))
            inflaterStream.close()
            ByteString(outputStream.toByteArray())
        } catch (e: IOException) {
            null
        }
    }
}
