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

import java.io.IOException

/**
 * Utilities for connecting to REST services such as Splunk via HTTP.
 */
object HttpUtils {
    @Throws(IOException::class)
    fun getURLConnection(url: String?): HttpURLConnection {
        return URL(url).openConnection() as HttpURLConnection
    }

    fun appendURLEncodedArgs(
        out: StringBuilder, args: Map<String?, String?>
    ) {
        var i = 0
        try {
            for (me in args.entrySet()) {
                if (i++ != 0) {
                    out.append("&")
                }
                out.append(URLEncoder.encode(me.getKey(), "UTF-8"))
                    .append("=")
                    .append(URLEncoder.encode(me.getValue(), "UTF-8"))
            }
        } catch (ignore: UnsupportedEncodingException) {
            // ignore
        }
    }

    fun appendURLEncodedArgs(
        out: StringBuilder, vararg args: CharSequence?
    ) {
        if (args.size % 2 != 0) {
            throw IllegalArgumentException(
                "args should contain an even number of items"
            )
        }
        try {
            var appended = 0
            var i = 0
            while (i < args.size) {
                if (args[i + 1] == null) {
                    i += 2
                    continue
                }
                if (appended++ > 0) {
                    out.append("&")
                }
                out.append(URLEncoder.encode(args[i].toString(), "UTF-8"))
                    .append("=")
                    .append(URLEncoder.encode(args[i + 1].toString(), "UTF-8"))
                i += 2
            }
        } catch (ignore: UnsupportedEncodingException) {
            // ignore
        }
    }

    @Throws(IOException::class)
    fun post(
        url: String?,
        data: CharSequence?,
        headers: Map<String?, String?>?
    ): InputStream {
        return post(url, data, headers, 10000, 60000)
    }

    @Throws(IOException::class)
    fun post(
        url: String?,
        data: CharSequence?,
        headers: Map<String?, String?>?,
        cTimeout: Int,
        rTimeout: Int
    ): InputStream {
        return executeMethod(
            if (data == null) "GET" else "POST", url, data, headers,
            cTimeout, rTimeout
        )
    }

    @Throws(IOException::class)
    fun executeMethod(
        method: String?, url: String?,
        data: CharSequence?, headers: Map<String?, String?>?,
        cTimeout: Int, rTimeout: Int
    ): InputStream {
        // NOTE: do not log "data" or "url"; may contain user name or password.
        val conn: HttpURLConnection = getURLConnection(url)
        conn.setRequestMethod(method)
        conn.setReadTimeout(rTimeout)
        conn.setConnectTimeout(cTimeout)
        if (headers != null) {
            for (me in headers.entrySet()) {
                conn.setRequestProperty(me.getKey(), me.getValue())
            }
        }
        if (data == null) {
            return conn.getInputStream()
        }
        conn.setDoOutput(true)
        OutputStreamWriter(
            conn.getOutputStream(),
            StandardCharsets.UTF_8
        ).use { w ->
            w.write(data.toString())
            w.flush() // Get the response
            return conn.getInputStream()
        }
    }
}
