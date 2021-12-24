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
 * Extends the SocketFactory object with the main functionality being that the
 * created sockets inherit a set of options whose values are set in the
 * SocketFactoryImpl.
 *
 * <blockquote><pre>
 * 1.  SO_KEEPALIVE          - is keepalive enabled?
 * 2.  OOBINLINE             - is out of band in-line enabled?
 * 3.  SO_REUSEADDR          - should the address be reused?
 * 4.  TCP_NODELAY           - should data buffering for tcp be used?
 * 5.  SO_RCVBUF             - size of receive buffer
 * 6.  SO_SNDBUF             - size of send buffer
 * 7.  SO_TIMEOUT            - read timeout (millisecs)
 * 8.  SO_CONNECT_TIMEOUT    - connect timeout (millisecs)
 * 9.  SO_LINGER             - is lingering enabled?
 * 10. LINGER                - amount of time to linger (seconds)
</pre></blockquote> *
 */
class SocketFactoryImpl : SocketFactory() {
    @Override
    @Throws(IOException::class)
    fun createSocket(): Socket {
        val s = Socket()
        return applySettings(s)
    }

    /**
     * Applies the current settings to the given socket.
     *
     * @param s Socket to apply the settings to
     * @return Socket the input socket
     */
    protected fun applySettings(s: Socket): Socket {
        try {
            s.setKeepAlive(SO_KEEPALIVE)
            s.setOOBInline(OOBINLINE)
            s.setReuseAddress(SO_REUSEADDR)
            s.setTcpNoDelay(TCP_NODELAY)
            s.setOOBInline(OOBINLINE)
            s.setReceiveBufferSize(SO_RCVBUF)
            s.setSendBufferSize(SO_SNDBUF)
            s.setSoTimeout(SO_TIMEOUT)
            s.setSoLinger(SO_LINGER, LINGER)
        } catch (e: SocketException) {
            throw RuntimeException(e)
        }
        return s
    }

    @Override
    @Throws(IOException::class)
    fun createSocket(host: String?, port: Int): Socket {
        val s: Socket = createSocket()
        s.connect(InetSocketAddress(host, port), SO_CONNECT_TIMEOUT)
        return s
    }

    @Override
    @Throws(IOException::class)
    fun createSocket(host: InetAddress?, port: Int): Socket {
        val s: Socket = createSocket()
        s.connect(InetSocketAddress(host, port), SO_CONNECT_TIMEOUT)
        return s
    }

    @Override
    @Throws(IOException::class)
    fun createSocket(
        host: String?, port: Int, local: InetAddress?,
        localPort: Int
    ): Socket {
        val s: Socket = createSocket()
        s.bind(InetSocketAddress(local, localPort))
        s.connect(InetSocketAddress(host, port), SO_CONNECT_TIMEOUT)
        return s
    }

    @Override
    @Throws(IOException::class)
    fun createSocket(
        host: InetAddress?, port: Int,
        local: InetAddress?, localPort: Int
    ): Socket {
        val s: Socket = createSocket()
        s.bind(InetSocketAddress(local, localPort))
        s.connect(InetSocketAddress(host, port), SO_CONNECT_TIMEOUT)
        return s
    }

    companion object {
        /**
         * Whether keep-alives should be sent.
         */
        const val SO_KEEPALIVE = false

        /**
         * Whether out-of-band in-line is enabled.
         */
        const val OOBINLINE = false

        /**
         * Whether the address should be reused.
         */
        const val SO_REUSEADDR = false

        /**
         * Whether to not buffer send(s).
         */
        const val TCP_NODELAY = true

        /**
         * Size of receiving buffer.
         */
        const val SO_RCVBUF = 8192

        /**
         * Size of sending buffer iff needed.
         */
        const val SO_SNDBUF = 1024

        /**
         * Read timeout in milliseconds.
         */
        const val SO_TIMEOUT = 12000

        /**
         * Connect timeout in milliseconds.
         */
        const val SO_CONNECT_TIMEOUT = 5000

        /**
         * Enabling lingering with 0-timeout will cause the socket to be
         * closed forcefully upon execution of `close()`.
         */
        const val SO_LINGER = true

        /**
         * Amount of time to linger.
         */
        const val LINGER = 0

        /**
         * Returns a copy of the environment's default socket factory.
         *
         * @see javax.net.SocketFactory.getDefault
         */
        val default: SocketFactory
            get() = SocketFactoryImpl()
    }
}
