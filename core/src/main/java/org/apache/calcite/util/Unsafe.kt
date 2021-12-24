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

import java.io.StringWriter

/**
 * Contains methods that call JDK methods that the
 * [forbidden
 * APIs checker](https://github.com/policeman-tools/forbidden-apis) does not approve of.
 *
 *
 * This class is excluded from the check, so methods called via this class
 * will not fail the build.
 */
object Unsafe {
    /** Calls [System.exit].  */
    fun systemExit(status: Int) {
        System.exit(status)
    }

    /** Calls [Object.notifyAll].  */
    fun notifyAll(o: Object) {
        o.notifyAll()
    }

    /** Calls [Object.wait].  */
    @SuppressWarnings("WaitNotInLoop")
    @Throws(InterruptedException::class)
    fun wait(o: Object) {
        o.wait()
    }

    /** Clears the contents of a [StringWriter].  */
    @SuppressWarnings("JdkObsolete")
    fun clear(sw: StringWriter) {
        // Included in this class because StringBuffer is banned.
        sw.getBuffer().setLength(0)
    }

    /** Helper for the SQL `REGEXP_REPLACE` function.
     *
     *
     * It is marked "unsafe" because it uses [StringBuffer];
     * Versions of [Matcher.appendReplacement]
     * and [Matcher.appendTail]
     * that use [StringBuilder] are not available until JDK 9.  */
    @SuppressWarnings("JdkObsolete")
    fun regexpReplace(
        s: String, pattern: Pattern,
        replacement: String?, pos: Int, occurrence: Int
    ): String {
        Bug.upgrade("when we drop JDK 8, replace StringBuffer with StringBuilder")
        val sb = StringBuffer()
        val input: String
        if (pos != 1) {
            sb.append(s, 0, pos - 1)
            input = s.substring(pos - 1)
        } else {
            input = s
        }
        var count = 0
        val matcher: Matcher = pattern.matcher(input)
        while (matcher.find()) {
            if (occurrence == 0) {
                matcher.appendReplacement(sb, replacement)
            } else if (++count == occurrence) {
                matcher.appendReplacement(sb, replacement)
                break
            }
        }
        matcher.appendTail(sb)
        return sb.toString()
    }
}
