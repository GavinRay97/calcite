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

import java.io.File

/**
 * Source of data.
 */
interface Source {
    fun url(): URL
    fun file(): File
    fun path(): String

    @Throws(IOException::class)
    fun reader(): Reader?

    @Throws(IOException::class)
    fun openStream(): InputStream?
    fun protocol(): String

    /** Looks for a suffix on a path and returns
     * either the path with the suffix removed
     * or the original path.  */
    fun trim(suffix: String?): Source?

    /** Looks for a suffix on a path and returns
     * either the path with the suffix removed
     * or null.  */
    @Nullable
    fun trimOrNull(suffix: String?): Source?

    /** Returns a source whose path concatenates this with a child.
     *
     *
     * For example,
     *
     *  * source("/foo").append(source("bar"))
     * returns source("/foo/bar")
     *  * source("/foo").append(source("/bar"))
     * returns source("/bar")
     * because "/bar" was already absolute
     *
     */
    fun append(child: Source?): Source?

    /** Returns a relative source, if this source is a child of a given base.
     *
     *
     * For example,
     *
     *  * source("/foo/bar").relative(source("/foo"))
     * returns source("bar")
     *  * source("/baz/bar").relative(source("/foo"))
     * returns source("/baz/bar")
     *
     */
    fun relative(source: Source?): Source?
}
