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

import org.apache.commons.io.input.ReaderInputStream

/**
 * Utilities for [Source].
 */
object Sources {
    fun of(file: File?): Source {
        return FileSource(file)
    }

    fun of(url: URL?): Source {
        return FileSource(url)
    }

    fun file(@Nullable baseDirectory: File?, fileName: String?): Source {
        val file = File(fileName)
        return if (baseDirectory != null && !file.isAbsolute()) {
            of(File(baseDirectory, fileName))
        } else {
            of(file)
        }
    }

    /**
     * Create [Source] from a generic text source such as string, [java.nio.CharBuffer]
     * or text file. Useful when data is already in memory or can't be directly read from
     * a file or url.
     *
     * @param source generic "re-redable" source of characters
     * @return `Source` delegate for `CharSource` (can't be null)
     * @throws NullPointerException when `source` is null
     */
    fun fromCharSource(source: CharSource): Source {
        return GuavaCharSource(source)
    }

    fun url(url: String): Source {
        return try {
            of(URL(url))
        } catch (e: MalformedURLException) {
            throw RuntimeException("Malformed URL: '$url'", e)
        } catch (e: IllegalArgumentException) {
            throw RuntimeException("Malformed URL: '$url'", e)
        }
    }

    /** Looks for a suffix on a path and returns
     * either the path with the suffix removed
     * or null.  */
    @Nullable
    private fun trimOrNull(s: String, suffix: String): String? {
        return if (s.endsWith(suffix)) s.substring(0, s.length() - suffix.length()) else null
    }

    private fun isFile(source: Source): Boolean {
        return source.protocol().equals("file")
    }

    /** Adapter for [CharSource].  */
    private class GuavaCharSource(charSource: CharSource) : Source {
        private val charSource: CharSource

        init {
            this.charSource = Objects.requireNonNull(charSource, "charSource")
        }

        private fun unsupported(): UnsupportedOperationException {
            return UnsupportedOperationException(
                String.format(Locale.ROOT, "Invalid operation for '%s' protocol", protocol())
            )
        }

        @Override
        override fun url(): URL {
            throw unsupported()
        }

        @Override
        override fun file(): File {
            throw unsupported()
        }

        @Override
        override fun path(): String {
            throw unsupported()
        }

        @Override
        @Throws(IOException::class)
        override fun reader(): Reader {
            return charSource.openStream()
        }

        @Override
        @Throws(IOException::class)
        override fun openStream(): InputStream {
            // use charSource.asByteSource() once calcite can use guava v21+
            return ReaderInputStream(reader(), StandardCharsets.UTF_8)
        }

        @Override
        override fun protocol(): String {
            return "memory"
        }

        @Override
        override fun trim(suffix: String?): Source {
            throw unsupported()
        }

        @Override
        @Nullable
        override fun trimOrNull(suffix: String?): Source {
            throw unsupported()
        }

        @Override
        override fun append(child: Source?): Source {
            throw unsupported()
        }

        @Override
        override fun relative(source: Source?): Source {
            throw unsupported()
        }

        @Override
        override fun toString(): String {
            return getClass().getSimpleName() + "{" + protocol() + "}"
        }
    }

    /** Implementation of [Source] on the top of a [File] or
     * [URL].  */
    private class FileSource : Source {
        @Nullable
        private val file: File?
        private val url: URL

        /**
         * A flag indicating if the url is deduced from the file object.
         */
        private val urlGenerated: Boolean

        private constructor(url: URL) {
            this.url = Objects.requireNonNull(url, "url")
            file = urlToFile(url)
            urlGenerated = false
        }

        private constructor(file: File) {
            this.file = Objects.requireNonNull(file, "file")
            url = fileToUrl(file)
            urlGenerated = true
        }

        private fun fileNonNull(): File {
            return Objects.requireNonNull(file, "file")
        }

        @Override
        override fun toString(): String {
            return (if (urlGenerated) fileNonNull() else url).toString()
        }

        @Override
        override fun url(): URL {
            return url
        }

        @Override
        override fun file(): File {
            if (file == null) {
                throw UnsupportedOperationException()
            }
            return file
        }

        @Override
        override fun protocol(): String {
            return if (file != null) "file" else url.getProtocol()
        }

        @Override
        override fun path(): String {
            return if (file != null) {
                file.getPath()
            } else try {
                // Decode %20 and friends
                url.toURI().getSchemeSpecificPart()
            } catch (e: URISyntaxException) {
                throw IllegalArgumentException("Unable to convert URL $url to URI", e)
            }
        }

        @Override
        @Throws(IOException::class)
        override fun reader(): Reader {
            val `is`: InputStream
            if (path().endsWith(".gz")) {
                val fis: InputStream = openStream()
                `is` = GZIPInputStream(fis)
            } else {
                `is` = openStream()
            }
            return InputStreamReader(`is`, StandardCharsets.UTF_8)
        }

        @Override
        @Throws(IOException::class)
        override fun openStream(): InputStream {
            return if (file != null) {
                FileInputStream(file)
            } else {
                url.openStream()
            }
        }

        @Override
        fun trim(suffix: String): Source {
            val x: Source? = trimOrNull(suffix)
            return if (x == null) this else x
        }

        @Override
        @Nullable
        fun trimOrNull(suffix: String): Source? {
            return if (!urlGenerated) {
                val s = trimOrNull(url.toExternalForm(), suffix)
                if (s == null) null else url(s)
            } else {
                val s = trimOrNull(fileNonNull().getPath(), suffix)
                if (s == null) null else of(File(s))
            }
        }

        @Override
        fun append(child: Source): Source {
            if (isFile(child)) {
                if (child.file().isAbsolute()) {
                    return child
                }
            } else {
                try {
                    val uri: URI = child.url().toURI()
                    if (!uri.isOpaque()) {
                        // The URL is "absolute" (it starts with a slash)
                        return child
                    }
                } catch (e: URISyntaxException) {
                    throw IllegalArgumentException("Unable to convert URL " + child.url().toString() + " to URI", e)
                }
            }
            val path: String = child.path()
            return if (!urlGenerated) {
                val encodedPath: String = File(".").toURI().relativize(File(path).toURI())
                    .getRawSchemeSpecificPart()
                url(url.toString() + "/" + encodedPath)
            } else {
                file(file, path)
            }
        }

        @Override
        fun relative(parent: Source): Source {
            return if (isFile(parent)) {
                if (isFile(this)
                    && fileNonNull().getPath().startsWith(parent.file().getPath())
                ) {
                    val rest: String = fileNonNull().getPath().substring(parent.file().getPath().length())
                    if (rest.startsWith(File.separator)) {
                        return file(null, rest.substring(File.separator.length()))
                    }
                }
                this
            } else {
                if (!isFile(this)) {
                    val rest = trimOrNull(
                        url.toExternalForm(),
                        parent.url().toExternalForm()
                    )
                    if (rest != null
                        && rest.startsWith("/")
                    ) {
                        return file(null, rest.substring(1))
                    }
                }
                this
            }
        }

        companion object {
            @Nullable
            private fun urlToFile(url: URL): File? {
                if (!"file".equals(url.getProtocol())) {
                    return null
                }
                val uri: URI
                uri = try {
                    url.toURI()
                } catch (e: URISyntaxException) {
                    throw IllegalArgumentException("Unable to convert URL $url to URI", e)
                }
                return if (uri.isOpaque()) {
                    // It is like file:test%20file.c++
                    // getSchemeSpecificPart would return "test file.c++"
                    File(uri.getSchemeSpecificPart())
                } else Paths.get(uri).toFile()
                // See https://stackoverflow.com/a/17870390/1261287
            }

            private fun fileToUrl(file: File): URL {
                var filePath: String = file.getPath()
                if (!file.isAbsolute()) {
                    // convert relative file paths
                    filePath = filePath.replace(File.separatorChar, '/')
                    if (file.isDirectory() && !filePath.endsWith("/")) {
                        filePath += "/"
                    }
                    return try {
                        // We need to encode path. For instance, " " should become "%20"
                        // That is why java.net.URLEncoder.encode(java.lang.String, java.lang.String) is not
                        // suitable because it replaces " " with "+".
                        val encodedPath: String = URI(null, null, filePath, null).getRawPath()
                        URL("file", null, 0, encodedPath)
                    } catch (e: MalformedURLException) {
                        throw IllegalArgumentException("Unable to create URL for file $filePath", e)
                    } catch (e: URISyntaxException) {
                        throw IllegalArgumentException("Unable to create URL for file $filePath", e)
                    }
                }
                var uri: URI? = null
                return try {
                    // convert absolute file paths
                    uri = file.toURI()
                    uri.toURL()
                } catch (e: SecurityException) {
                    throw IllegalArgumentException("No access to the underlying file $filePath", e)
                } catch (e: MalformedURLException) {
                    throw IllegalArgumentException("Unable to convert URI $uri to URL", e)
                }
            }
        }
    }
}
