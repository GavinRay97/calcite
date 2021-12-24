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
package org.apache.calcite.util.javac

import org.apache.calcite.config.CalciteSystemProperty

/**
 * `JaninoCompiler` implements the [JavaCompiler] interface by
 * calling [Janino](http://www.janino.net).
 */
class JaninoCompiler  //~ Constructors -----------------------------------------------------------
    : JavaCompiler {
    //~ Instance fields --------------------------------------------------------
    override var args = JaninoCompilerArgs()

    // REVIEW jvs 28-June-2004:  pool this instance?  Is it thread-safe?
    @Nullable
    private override var classLoader: AccountingClassLoader? = null

    //~ Methods ----------------------------------------------------------------
    // implement JavaCompiler
    @Override
    override fun compile() {
        // REVIEW: SWZ: 3/12/2006: When this method is invoked multiple times,
        // it creates a series of AccountingClassLoader objects, each with
        // the previous as its parent ClassLoader.  If we refactored this
        // class and its callers to specify all code to compile in one
        // go, we could probably just use a single AccountingClassLoader.
        val destdir: String = requireNonNull(args.destdir, "args.destdir")
        val fullClassName: String = requireNonNull(args.fullClassName, "args.fullClassName")
        val source: String = requireNonNull(args.source, "args.source")
        var parentClassLoader: ClassLoader? = args.getClassLoader()
        if (classLoader != null) {
            parentClassLoader = classLoader
        }
        val sourceMap: Map<String, ByteArray> = HashMap()
        sourceMap.put(
            ClassFile.getSourceResourceName(fullClassName),
            source.getBytes(StandardCharsets.UTF_8)
        )
        val sourceFinder = MapResourceFinder(sourceMap)
        classLoader = AccountingClassLoader(
            parentClassLoader,
            sourceFinder,
            null,
            if (destdir == null) null else File(destdir)
        )
        val classLoader = classLoader
        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            classLoader.setDebuggingInfo(true, true, true)
        }
        try {
            classLoader.loadClass(fullClassName)
        } catch (ex: ClassNotFoundException) {
            throw RuntimeException("while compiling $fullClassName", ex)
        }
    }

    // implement JavaCompiler
    @Override
    fun getArgs(): JavaCompilerArgs {
        return args
    }

    // implement JavaCompiler
    @Override
    fun getClassLoader(): ClassLoader {
        return accountingClassLoader
    }

    private val accountingClassLoader: AccountingClassLoader
        private get() = requireNonNull(classLoader, "classLoader is null. Need to call #compile()")

    // implement JavaCompiler
    @get:Override
    override val totalByteCodeSize: Int
        get() = accountingClassLoader.getTotalByteCodeSize()
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Arguments to an invocation of the Janino compiler.
     */
    class JaninoCompilerArgs : JavaCompilerArgs() {
        @Nullable
        var destdir: String? = null

        @Nullable
        var fullClassName: String? = null

        @Nullable
        var source: String? = null

        @Override
        override fun supportsSetSource(): Boolean {
            return true
        }

        @Override
        override fun setDestdir(destdir: String?) {
            super.setDestdir(destdir)
            this.destdir = destdir
        }

        @Override
        override fun setSource(source: String?, fileName: String?) {
            this.source = source
            addFile(fileName)
        }

        @Override
        override fun setFullClassName(fullClassName: String?) {
            this.fullClassName = fullClassName
        }
    }

    /**
     * Refinement of JavaSourceClassLoader which keeps track of the total
     * bytecode length of the classes it has compiled.
     */
    private class AccountingClassLoader internal constructor(
        parentClassLoader: ClassLoader?,
        sourceFinder: ResourceFinder?,
        @Nullable optionalCharacterEncoding: String?,
        @Nullable destDir: File?
    ) : JavaSourceClassLoader(
        parentClassLoader,
        sourceFinder,
        optionalCharacterEncoding
    ) {
        @Nullable
        private val destDir: File?
        private var nBytes = 0

        init {
            this.destDir = destDir
        }

        fun getTotalByteCodeSize(): Int {
            return nBytes
        }

        @Override
        @Nullable
        @Throws(ClassNotFoundException::class)
        fun generateBytecodes(name: String?): Map<String, ByteArray>? {
            val map: Map<String, ByteArray> = super.generateBytecodes(name) ?: return null
            if (destDir != null) {
                try {
                    for (entry in map.entrySet()) {
                        val file = File(destDir, entry.getKey() + ".class")
                        val fos = FileOutputStream(file)
                        fos.write(entry.getValue())
                        fos.close()
                    }
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            }

            // NOTE jvs 18-Oct-2006:  Janino has actually compiled everything
            // to bytecode even before all of the classes have actually
            // been loaded.  So we intercept their sizes here just
            // after they've been compiled.
            for (obj in map.values()) {
                val bytes = obj as ByteArray
                nBytes += bytes.size
            }
            return map
        }
    }
}
