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

import java.util.ArrayList

/**
 * A `JavaCompilerArgs` holds the arguments for a
 * [JavaCompiler].
 *
 *
 * Specific implementations of [JavaCompiler] may override `
 * set*Argument*` methods to store arguments in a different fashion,
 * or may throw [UnsupportedOperationException] to indicate that the
 * compiler does not support that argument.
 */
class JavaCompilerArgs {
    //~ Instance fields --------------------------------------------------------
    var argsList: List<String> = ArrayList()
    var fileNameList: List<String> = ArrayList()
    var classLoader: ClassLoader

    //~ Constructors -----------------------------------------------------------
    init {
        classLoader = requireNonNull(
            getClass().getClassLoader()
        ) { "getClassLoader is null for " + getClass() }
    }

    //~ Methods ----------------------------------------------------------------
    fun clear() {
        fileNameList.clear()
    }

    /**
     * Sets the arguments by parsing a standard java argument string.
     *
     *
     * A typical such string is `"-classpath *classpath* -d *
     * dir* -verbose [*file*...]"`
     */
    fun setString(args: String?) {
        val list: List<String> = ArrayList()
        val tok = StringTokenizer(args)
        while (tok.hasMoreTokens()) {
            list.add(tok.nextToken())
        }
        stringArray = list.toArray(arrayOfNulls<String>(0))
    }

    /**
     * Sets the arguments by parsing a standard java argument string. A typical
     * such string is `"-classpath *classpath* -d *dir* -verbose
     * [*file*...]"`
     */
    var stringArray: Array<String>
        get() {
            argsList.addAll(fileNameList)
            return argsList.toArray(arrayOfNulls<String>(0))
        }
        set(args) {
            var i = 0
            while (i < args.size) {
                val arg = args[i]
                if (arg.equals("-classpath")) {
                    if (++i < args.size) {
                        setClasspath(args[i])
                    }
                } else if (arg.equals("-d")) {
                    if (++i < args.size) {
                        setDestdir(args[i])
                    }
                } else if (arg.equals("-verbose")) {
                    setVerbose(true)
                } else {
                    argsList.add(args[i])
                }
                i++
            }
        }

    fun addFile(fileName: String?) {
        fileNameList.add(fileName)
    }

    val fileNames: Array<String>
        get() = fileNameList.toArray(arrayOfNulls<String>(0))

    fun setVerbose(verbose: Boolean) {
        if (verbose) {
            argsList.add("-verbose")
        }
    }

    fun setDestdir(destdir: String?) {
        argsList.add("-d")
        argsList.add(destdir)
    }

    fun setClasspath(classpath: String?) {
        argsList.add("-classpath")
        argsList.add(classpath)
    }

    fun setDebugInfo(i: Int) {
        if (i > 0) {
            argsList.add("-g=$i")
        }
    }

    /**
     * Sets the source code (that is, the full java program, generally starting
     * with something like "package com.foo.bar;") and the file name.
     *
     *
     * This method is optional. It only works if the compiler supports
     * in-memory compilation. If this compiler does not return in-memory
     * compilation (which the base class does not), [.supportsSetSource]
     * returns false, and this method throws
     * [UnsupportedOperationException].
     */
    fun setSource(source: String?, fileName: String?) {
        throw UnsupportedOperationException()
    }

    /**
     * Returns whether [.setSource] will work.
     */
    fun supportsSetSource(): Boolean {
        return false
    }

    fun setFullClassName(fullClassName: String?) {
        // NOTE jvs 28-June-2004: I added this in order to support Janino's
        // JavaSourceClassLoader, which needs it.  Non-Farrago users
        // don't need to call this method.
    }

    fun setClassLoader(classLoader: ClassLoader) {
        this.classLoader = classLoader
    }

    fun getClassLoader(): ClassLoader {
        return classLoader
    }
}
