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

import org.apache.calcite.runtime.Resources

/**
 * Provides an environment for debugging information, et cetera, used by
 * saffron.
 *
 *
 * It is a singleton, accessed via the [.INSTANCE] object. It is
 * populated from System properties if saffron is invoked via a `
 * main()` method, from a `javax.servlet.ServletContext` if
 * saffron is invoked from a servlet, and so forth. If there is a file called
 * `"saffron.properties"` in the current directory, it is read too.
 *
 *
 * Every property used in saffron code must have a method in this interface.
 * The method must return a sub-class of
 * [org.apache.calcite.runtime.Resources.Prop]. The javadoc
 * comment must describe the name of the property (for example,
 * "net.sf.saffron.connection.PoolSize") and the default value, if any. *
 * Developers, please make sure that this remains so!*
 *
 */
@Deprecated
@Deprecated(
    """As of release 1.19,
  replaced by {@link org.apache.calcite.config.CalciteSystemProperty}"""
)
interface SaffronProperties {
    /**
     * The boolean property "saffron.opt.allowInfiniteCostConverters" determines
     * whether the optimizer will consider adding converters of infinite cost in
     * order to convert a relational expression from one calling convention to
     * another. The default value is `true`.
     */
    @Resource("saffron.opt.allowInfiniteCostConverters")
    @Default("true")
    fun allowInfiniteCostConverters(): BooleanProp?

    /**
     * The string property "saffron.default.charset" is the name of the default
     * character set. The default is "ISO-8859-1". It is used in
     * [org.apache.calcite.sql.validate.SqlValidator].
     */
    @Resource("saffron.default.charset")
    @Default("ISO-8859-1")
    fun defaultCharset(): StringProp?

    /**
     * The string property "saffron.default.nationalcharset" is the name of the
     * default national character set which is used with the N'string' construct
     * which may or may not be different from the [.defaultCharset]. The
     * default is "ISO-8859-1". It is used in
     * [org.apache.calcite.sql.SqlLiteral.SqlLiteral]
     */
    @Resource("saffron.default.nationalcharset")
    @Default("ISO-8859-1")
    fun defaultNationalCharset(): StringProp?

    /**
     * The string property "saffron.default.collation.name" is the name of the
     * default collation. The default is "ISO-8859-1$en_US". Used in
     * [org.apache.calcite.sql.SqlCollation] and
     * [org.apache.calcite.sql.SqlLiteral.SqlLiteral]
     */
    @Resource("saffron.default.collation.name")
    @Default("ISO-8859-1\$en_US")
    fun defaultCollation(): StringProp?

    /**
     * The string property "saffron.default.collation.strength" is the strength
     * of the default collation. The default is "primary". Used in
     * [org.apache.calcite.sql.SqlCollation] and
     * [org.apache.calcite.sql.SqlLiteral.SqlLiteral]
     */
    @Resource("saffron.default.collation.strength")
    @Default("primary")
    fun defaultCollationStrength(): StringProp?

    /**
     * The string property "saffron.metadata.handler.cache.maximum.size" is the
     * maximum size of the cache of metadata handlers. A typical value is
     * the number of queries being concurrently prepared multiplied by the number
     * of types of metadata.
     *
     *
     * If the value is less than 0, there is no limit. The default is 1,000.
     */
    @Resource("saffron.metadata.handler.cache.maximum.size")
    @Default("1000")
    fun metadataHandlerCacheMaximumSize(): IntProp?

    /** Helper class.  */
    object Helper {
        /**
         * Retrieves the singleton instance of [SaffronProperties].
         */
        fun instance(): SaffronProperties {
            val properties = Properties()

            // read properties from the file "saffron.properties", if it exists in classpath
            try {
                Objects.requireNonNull(Helper::class.java.getClassLoader(), "classLoader")
                    .getResourceAsStream("saffron.properties").use { stream ->
                        if (stream != null) {
                            properties.load(stream)
                        }
                    }
            } catch (e: IOException) {
                throw RuntimeException("while reading from saffron.properties file", e)
            } catch (e: RuntimeException) {
                if (!"java.security.AccessControlException".equals(e.getClass().getName())) {
                    throw e
                }
            }

            // copy in all system properties which start with "saffron."
            val source: Properties = System.getProperties()
            for (objectKey in Collections.list(source.keys())) {
                val key = objectKey as String
                val value: String = Objects.requireNonNull(
                    source.getProperty(key)
                ) { "value for $key" }
                if (key.startsWith("saffron.") || key.startsWith("net.sf.saffron.")) {
                    properties.setProperty(key, value)
                }
            }
            return Resources.create(properties, SaffronProperties::class.java)
        }
    }

    companion object {
        val INSTANCE = Helper.instance()
    }
}
