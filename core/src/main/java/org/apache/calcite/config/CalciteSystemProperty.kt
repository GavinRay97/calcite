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
package org.apache.calcite.config

import com.google.common.base.MoreObjects
import com.google.common.collect.ImmutableSet
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.util.Locale
import java.util.Properties
import java.util.Set
import java.util.function.Function
import java.util.function.IntPredicate
import java.util.stream.Stream

/**
 * A Calcite specific system property that is used to configure various aspects of the framework.
 *
 *
 * Calcite system properties must always be in the "calcite" root namespace.
 *
 * @param <T> the type of the property value
</T> */
class CalciteSystemProperty<T> private constructor(
    key: String,
    valueParser: Function<in String?, out T>
) {
    private val value: T

    init {
        value = valueParser.apply(PROPERTIES.getProperty(key))
    }

    /**
     * Returns the value of this property.
     *
     * @return the value of this property or `null` if a default value has not been
     * defined for this property.
     */
    fun value(): T {
        return value
    }

    companion object {
        /**
         * Holds all system properties related with the Calcite.
         *
         *
         * Deprecated `"saffron.properties"` (in namespaces"saffron" and "net.sf.saffron")
         * are also kept here but under "calcite" namespace.
         */
        private val PROPERTIES: Properties = loadProperties()

        /**
         * Whether to run Calcite in debug mode.
         *
         *
         * When debug mode is activated significantly more information is gathered and printed to
         * STDOUT. It is most commonly used to print and identify problems in generated java code. Debug
         * mode is also used to perform more verifications at runtime, which are not performed during
         * normal execution.
         */
        val DEBUG = booleanProperty("calcite.debug", false)

        /**
         * Whether to exploit join commutative property.
         */
        // TODO review zabetak:
        // Does the property control join commutativity or rather join associativity? The property is
        // associated with {@link org.apache.calcite.rel.rules.JoinAssociateRule} and not with
        // {@link org.apache.calcite.rel.rules.JoinCommuteRule}.
        val COMMUTE = booleanProperty("calcite.enable.join.commute", false)

        /** Whether to enable the collation trait in the default planner configuration.
         *
         *
         * Some extra optimizations are possible if enabled, but queries should
         * work either way. At some point this will become a preference, or we will
         * run multiple phases: first disabled, then enabled.  */
        val ENABLE_COLLATION_TRAIT = booleanProperty("calcite.enable.collation.trait", true)

        /** Whether the enumerable convention is enabled in the default planner configuration.  */
        val ENABLE_ENUMERABLE = booleanProperty("calcite.enable.enumerable", true)

        /** Whether the EnumerableTableScan should support ARRAY fields.  */
        val ENUMERABLE_ENABLE_TABLESCAN_ARRAY = booleanProperty("calcite.enable.enumerable.tablescan.array", false)

        /** Whether the EnumerableTableScan should support MAP fields.  */
        val ENUMERABLE_ENABLE_TABLESCAN_MAP = booleanProperty("calcite.enable.enumerable.tablescan.map", false)

        /** Whether the EnumerableTableScan should support MULTISET fields.  */
        val ENUMERABLE_ENABLE_TABLESCAN_MULTISET =
            booleanProperty("calcite.enable.enumerable.tablescan.multiset", false)

        /** Whether streaming is enabled in the default planner configuration.  */
        val ENABLE_STREAM = booleanProperty("calcite.enable.stream", true)

        /**
         * Whether RexNode digest should be normalized (e.g. call operands ordered).
         *
         * Normalization helps to treat $0=$1 and $1=$0 expressions equal, thus it saves efforts
         * on planning.  */
        val ENABLE_REX_DIGEST_NORMALIZE = booleanProperty("calcite.enable.rexnode.digest.normalize", true)

        /**
         * Whether to follow the SQL standard strictly.
         */
        val STRICT = booleanProperty("calcite.strict.sql", false)

        /**
         * Whether to include a GraphViz representation when dumping the state of the Volcano planner.
         */
        val DUMP_GRAPHVIZ = booleanProperty("calcite.volcano.dump.graphviz", true)

        /**
         * Whether to include `RelSet` information when dumping the state of the Volcano
         * planner.
         */
        val DUMP_SETS = booleanProperty("calcite.volcano.dump.sets", true)

        /**
         * Whether to enable top-down optimization. This config can be overridden
         * by [CalciteConnectionProperty.TOPDOWN_OPT].
         *
         *
         * Note: Enabling top-down optimization will automatically disable
         * the use of AbstractConverter and related rules.
         */
        val TOPDOWN_OPT = booleanProperty("calcite.planner.topdown.opt", false)

        /**
         * Whether to run integration tests.
         */
        // TODO review zabetak:
        // The property is used in only one place and it is associated with mongodb. Should we drop this
        // property and just use TEST_MONGODB?
        val INTEGRATION_TEST = booleanProperty("calcite.integrationTest", false)

        /**
         * Which database to use for tests that require a JDBC data source.
         *
         *
         * The property can take one of the following values:
         *
         *
         *  * HSQLDB (default)
         *  * H2
         *  * MYSQL
         *  * ORACLE
         *  * POSTGRESQL
         *
         *
         *
         * If the specified value is not included in the previous list, the default
         * is used.
         *
         *
         * We recommend that casual users use hsqldb, and frequent Calcite
         * developers use MySQL. The test suite runs faster against the MySQL database
         * (mainly because of the 0.1 second versus 6 seconds startup time). You have
         * to populate MySQL manually with the foodmart data set, otherwise there will
         * be test failures.
         */
        val TEST_DB = stringProperty(
            "calcite.test.db", "HSQLDB",
            ImmutableSet.of(
                "HSQLDB",
                "H2",
                "MYSQL",
                "ORACLE",
                "POSTGRESQL"
            )
        )

        /**
         * Path to the dataset file that should used for integration tests.
         *
         *
         * If a path is not set, then one of the following values will be used:
         *
         *
         *  * ../calcite-test-dataset
         *  * ../../calcite-test-dataset
         *  * .
         *
         * The first valid path that exists in the filesystem will be chosen.
         */
        val TEST_DATASET_PATH: CalciteSystemProperty<String> =
            CalciteSystemProperty<Any>("calcite.test.dataset", label@ Function<in String?, out T?> { v ->
                if (v != null) {
                    return@label v
                }
                val dirs = arrayOf(
                    "../calcite-test-dataset",
                    "../../calcite-test-dataset"
                )
                for (s in dirs) {
                    if (File(s).exists() && File(s, "vm").exists()) {
                        return@label s
                    }
                }
                "."
            })

        /**
         * Whether to run MongoDB tests.
         */
        val TEST_MONGODB = booleanProperty("calcite.test.mongodb", true)

        /**
         * Whether to run Splunk tests.
         *
         *
         * Disabled by default, because we do not expect Splunk to be installed
         * and populated with the data set necessary for testing.
         */
        val TEST_SPLUNK = booleanProperty("calcite.test.splunk", false)

        /**
         * Whether to run Druid tests.
         */
        val TEST_DRUID = booleanProperty("calcite.test.druid", false)

        /**
         * Whether to run Cassandra tests.
         */
        val TEST_CASSANDRA = booleanProperty("calcite.test.cassandra", true)

        /**
         * Whether to run InnoDB tests.
         */
        val TEST_INNODB = booleanProperty("calcite.test.innodb", true)

        /**
         * Whether to run Redis tests.
         */
        val TEST_REDIS = booleanProperty("calcite.test.redis", true)

        /**
         * Whether to use Docker containers (https://www.testcontainers.org/) in tests.
         *
         * If the property is set to `true`, affected tests will attempt to start Docker
         * containers; when Docker is not available tests fallback to other execution modes and if it's
         * not possible they are skipped entirely.
         *
         * If the property is set to `false`, Docker containers are not used at all and
         * affected tests either fallback to other execution modes or skipped entirely.
         *
         * Users can override the default behavior to force non-Dockerized execution even when Docker
         * is installed on the machine; this can be useful for replicating an issue that appears only in
         * non-docker test mode or for running tests both with and without containers in CI.
         */
        val TEST_WITH_DOCKER_CONTAINER = booleanProperty("calcite.test.docker", true)

        /**
         * A list of ids designating the queries
         * (from query.json in new.hydromatic:foodmart-queries:0.4.1)
         * that should be run as part of FoodmartTest.
         */
        // TODO review zabetak:
        // The name of the property is not appropriate. A better alternative would be
        // calcite.test.foodmart.queries.ids. Moreover, I am not in favor of using system properties for
        // parameterized tests.
        val TEST_FOODMART_QUERY_IDS: CalciteSystemProperty<String> =
            CalciteSystemProperty<Any>("calcite.ids", Function.< String > identity < String ? > ())

        /**
         * Whether the optimizer will consider adding converters of infinite cost in
         * order to convert a relational expression from one calling convention to
         * another.
         */
        val ALLOW_INFINITE_COST_CONVERTERS = booleanProperty("calcite.opt.allowInfiniteCostConverters", true)

        /**
         * The name of the default character set.
         *
         *
         * It is used by [org.apache.calcite.sql.validate.SqlValidator].
         */
        // TODO review zabetak:
        // What happens if a wrong value is specified?
        val DEFAULT_CHARSET = stringProperty("calcite.default.charset", "ISO-8859-1")

        /**
         * The name of the default national character set.
         *
         *
         * It is used with the N'string' construct in
         * [org.apache.calcite.sql.SqlLiteral.SqlLiteral]
         * and may be different from the [.DEFAULT_CHARSET].
         */
        // TODO review zabetak:
        // What happens if a wrong value is specified?
        val DEFAULT_NATIONAL_CHARSET = stringProperty("calcite.default.nationalcharset", "ISO-8859-1")

        /**
         * The name of the default collation.
         *
         *
         * It is used in [org.apache.calcite.sql.SqlCollation] and
         * [org.apache.calcite.sql.SqlLiteral.SqlLiteral].
         */
        // TODO review zabetak:
        // What happens if a wrong value is specified?
        val DEFAULT_COLLATION = stringProperty("calcite.default.collation.name", "ISO-8859-1\$en_US")

        /**
         * The strength of the default collation.
         * Allowed values (as defined in [java.text.Collator]) are: primary, secondary,
         * tertiary, identical.
         *
         *
         * It is used in [org.apache.calcite.sql.SqlCollation] and
         * [org.apache.calcite.sql.SqlLiteral.SqlLiteral].
         */
        // TODO review zabetak:
        // What happens if a wrong value is specified?
        val DEFAULT_COLLATION_STRENGTH = stringProperty("calcite.default.collation.strength", "primary")

        /**
         * The maximum size of the cache of metadata handlers.
         *
         *
         * A typical value is the number of queries being concurrently prepared multiplied by the
         * number of types of metadata.
         *
         *
         * If the value is less than 0, there is no limit.
         */
        val METADATA_HANDLER_CACHE_MAXIMUM_SIZE: CalciteSystemProperty<Integer> =
            intProperty("calcite.metadata.handler.cache.maximum.size", 1000)

        /**
         * The maximum size of the cache used for storing Bindable objects, instantiated via
         * dynamically generated Java classes.
         *
         *
         * The default value is 0.
         *
         *
         * The property can take any value between [0, [Integer.MAX_VALUE]] inclusive. If the
         * value is not valid (or not specified) then the default value is used.
         *
         *
         * The cached objects may be quite big so it is suggested to use a rather small cache size
         * (e.g., 1000). For the most common use cases a number close to 1000 should be enough to
         * alleviate the performance penalty of compiling and loading classes.
         *
         *
         * Setting this property to 0 disables the cache.
         */
        val BINDABLE_CACHE_MAX_SIZE: CalciteSystemProperty<Integer> =
            intProperty("calcite.bindable.cache.maxSize", 0, IntPredicate { v -> v >= 0 && v <= Integer.MAX_VALUE })

        /**
         * The concurrency level of the cache used for storing Bindable objects, instantiated via
         * dynamically generated Java classes.
         *
         *
         * The default value is 1.
         *
         *
         * The property can take any value between [1, [Integer.MAX_VALUE]] inclusive. If the
         * value is not valid (or not specified) then the default value is used.
         *
         *
         * This property has no effect if the cache is disabled (i.e., [.BINDABLE_CACHE_MAX_SIZE]
         * set to 0.
         */
        val BINDABLE_CACHE_CONCURRENCY_LEVEL: CalciteSystemProperty<Integer> =
            intProperty("calcite.bindable.cache.concurrencyLevel", 1,
                IntPredicate { v -> v >= 1 && v <= Integer.MAX_VALUE })

        private fun booleanProperty(
            key: String,
            defaultValue: Boolean
        ): CalciteSystemProperty<Boolean> {
            // Note that "" -> true (convenient for command-lines flags like '-Dflag')
            return CalciteSystemProperty<Any>(key,
                Function<in String?, out T?> { v ->
                    if (v == null) defaultValue else "".equals(v) || Boolean.parseBoolean(
                        v
                    )
                })
        }

        private fun intProperty(key: String, defaultValue: Int): CalciteSystemProperty<Integer> {
            return intProperty(key, defaultValue, IntPredicate { v -> true })
        }

        /**
         * Returns the value of the system property with the specified name as `int`. If any of the conditions below hold, returns the
         * `defaultValue`:
         *
         *
         *  1. the property is not defined;
         *  1. the property value cannot be transformed to an int;
         *  1. the property value does not satisfy the checker.
         *
         */
        private fun intProperty(
            key: String, defaultValue: Int,
            valueChecker: IntPredicate
        ): CalciteSystemProperty<Integer> {
            return CalciteSystemProperty<Any>(key, label@ Function<in String?, out T?> { v ->
                if (v == null) {
                    return@label defaultValue
                }
                try {
                    val intVal: Int = Integer.parseInt(v)
                    return@label if (valueChecker.test(intVal)) intVal else defaultValue
                } catch (nfe: NumberFormatException) {
                    return@label defaultValue
                }
            })
        }

        private fun stringProperty(key: String, defaultValue: String): CalciteSystemProperty<String> {
            return CalciteSystemProperty<Any>(
                key,
                Function<in String?, out T?> { v -> if (v == null) defaultValue else v })
        }

        private fun stringProperty(
            key: String,
            defaultValue: String,
            allowedValues: Set<String>
        ): CalciteSystemProperty<String> {
            return CalciteSystemProperty<Any>(key, label@ Function<in String?, out T?> { v ->
                if (v == null) {
                    return@label defaultValue
                }
                val normalizedValue: String = v.toUpperCase(Locale.ROOT)
                if (allowedValues.contains(normalizedValue)) normalizedValue else defaultValue
            })
        }

        private fun loadProperties(): Properties {
            val saffronProperties = Properties()
            val classLoader: ClassLoader = MoreObjects.firstNonNull(
                Thread.currentThread().getContextClassLoader(),
                CalciteSystemProperty::class.java.getClassLoader()
            )
            // Read properties from the file "saffron.properties", if it exists in classpath
            try {
                classLoader.getResourceAsStream("saffron.properties").use { stream ->
                    if (stream != null) {
                        saffronProperties.load(stream)
                    }
                }
            } catch (e: IOException) {
                throw RuntimeException("while reading from saffron.properties file", e)
            } catch (e: RuntimeException) {
                if (!"java.security.AccessControlException".equals(e.getClass().getName())) {
                    throw e
                }
            }

            // Merge system and saffron properties, mapping deprecated saffron
            // namespaces to calcite
            val allProperties = Properties()
            Stream.concat(
                saffronProperties.entrySet().stream(),
                System.getProperties().entrySet().stream()
            )
                .forEach { prop ->
                    val deprecatedKey = prop.getKey() as String
                    val newKey: String = deprecatedKey
                        .replace("net.sf.saffron.", "calcite.")
                        .replace("saffron.", "calcite.")
                    if (newKey.startsWith("calcite.")) {
                        allProperties.setProperty(newKey, prop.getValue() as String)
                    }
                }
            return allProperties
        }
    }
}
