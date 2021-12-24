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
package org.apache.calcite.util.trace

import org.apache.calcite.linq4j.function.Function2
import org.apache.calcite.linq4j.function.Functions
import org.apache.calcite.plan.AbstractRelOptPlanner
import org.apache.calcite.plan.RelImplementor
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.prepare.Prepare
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File

/**
 * Contains all of the [tracers][org.slf4j.Logger] used within
 * org.apache.calcite class libraries.
 *
 * <h2>Note to developers</h2>
 *
 *
 * Please ensure that every tracer used in org.apache.calcite is added to
 * this class as a *public static final* member called `
 * *component*Tracer`. For example, [.getPlannerTracer] is the
 * tracer used by all classes which take part in the query planning process.
 *
 *
 * The javadoc in this file is the primary source of information on what
 * tracers are available, so the javadoc against each tracer member must be an
 * up-to-date description of what that tracer does.
 *
 *
 * In the class where the tracer is used, create a *private* (or
 * perhaps *protected*) *static final* member called `
 * tracer`.
 */
object CalciteTrace {
    //~ Static fields/initializers ---------------------------------------------
    /**
     * The "org.apache.calcite.sql.parser" tracer reports parser events in
     * [org.apache.calcite.sql.parser.SqlParser] and other classes at DEBUG.
     */
    val PARSER_LOGGER: Logger = parserTracer
    private val DYNAMIC_HANDLER: ThreadLocal<Function2<Void, File, String>> =
        ThreadLocal.withInitial(Functions::ignore2)
    //~ Methods ----------------------------------------------------------------
    /**
     * The "org.apache.calcite.plan.RelOptPlanner" tracer prints the query
     * optimization process.
     *
     *
     * Levels:
     *
     *
     *  * [Logger.debug] (formerly FINE) prints rules as they fire;
     *  * [Logger.trace] (formerly FINER) prints and validates the whole expression
     * pool and rule queue as each rule fires;
     *  * [Logger.trace] (formerly FINEST) also prints finer details like rule
     * importances.
     *
     */
    val plannerTracer: Logger
        get() = LoggerFactory.getLogger(RelOptPlanner::class.java.getName())

    /**
     * Reports volcano planner optimization task events.
     */
    val plannerTaskTracer: Logger
        get() = LoggerFactory.getLogger("org.apache.calcite.plan.volcano.task")

    /**
     * The "org.apache.calcite.prepare.Prepare" tracer prints the generated
     * program at DEBUG (formerly, FINE)  or higher.
     */
    val statementTracer: Logger
        get() = LoggerFactory.getLogger(Prepare::class.java.getName())

    /**
     * The "org.apache.calcite.rel.RelImplementorImpl" tracer reports when
     * expressions are bound to variables (DEBUG, formerly FINE)
     */
    val relImplementorTracer: Logger
        get() = LoggerFactory.getLogger(RelImplementor::class.java)

    /**
     * The tracer "org.apache.calcite.sql.timing" traces timing for
     * various stages of query processing.
     *
     * @see CalciteTimingTracer
     */
    val sqlTimingTracer: Logger
        get() = LoggerFactory.getLogger("org.apache.calcite.sql.timing")

    /**
     * The "org.apache.calcite.sql.parser" tracer reports parse events.
     */
    val parserTracer: Logger
        get() = LoggerFactory.getLogger("org.apache.calcite.sql.parser")

    /**
     * The "org.apache.calcite.sql2rel" tracer reports parse events.
     */
    val sqlToRelTracer: Logger
        get() = LoggerFactory.getLogger("org.apache.calcite.sql2rel")
    val ruleAttemptsTracer: Logger
        get() = LoggerFactory.getLogger(
            AbstractRelOptPlanner::class.java.getName() + ".rule_execution_summary"
        )

    /**
     * The tracers report important/useful information related with the execution
     * of unit tests.
     */
    fun getTestTracer(testClass: Class<*>): Logger {
        return LoggerFactory.getLogger(testClass.getName())
    }

    /**
     * Thread-local handler that is called with dynamically generated Java code.
     * It exists for unit-testing.
     * The handler is never null; the default handler does nothing.
     */
    val dynamicHandler: ThreadLocal<Function2<Void, File, String>>
        get() = DYNAMIC_HANDLER
}
