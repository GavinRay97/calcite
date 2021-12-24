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

import org.slf4j.Logger

/**
 * Small extension to [Logger] with some performance improvements.
 *
 *
 * [Logger.info] is expensive
 * to call, since the caller must always allocate and fill in the array
 * `params`, even when the `level` will prevent a message
 * being logged. On the other hand, [Logger.info]
 * and [Logger.info] do not have this
 * problem.
 *
 *
 * As a workaround this class provides
 * [.info] etc. (The varargs feature of
 * java 1.5 half-solves this problem, by automatically wrapping args in an
 * array, but it does so without testing the level.)
 *
 *
 * Usage: replace:
 *
 * <blockquote>`static final Logger tracer =
 * CalciteTracer.getMyTracer();`</blockquote>
 *
 *
 * by:
 *
 * <blockquote>`static final CalciteLogger tracer =
 * new CalciteLogger(CalciteTrace.getMyTracer());`</blockquote>
 */
class CalciteLogger(logger: Logger?) {
    //~ Instance fields --------------------------------------------------------
    private val logger // delegate
            : Logger?

    //~ Constructors -----------------------------------------------------------
    init {
        assert(logger != null)
        this.logger = logger
    }
    //~ Methods ----------------------------------------------------------------
    // WARN
    /**
     * Logs a WARN message with two Object parameters.
     */
    fun warn(format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?) {
        // slf4j already avoids the array creation for 1 or 2 arg invocations
        logger.warn(format, arg1, arg2)
    }

    /**
     * Conditionally logs a WARN message with three Object parameters.
     */
    fun warn(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?
    ) {
        if (logger.isWarnEnabled()) {
            logger.warn(format, arg1, arg2, arg3)
        }
    }

    /**
     * Conditionally logs a WARN message with four Object parameters.
     */
    fun warn(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?, @Nullable arg4: Object?
    ) {
        if (logger.isWarnEnabled()) {
            logger.warn(format, arg1, arg2, arg3, arg4)
        }
    }

    fun warn(format: String?, vararg args: Object?) {
        if (logger.isWarnEnabled()) {
            logger.warn(format, args)
        }
    }
    // INFO
    /**
     * Logs an INFO message with two Object parameters.
     */
    fun info(format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?) {
        // slf4j already avoids the array creation for 1 or 2 arg invocations
        logger.info(format, arg1, arg2)
    }

    /**
     * Conditionally logs an INFO message with three Object parameters.
     */
    fun info(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?
    ) {
        if (logger.isInfoEnabled()) {
            logger.info(format, arg1, arg2, arg3)
        }
    }

    /**
     * Conditionally logs an INFO message with four Object parameters.
     */
    fun info(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?, @Nullable arg4: Object?
    ) {
        if (logger.isInfoEnabled()) {
            logger.info(format, arg1, arg2, arg3, arg4)
        }
    }

    fun info(format: String?, vararg args: Object?) {
        if (logger.isInfoEnabled()) {
            logger.info(format, args)
        }
    }
    // DEBUG
    /**
     * Logs a DEBUG message with two Object parameters.
     */
    fun debug(format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?) {
        // slf4j already avoids the array creation for 1 or 2 arg invocations
        logger.debug(format, arg1, arg2)
    }

    /**
     * Conditionally logs a DEBUG message with three Object parameters.
     */
    fun debug(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?
    ) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, arg1, arg2, arg3)
        }
    }

    /**
     * Conditionally logs a DEBUG message with four Object parameters.
     */
    fun debug(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?, @Nullable arg4: Object?
    ) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, arg1, arg2, arg3, arg4)
        }
    }

    fun debug(format: String?, vararg args: Object?) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, args)
        }
    }
    // TRACE
    /**
     * Logs a TRACE message with two Object parameters.
     */
    fun trace(format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?) {
        // slf4j already avoids the array creation for 1 or 2 arg invocations
        logger.trace(format, arg1, arg2)
    }

    /**
     * Conditionally logs a TRACE message with three Object parameters.
     */
    fun trace(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace(format, arg1, arg2, arg3)
        }
    }

    /**
     * Conditionally logs a TRACE message with four Object parameters.
     */
    fun trace(
        format: String?, @Nullable arg1: Object?, @Nullable arg2: Object?,
        @Nullable arg3: Object?, @Nullable arg4: Object?
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace(format, arg1, arg2, arg3, arg4)
        }
    }

    fun trace(format: String?, @Nullable vararg args: Object?) {
        if (logger.isTraceEnabled()) {
            logger.trace(format, args)
        }
    }

    // We expose and delegate the commonly used part of the Logger interface.
    // For everything else, just expose the delegate. (Could use reflection.)
    fun getLogger(): Logger? {
        return logger
    }

    // Hold-over from the previous j.u.logging implementation
    fun warn(msg: String?) {
        logger.warn(msg)
    }

    fun info(msg: String?) {
        logger.info(msg)
    }
}
