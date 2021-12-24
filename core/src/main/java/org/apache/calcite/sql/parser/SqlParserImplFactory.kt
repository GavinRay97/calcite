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
package org.apache.calcite.sql.parser

import org.apache.calcite.linq4j.function.Experimental

/**
 * Factory for
 * [org.apache.calcite.sql.parser.SqlAbstractParserImpl] objects.
 *
 *
 * A parser factory allows you to include a custom parser in
 * [org.apache.calcite.tools.Planner] created through
 * [org.apache.calcite.tools.Frameworks].
 */
@FunctionalInterface
interface SqlParserImplFactory {
    /**
     * Get the underlying parser implementation.
     *
     * @return [SqlAbstractParserImpl] object.
     */
    fun getParser(stream: Reader?): SqlAbstractParserImpl

    /**
     * Returns a DDL executor.
     *
     *
     * The default implementation returns [DdlExecutor.USELESS],
     * which cannot handle any DDL commands.
     *
     *
     * DDL execution is related to parsing but it is admittedly a stretch to
     * control them in the same factory. Therefore this is marked 'experimental'.
     * We are bundling them because they are often overridden at the same time. In
     * particular, we want a way to refine the behavior of the "server" module,
     * which supports DDL parsing and execution, and we're not yet ready to define
     * a new [java.sql.Driver] or
     * [org.apache.calcite.server.CalciteServer].
     */
    @get:Experimental
    val ddlExecutor: DdlExecutor?
        get() = DdlExecutor.USELESS
}
