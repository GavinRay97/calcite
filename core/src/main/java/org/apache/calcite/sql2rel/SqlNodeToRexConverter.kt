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
package org.apache.calcite.sql2rel

import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode

/**
 * Converts expressions from [SqlNode] to [RexNode].
 */
interface SqlNodeToRexConverter {
    //~ Methods ----------------------------------------------------------------
    /**
     * Converts a [SqlCall] to a [RexNode] expression.
     */
    fun convertCall(
        cx: SqlRexContext?,
        call: SqlCall?
    ): RexNode?

    /**
     * Converts a [SQL literal][SqlLiteral] to a
     * [REX literal][RexLiteral].
     *
     *
     * The result is [RexNode], not [RexLiteral] because if the
     * literal is NULL (or the boolean Unknown value), we make a `CAST(NULL
     * AS type)` expression.
     */
    fun convertLiteral(
        cx: SqlRexContext?,
        literal: SqlLiteral?
    ): RexNode?

    /**
     * Converts a [SQL Interval Qualifier][SqlIntervalQualifier] to a
     * [REX literal][RexLiteral].
     */
    fun convertInterval(
        cx: SqlRexContext?,
        intervalQualifier: SqlIntervalQualifier?
    ): RexLiteral?
}
