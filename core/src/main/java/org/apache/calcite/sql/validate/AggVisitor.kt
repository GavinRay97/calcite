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
package org.apache.calcite.sql.validate

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.SqlSyntax
import org.apache.calcite.sql.`fun`.SqlAbstractGroupFunction
import org.apache.calcite.sql.util.SqlBasicVisitor
import java.util.ArrayList
import java.util.List
import java.util.Objects

/** Visitor that can find aggregate and windowed aggregate functions.
 *
 * @see AggFinder
 */
internal abstract class AggVisitor(
    opTab: SqlOperatorTable?,
    /** Whether to find windowed aggregates.  */
    protected val over: Boolean,
    /** Whether to find regular (non-windowed) aggregates.  */
    protected val aggregate: Boolean,
    /** Whether to find group functions (e.g. `TUMBLE`)
     * or group auxiliary functions (e.g. `TUMBLE_START`).  */
    protected val group: Boolean, @Nullable delegate: AggFinder?, nameMatcher: SqlNameMatcher?
) : SqlBasicVisitor<Void?>() {
    protected val opTab: SqlOperatorTable

    @Nullable
    protected val delegate: AggFinder?
    protected val nameMatcher: SqlNameMatcher

    /**
     * Creates an AggVisitor.
     *
     * @param opTab Operator table
     * @param over Whether to find windowed function calls `agg(x) OVER
     * windowSpec`
     * @param aggregate Whether to find non-windowed aggregate calls
     * @param group Whether to find group functions (e.g. `TUMBLE`)
     * @param delegate Finder to which to delegate when processing the arguments
     * @param nameMatcher Whether to match the agg function names case-sensitively
     */
    init {
        this.delegate = delegate
        this.opTab = Objects.requireNonNull(opTab, "opTab")
        this.nameMatcher = Objects.requireNonNull(nameMatcher, "nameMatcher")
    }

    @Override
    fun visit(call: SqlCall): Void? {
        val operator: SqlOperator = call.getOperator()
        // If nested aggregates disallowed or found an aggregate at invalid level
        if (operator.isAggregator()
            && operator !is SqlAbstractGroupFunction
            && !operator.requiresOver()
        ) {
            if (delegate != null) {
                return operator.acceptCall(delegate, call)
            }
            if (aggregate) {
                return found(call)
            }
        }
        if (group && operator.isGroup()) {
            return found(call)
        }
        // User-defined function may not be resolved yet.
        if (operator is SqlFunction) {
            val sqlFunction: SqlFunction = operator as SqlFunction
            if (sqlFunction.getFunctionType().isUserDefinedNotSpecificFunction()) {
                val list: List<SqlOperator> = ArrayList()
                val identifier: SqlIdentifier = sqlFunction.getSqlIdentifier()
                if (identifier != null) {
                    opTab.lookupOperatorOverloads(
                        identifier,
                        sqlFunction.getFunctionType(), SqlSyntax.FUNCTION, list,
                        nameMatcher
                    )
                    for (operator2 in list) {
                        if (operator2.isAggregator() && !operator2.requiresOver()) {
                            // If nested aggregates disallowed or found aggregate at invalid
                            // level
                            if (aggregate) {
                                found(call)
                            }
                        }
                    }
                }
            }
        }
        if (call.isA(SqlKind.QUERY)) {
            // don't traverse into queries
            return null
        }
        if (call.getKind() === SqlKind.WITHIN_GROUP) {
            if (aggregate) {
                return found(call)
            }
        }
        return if (call.getKind() === SqlKind.OVER) {
            if (over) {
                found(call)
            } else {
                // an aggregate function over a window is not an aggregate!
                null
            }
        } else super.visit(call)
    }

    protected abstract fun found(call: SqlCall?): Void?
}
