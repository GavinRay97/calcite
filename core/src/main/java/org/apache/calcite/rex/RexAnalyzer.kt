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
package org.apache.calcite.rex

import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.rel.metadata.NullSentinel
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import java.math.BigDecimal
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set

/** Analyzes an expression, figures out what are the unbound variables,
 * assigns a variety of values to each unbound variable, and evaluates
 * the expression.  */
class RexAnalyzer(e: RexNode, predicates: RelOptPredicateList) {
    val e: RexNode
    val variables: List<RexNode>
    val unsupportedCount: Int

    /** Creates a RexAnalyzer.  */
    init {
        this.e = e
        val variableCollector = VariableCollector()
        e.accept(variableCollector)
        variableCollector.visitEach(predicates.pulledUpPredicates)
        variables = ImmutableList.copyOf(variableCollector.builder)
        unsupportedCount = variableCollector.unsupportedCount
    }

    /** Generates a map of variables and lists of values that could be assigned
     * to them.  */
    @SuppressWarnings("BetaApi")
    fun assignments(): Iterable<Map<RexNode, Comparable>> {
        val generators: List<List<Comparable>> =
            variables.stream().map { variable: RexNode -> getComparables(variable) }
                .collect(Util.toImmutableList())
        val product: Iterable<List<Comparable>> = Linq4j.product(generators)
        return Util.transform(
            product
        ) { values -> ImmutableMap.copyOf(Pair.zip(variables, values)) }
    }

    /** Collects the variables (or other bindable sites) in an expression, and
     * counts features (such as CAST) that [RexInterpreter] cannot
     * handle.  */
    private class VariableCollector internal constructor() : RexVisitorImpl<Void?>(true) {
        val builder: Set<RexNode> = LinkedHashSet()
        var unsupportedCount = 0
        @Override
        fun visitInputRef(inputRef: RexInputRef?): Void {
            builder.add(inputRef)
            return super.visitInputRef(inputRef)
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess): Void? {
            return if (fieldAccess.getReferenceExpr() is RexDynamicParam) {
                builder.add(fieldAccess)
                null
            } else {
                super.visitFieldAccess(fieldAccess)
            }
        }

        @Override
        fun visitCall(call: RexCall): Void? {
            return when (call.getKind()) {
                CAST, OTHER_FUNCTION -> {
                    ++unsupportedCount
                    null
                }
                else -> super.visitCall(call)
            }
        }
    }

    companion object {
        private fun getComparables(variable: RexNode): List<Comparable> {
            val values: ImmutableList.Builder<Comparable> = ImmutableList.builder()
            when (variable.getType().getSqlTypeName()) {
                BOOLEAN -> {
                    values.add(true)
                    values.add(false)
                }
                INTEGER -> {
                    values.add(BigDecimal.valueOf(-1L))
                    values.add(BigDecimal.valueOf(0L))
                    values.add(BigDecimal.valueOf(1L))
                    values.add(BigDecimal.valueOf(1000000L))
                }
                DECIMAL -> {
                    values.add(BigDecimal.valueOf(-100L))
                    values.add(BigDecimal.valueOf(100L))
                }
                VARCHAR -> {
                    values.add(NlsString("", null, null))
                    values.add(NlsString("hello", null, null))
                }
                TIMESTAMP -> values.add(0L) // 1970-01-01 00:00:00
                DATE -> {
                    values.add(0) // 1970-01-01
                    values.add(365) // 1971-01-01
                    values.add(-365) // 1969-01-01
                }
                TIME -> {
                    values.add(0) // 00:00:00.000
                    values.add(86399000) // 23:59:59.000
                }
                else -> throw AssertionError(
                    "don't know values for " + variable
                            + " of type " + variable.getType()
                )
            }
            if (variable.getType().isNullable()) {
                values.add(NullSentinel.INSTANCE)
            }
            return values.build()
        }
    }
}
