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
package org.apache.calcite.prepare

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Translates a tree of linq4j [Queryable] nodes to a tree of
 * [RelNode] planner nodes.
 *
 * @see QueryableRelBuilder
 */
internal class LixToRelTranslator(cluster: RelOptCluster, preparingStmt: Prepare) {
    val cluster: RelOptCluster
    private val preparingStmt: Prepare
    val typeFactory: JavaTypeFactory

    init {
        this.cluster = cluster
        this.preparingStmt = preparingStmt
        typeFactory = cluster.getTypeFactory() as JavaTypeFactory
    }

    fun toRelContext(): RelOptTable.ToRelContext {
        return if (preparingStmt is RelOptTable.ViewExpander) {
            val viewExpander: RelOptTable.ViewExpander = preparingStmt as RelOptTable.ViewExpander
            ViewExpanders.toRelContext(viewExpander, cluster)
        } else {
            ViewExpanders.simpleContext(cluster)
        }
    }

    fun <T> translate(queryable: Queryable<T>): RelNode {
        val translatorQueryable: QueryableRelBuilder<T> = QueryableRelBuilder(this)
        return translatorQueryable.toRel(queryable)
    }

    fun translate(expression: Expression): RelNode {
        if (expression is MethodCallExpression) {
            val call: MethodCallExpression = expression as MethodCallExpression
            val method: BuiltInMethod = BuiltInMethod.MAP.get(call.method)
                ?: throw UnsupportedOperationException(
                    "unknown method " + call.method
                )
            val input: RelNode
            return when (method) {
                SELECT -> {
                    input = translate(getTargetExpression(call))
                    LogicalProject.create(
                        input,
                        ImmutableList.of(),
                        toRex(input, call.expressions.get(0) as FunctionExpression),
                        null as List<String?>?
                    )
                }
                WHERE -> {
                    input = translate(getTargetExpression(call))
                    LogicalFilter.create(
                        input,
                        toRex(call.expressions.get(0) as FunctionExpression, input)
                    )
                }
                AS_QUERYABLE -> LogicalTableScan.create(
                    cluster,
                    RelOptTableImpl.create(
                        null,
                        typeFactory.createJavaType(
                            Types.toClass(
                                getElementType(call)
                            )
                        ),
                        ImmutableList.of(),
                        getTargetExpression(call)
                    ),
                    ImmutableList.of()
                )
                SCHEMA_GET_TABLE -> LogicalTableScan.create(
                    cluster,
                    RelOptTableImpl.create(
                        null,
                        typeFactory.createJavaType(
                            requireNonNull(
                                (call.expressions.get(1) as ConstantExpression).value,
                                "argument 1 (0-based) is null Class"
                            ) as Class?
                        ),
                        ImmutableList.of(),
                        getTargetExpression(call)
                    ),
                    ImmutableList.of()
                )
                else -> throw UnsupportedOperationException(
                    "unknown method " + call.method
                )
            }
        }
        throw UnsupportedOperationException(
            "unknown expression type " + expression.getNodeType()
        )
    }

    private fun toRex(
        child: RelNode, expression: FunctionExpression
    ): List<RexNode> {
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        val list: List<RexNode> = Collections.singletonList(
            rexBuilder.makeRangeReference(child)
        )
        val translator: CalcitePrepareImpl.ScalarTranslator = CalcitePrepareImpl.EmptyScalarTranslator
            .empty(rexBuilder)
            .bind(getParameterList(expression), list)
        val rexList: List<RexNode> = ArrayList()
        val simple: Expression = Blocks.simple(getBody(expression))
        for (expression1 in fieldExpressions(simple)) {
            rexList.add(translator.toRex(expression1))
        }
        return rexList
    }

    fun fieldExpressions(expression: Expression): List<Expression> {
        if (expression is NewExpression) {
            // Note: We are assuming that the arguments to the constructor
            // are the same order as the fields of the class.
            return (expression as NewExpression).arguments
        }
        throw RuntimeException(
            "unsupported expression type $expression"
        )
    }

    fun toRexList(
        expression: FunctionExpression,
        vararg inputs: RelNode?
    ): List<RexNode> {
        val list: List<RexNode> = ArrayList()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        for (input in inputs) {
            list.add(rexBuilder.makeRangeReference(input))
        }
        return CalcitePrepareImpl.EmptyScalarTranslator.empty(rexBuilder)
            .bind(getParameterList(expression), list)
            .toRexList(getBody(expression))
    }

    fun toRex(
        expression: FunctionExpression,
        vararg inputs: RelNode?
    ): RexNode {
        val list: List<RexNode> = ArrayList()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        for (input in inputs) {
            list.add(rexBuilder.makeRangeReference(input))
        }
        return CalcitePrepareImpl.EmptyScalarTranslator.empty(rexBuilder)
            .bind(getParameterList(expression), list)
            .toRex(getBody(expression))
    }

    companion object {
        private fun getBody(expression: FunctionExpression<*>): BlockStatement {
            return requireNonNull(expression.body) { "body in $expression" }
        }

        private fun getParameterList(expression: FunctionExpression<*>): List<ParameterExpression> {
            return requireNonNull(expression.parameterList) { "parameterList in $expression" }
        }

        private fun getTargetExpression(call: MethodCallExpression): Expression {
            return requireNonNull(
                call.targetExpression,
                "translation of static calls is not supported yet"
            )
        }

        private fun getElementType(call: MethodCallExpression): Type {
            val type: Type = getTargetExpression(call).getType()
            return requireNonNull(
                Types.getElementType(type)
            ) { "unable to figure out element type from $type" }
        }
    }
}
