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
package org.apache.calcite.rel

import org.apache.calcite.plan.RelOptCluster

/**
 * Context from which a relational expression can initialize itself,
 * reading from a serialized form of the relational expression.
 */
interface RelInput {
    val cluster: RelOptCluster?
    val traitSet: RelTraitSet?
    fun getTable(table: String?): RelOptTable?

    /**
     * Returns the input relational expression. Throws if there is not precisely
     * one input.
     */
    val input: org.apache.calcite.rel.RelNode?
    val inputs: List<org.apache.calcite.rel.RelNode?>?

    /**
     * Returns an expression.
     */
    @Nullable
    fun getExpression(tag: String?): RexNode?
    fun getBitSet(tag: String?): ImmutableBitSet?

    @Nullable
    fun getBitSetList(tag: String?): List<ImmutableBitSet?>?
    fun getAggregateCalls(tag: String?): List<AggregateCall?>?

    @Nullable
    operator fun get(tag: String?): Object?

    /**
     * Returns a `string` value. Throws if wrong type.
     */
    @Nullable
    fun getString(tag: String?): String?

    /**
     * Returns a `float` value. Throws if not present or wrong type.
     */
    fun getFloat(tag: String?): Float

    /**
     * Returns an enum value. Throws if not a valid member.
     */
    fun <E : Enum<E>?> getEnum(tag: String?, enumClass: Class<E>?): @Nullable E?

    @Nullable
    fun getExpressionList(tag: String?): List<RexNode?>?

    @Nullable
    fun getStringList(tag: String?): List<String?>?

    @Nullable
    fun getIntegerList(tag: String?): List<Integer?>?

    @Nullable
    fun getIntegerListList(tag: String?): List<List<Integer?>?>?
    fun getRowType(tag: String?): RelDataType?
    fun getRowType(expressionsTag: String?, fieldsTag: String?): RelDataType?
    val collation: org.apache.calcite.rel.RelCollation?
    val distribution: org.apache.calcite.rel.RelDistribution?
    fun getTuples(tag: String?): ImmutableList<ImmutableList<RexLiteral?>?>?
    fun getBoolean(tag: String?, default_: Boolean): Boolean
}
