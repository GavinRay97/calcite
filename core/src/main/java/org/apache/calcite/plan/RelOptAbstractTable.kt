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
package org.apache.calcite.plan

import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelDistribution
import org.apache.calcite.rel.RelDistributions
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelReferentialConstraint
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.schema.ColumnStrategy
import org.apache.calcite.util.ImmutableBitSet
import com.google.common.collect.ImmutableList
import java.util.Collections
import java.util.List

/**
 * Partial implementation of [RelOptTable].
 */
abstract class RelOptAbstractTable protected constructor(
    schema: RelOptSchema,
    name: String,
    rowType: RelDataType
) : RelOptTable {
    //~ Instance fields --------------------------------------------------------
    protected val schema: RelOptSchema
    protected val rowType: RelDataType

    //~ Methods ----------------------------------------------------------------
    val name: String

    //~ Constructors -----------------------------------------------------------
    init {
        this.schema = schema
        this.name = name
        this.rowType = rowType
    }

    @get:Override
    val qualifiedName: List<String>
        get() = ImmutableList.of(name)

    @get:Override
    val rowCount: Double
        get() = 100

    @Override
    fun getRowType(): RelDataType {
        return rowType
    }

    @get:Override
    val relOptSchema: RelOptSchema
        get() = schema

    // Override to define collations.
    @get:Nullable
    @get:Override
    val collationList: List<Any>
        get() = Collections.emptyList()

    @get:Nullable
    @get:Override
    val distribution: RelDistribution
        get() = RelDistributions.BROADCAST_DISTRIBUTED

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
        return if (clazz.isInstance(this)) clazz.cast(this) else null
    }

    // Override to define keys
    @Override
    fun isKey(columns: ImmutableBitSet?): Boolean {
        return false
    }

    // Override to get unique keys
    @get:Nullable
    @get:Override
    val keys: List<Any>
        get() = Collections.emptyList()

    // Override to define foreign keys
    @get:Nullable
    @get:Override
    val referentialConstraints: List<Any>
        get() = Collections.emptyList()

    @Override
    fun toRel(context: ToRelContext): RelNode {
        return LogicalTableScan.create(
            context.getCluster(), this,
            context.getTableHints()
        )
    }

    @Override
    @Nullable
    fun getExpression(clazz: Class?): Expression? {
        return null
    }

    @Override
    fun extend(extendedFields: List<RelDataTypeField?>?): RelOptTable {
        throw UnsupportedOperationException()
    }

    @get:Override
    val columnStrategies: List<Any>
        get() = RelOptTableImpl.columnStrategies(this)
}
