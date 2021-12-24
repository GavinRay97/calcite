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

import org.apache.calcite.plan.RelOptTable

/**
 * An implementation of [InitializerExpressionFactory] that always supplies NULL.
 */
class NullInitializerExpressionFactory : InitializerExpressionFactory {
    @SuppressWarnings("deprecation")
    @Override
    fun isGeneratedAlways(table: RelOptTable, iColumn: Int): Boolean {
        return when (generationStrategy(table, iColumn)) {
            VIRTUAL, STORED -> true
            else -> false
        }
    }

    @Override
    fun generationStrategy(table: RelOptTable, iColumn: Int): ColumnStrategy {
        return if (table.getRowType().getFieldList().get(iColumn).getType()
                .isNullable()
        ) ColumnStrategy.NULLABLE else ColumnStrategy.NOT_NULLABLE
    }

    @Override
    fun newColumnDefaultValue(
        table: RelOptTable, iColumn: Int,
        context: InitializerContext
    ): RexNode {
        val fieldType: RelDataType = table.getRowType().getFieldList().get(iColumn).getType()
        return context.getRexBuilder().makeNullLiteral(fieldType)
    }

    @Override
    @Nullable
    fun postExpressionConversionHook(): BiFunction<InitializerContext, RelNode, RelNode>? {
        return null
    }

    @Override
    fun newAttributeInitializer(
        type: RelDataType,
        constructor: SqlFunction?, iAttribute: Int, constructorArgs: List<RexNode?>?,
        context: InitializerContext
    ): RexNode {
        val fieldType: RelDataType = type.getFieldList().get(iAttribute).getType()
        return context.getRexBuilder().makeNullLiteral(fieldType)
    }

    companion object {
        val INSTANCE: InitializerExpressionFactory = NullInitializerExpressionFactory()
    }
}
