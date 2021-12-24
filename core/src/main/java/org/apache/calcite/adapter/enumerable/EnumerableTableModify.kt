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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.adapter.java.JavaTypeFactory

/** Implementation of [org.apache.calcite.rel.core.TableModify] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableTableModify(
    cluster: RelOptCluster?, traits: RelTraitSet?,
    table: RelOptTable, catalogReader: CatalogReader?, child: RelNode?,
    operation: Operation?, @Nullable updateColumnList: List<String?>?,
    @Nullable sourceExpressionList: List<RexNode?>?, flattened: Boolean
) : TableModify(
    cluster, traits, table, catalogReader, child, operation,
    updateColumnList, sourceExpressionList, flattened
), EnumerableRel {
    init {
        assert(child.getConvention() is EnumerableConvention)
        assert(getConvention() is EnumerableConvention)
        val modifiableTable: ModifiableTable = table.unwrap(ModifiableTable::class.java)
            ?: throw AssertionError() // TODO: user error in validator
    }

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return EnumerableTableModify(
            getCluster(),
            traitSet,
            getTable(),
            getCatalogReader(),
            sole(inputs),
            getOperation(),
            getUpdateColumnList(),
            getSourceExpressionList(),
            isFlattened()
        )
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val builder = BlockBuilder()
        val result: Result = implementor.visitChild(
            this, 0, getInput() as EnumerableRel, pref
        )
        val childExp: Expression = builder.append(
            "child", result.block
        )
        val collectionParameter: ParameterExpression = Expressions.parameter(
            Collection::class.java,
            builder.newName("collection")
        )
        val expression: Expression = table.getExpression(ModifiableTable::class.java)
        assert(
            expression != null // TODO: user error in validator
        )
        assert(
            ModifiableTable::class.java.isAssignableFrom(
                Types.toClass(expression.getType())
            )
        ) { expression.getType() }
        builder.add(
            Expressions.declare(
                Modifier.FINAL,
                collectionParameter,
                Expressions.call(
                    expression,
                    BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method
                )
            )
        )
        val countParameter: Expression = builder.append(
            "count",
            Expressions.call(collectionParameter, "size"),
            false
        )
        val convertedChildExp: Expression
        convertedChildExp = if (!getInput().getRowType().equals(getRowType())) {
            val typeFactory: JavaTypeFactory = getCluster().getTypeFactory() as JavaTypeFactory
            val format: JavaRowFormat = EnumerableTableScan.deduceFormat(table)
            val physType: PhysType = PhysTypeImpl.of(typeFactory, table.getRowType(), format)
            val expressionList: List<Expression> = ArrayList()
            val childPhysType: PhysType = result.physType
            val o_: ParameterExpression = Expressions.parameter(childPhysType.getJavaRowType(), "o")
            val fieldCount: Int = childPhysType.getRowType().getFieldCount()
            for (i in 0 until fieldCount) {
                expressionList.add(
                    childPhysType.fieldReference(o_, i, physType.getJavaFieldType(i))
                )
            }
            builder.append(
                "convertedChild",
                Expressions.call(
                    childExp,
                    BuiltInMethod.SELECT.method,
                    Expressions.lambda(
                        physType.record(expressionList), o_
                    )
                )
            )
        } else {
            childExp
        }
        val method: Method
        method = when (getOperation()) {
            INSERT -> BuiltInMethod.INTO.method
            DELETE -> BuiltInMethod.REMOVE_ALL.method
            else -> throw AssertionError(getOperation())
        }
        builder.add(
            Expressions.statement(
                Expressions.call(
                    convertedChildExp, method, collectionParameter
                )
            )
        )
        val updatedCountParameter: Expression = builder.append(
            "updatedCount",
            Expressions.call(collectionParameter, "size"),
            false
        )
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    BuiltInMethod.SINGLETON_ENUMERABLE.method,
                    Expressions.convert_(
                        Expressions.condition(
                            Expressions.greaterThanOrEqual(
                                updatedCountParameter, countParameter
                            ),
                            Expressions.subtract(
                                updatedCountParameter, countParameter
                            ),
                            Expressions.subtract(
                                countParameter, updatedCountParameter
                            )
                        ),
                        Long::class.javaPrimitiveType
                    )
                )
            )
        )
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            if (pref === Prefer.ARRAY) JavaRowFormat.ARRAY else JavaRowFormat.SCALAR
        )
        return implementor.result(physType, builder.toBlock())
    }
}
