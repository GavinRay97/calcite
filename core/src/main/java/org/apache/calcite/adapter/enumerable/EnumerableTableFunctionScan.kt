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

import org.apache.calcite.DataContext

/** Implementation of [org.apache.calcite.rel.core.TableFunctionScan] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableTableFunctionScan(
    cluster: RelOptCluster?,
    traits: RelTraitSet?, inputs: List<RelNode?>?, @Nullable elementType: Type?,
    rowType: RelDataType?, call: RexNode?,
    @Nullable columnMappings: Set<RelColumnMapping?>?
) : TableFunctionScan(
    cluster, traits, inputs, call, elementType, rowType,
    columnMappings
), EnumerableRel {
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?,
        rexCall: RexNode?,
        @Nullable elementType: Type?,
        rowType: RelDataType?,
        @Nullable columnMappings: Set<RelColumnMapping?>?
    ): EnumerableTableFunctionScan {
        return EnumerableTableFunctionScan(
            getCluster(), traitSet, inputs,
            elementType, rowType, rexCall, columnMappings
        )
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        return if (isImplementorDefined(getCall() as RexCall)) {
            tvfImplementorBasedImplement(implementor, pref)
        } else {
            defaultTableFunctionImplement(implementor, pref)
        }
    }

    private val isQueryable: Boolean
        private get() {
            if (getCall() !is RexCall) {
                return false
            }
            val call: RexCall = getCall() as RexCall
            if (call.getOperator() !is SqlUserDefinedTableFunction) {
                return false
            }
            val udtf: SqlUserDefinedTableFunction = call.getOperator() as SqlUserDefinedTableFunction
            if (udtf.getFunction() !is TableFunctionImpl) {
                return false
            }
            val tableFunction: TableFunctionImpl = udtf.getFunction() as TableFunctionImpl
            val method: Method = tableFunction.method
            return QueryableTable::class.java.isAssignableFrom(method.getReturnType())
        }

    private fun defaultTableFunctionImplement(
        implementor: EnumerableRelImplementor,
        @SuppressWarnings("unused") pref: Prefer
    ): Result { // TODO: remove or use
        val bb = BlockBuilder()
        // Non-array user-specified types are not supported yet
        val format: JavaRowFormat
        val elementType: Type = getElementType()
        format = if (elementType == null) {
            JavaRowFormat.ARRAY
        } else if (getRowType().getFieldCount() === 1 && isQueryable) {
            JavaRowFormat.SCALAR
        } else if (elementType is Class
            && Array<Object>::class.java.isAssignableFrom(elementType as Class<*>)
        ) {
            JavaRowFormat.ARRAY
        } else {
            JavaRowFormat.CUSTOM
        }
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(), format,
            false
        )
        var t: RexToLixTranslator = RexToLixTranslator.forAggregation(
            getCluster().getTypeFactory() as JavaTypeFactory, bb, null,
            implementor.getConformance()
        )
        t = t.setCorrelates(implementor.allCorrelateVariables)
        bb.add(Expressions.return_(null, t.translate(getCall())))
        return implementor.result(physType, bb.toBlock())
    }

    private fun tvfImplementorBasedImplement(
        implementor: EnumerableRelImplementor, pref: Prefer
    ): Result {
        val typeFactory: JavaTypeFactory = implementor.getTypeFactory()
        val builder = BlockBuilder()
        val result: Result = implementor.visitChild(this, 0, getInputs().get(0), pref)
        val physType: PhysType = PhysTypeImpl.of(
            typeFactory, getRowType(), pref.prefer(result.format)
        )
        val inputEnumerable: Expression = builder.append(
            "_input", result.block, false
        )
        val conformance: SqlConformance = implementor.map.getOrDefault(
            "_conformance",
            SqlConformanceEnum.DEFAULT
        ) as SqlConformance
        builder.add(
            RexToLixTranslator.translateTableFunction(
                typeFactory,
                conformance,
                builder,
                DataContext.ROOT,
                getCall() as RexCall?,
                inputEnumerable,
                result.physType,
                physType
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        private fun isImplementorDefined(call: RexCall): Boolean {
            return if (call.getOperator() is SqlWindowTableFunction
                && RexImpTable.INSTANCE.get(call.getOperator() as SqlWindowTableFunction) != null
            ) {
                true
            } else false
        }
    }
}
