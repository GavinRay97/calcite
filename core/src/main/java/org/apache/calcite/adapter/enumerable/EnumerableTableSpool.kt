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

import org.apache.calcite.linq4j.function.Experimental

/**
 * Implementation of [TableSpool] in
 * [enumerable calling convention][EnumerableConvention]
 * that writes into a [ModifiableTable] (which must exist in the current
 * schema).
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
class EnumerableTableSpool private constructor(
    cluster: RelOptCluster, traitSet: RelTraitSet,
    input: RelNode, readType: Type, writeType: Type, table: RelOptTable
) : TableSpool(cluster, traitSet, input, readType, writeType, table), EnumerableRel {
    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        // TODO for the moment only LAZY read & write is supported
        if (readType !== Type.LAZY || writeType !== Type.LAZY) {
            throw UnsupportedOperationException(
                "EnumerableTableSpool supports for the moment only LAZY read and LAZY write"
            )
        }

        //  ModifiableTable t = (ModifiableTable) root.getRootSchema().getTable(tableName);
        //  return lazyCollectionSpool(t.getModifiableCollection(), <inputExp>);
        val builder = BlockBuilder()
        val input: RelNode = getInput()
        val inputResult: Result = implementor.visitChild(this, 0, input as EnumerableRel, pref)
        val tableName: String = table.getQualifiedName().get(table.getQualifiedName().size() - 1)
        val tableExp: Expression = Expressions.convert_(
            Expressions.call(
                Expressions.call(
                    implementor.getRootExpression(),
                    BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method
                ),
                BuiltInMethod.SCHEMA_GET_TABLE.method,
                Expressions.constant(tableName, String::class.java)
            ),
            ModifiableTable::class.java
        )
        val collectionExp: Expression = Expressions.call(
            tableExp,
            BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method
        )
        val inputExp: Expression = builder.append("input", inputResult.block)
        val spoolExp: Expression = Expressions.call(
            BuiltInMethod.LAZY_COLLECTION_SPOOL.method,
            collectionExp,
            inputExp
        )
        builder.add(spoolExp)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(inputResult.format)
        )
        return implementor.result(physType, builder.toBlock())
    }

    @Override
    protected fun copy(
        traitSet: RelTraitSet, input: RelNode,
        readType: Type, writeType: Type
    ): Spool {
        return EnumerableTableSpool(
            input.getCluster(), traitSet, input,
            readType, writeType, table
        )
    }

    companion object {
        /** Creates an EnumerableTableSpool.  */
        fun create(
            input: RelNode, readType: Type,
            writeType: Type, table: RelOptTable
        ): EnumerableTableSpool {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { mq.collations(input) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { mq.distribution(input) }
            return EnumerableTableSpool(cluster, traitSet, input, readType, writeType, table)
        }
    }
}
