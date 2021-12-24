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

/** Implementation of [org.apache.calcite.rel.core.Values] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableValues
/** Creates an EnumerableValues.  */
private constructor(
    cluster: RelOptCluster, rowType: RelDataType,
    tuples: ImmutableList<ImmutableList<RexLiteral>>, traitSet: RelTraitSet
) : Values(cluster, rowType, tuples, traitSet), EnumerableRel {
    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>): RelNode {
        assert(inputs.isEmpty())
        return EnumerableValues(getCluster(), getRowType(), tuples, traitSet)
    }

    @Override
    @Nullable
    fun passThrough(required: RelTraitSet): RelNode? {
        val collation: RelCollation = required.getCollation()
        if (collation == null || collation.isDefault()) {
            return null
        }

        // A Values with 0 or 1 rows can be ordered by any collation.
        if (tuples.size() > 1) {
            var ordering: Ordering<List<RexLiteral?>?>? = null
            // Generate ordering comparator according to the required collations.
            for (fc in collation.getFieldCollations()) {
                val comparator: Ordering<List<RexLiteral?>?> = RelMdCollation.comparator(fc)
                ordering = if (ordering == null) {
                    comparator
                } else {
                    ordering.compound(comparator)
                }
            }
            // Check whether the tuples are sorted by required collations.
            if (!requireNonNull(ordering, "ordering").isOrdered(tuples)) {
                return null
            }
        }

        // The tuples order satisfies the collation, we just create a new
        // relnode with required collation info.
        return copy(traitSet.replace(collation), ImmutableList.of())
    }

    @get:Override
    val deriveMode: DeriveMode
        get() = DeriveMode.PROHIBITED

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
/*
          return Linq4j.asEnumerable(
              new Object[][] {
                  new Object[] {1, 2},
                  new Object[] {3, 4}
              });
*/
        val typeFactory: JavaTypeFactory = getCluster().getTypeFactory() as JavaTypeFactory
        val builder = BlockBuilder()
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferCustom()
        )
        val rowClass: Type = physType.getJavaRowType()
        val expressions: List<Expression> = ArrayList()
        val fields: List<RelDataTypeField> = getRowType().getFieldList()
        for (tuple in tuples) {
            val literals: List<Expression> = ArrayList()
            for (pair in Pair.zip(fields, tuple)) {
                literals.add(
                    RexToLixTranslator.translateLiteral(
                        pair.right,
                        pair.left.getType(),
                        typeFactory,
                        RexImpTable.NullAs.NULL
                    )
                )
            }
            expressions.add(physType.record(literals))
        }
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    BuiltInMethod.AS_ENUMERABLE.method,
                    Expressions.newArrayInit(
                        Primitive.box(rowClass), expressions
                    )
                )
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /** Creates an EnumerableValues.  */
        fun create(
            cluster: RelOptCluster,
            rowType: RelDataType,
            tuples: ImmutableList<ImmutableList<RexLiteral>>
        ): EnumerableValues {
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.values(mq, rowType, tuples) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.values(rowType, tuples) }
            return EnumerableValues(cluster, rowType, tuples, traitSet)
        }
    }
}
