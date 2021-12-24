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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.Convention

/**
 * Relational expression that unnests its input's columns into a relation.
 *
 *
 * The input may have multiple columns, but each must be a multiset or
 * array. If `withOrdinality`, the output contains an extra
 * `ORDINALITY` column.
 *
 *
 * Like its inverse operation [Collect], Uncollect is generally
 * invoked in a nested loop, driven by
 * [org.apache.calcite.rel.logical.LogicalCorrelate] or similar.
 */
class Uncollect @SuppressWarnings("method.invocation.invalid") constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
    val withOrdinality: Boolean, itemAliases: List<String?>?
) : SingleRel(cluster, traitSet, input) {
    // To alias the items in Uncollect list,
    // i.e., "UNNEST(a, b, c) as T(d, e, f)"
    // outputs as row type Record(d, e, f) where the field "d" has element type of "a",
    // field "e" has element type of "b"(Presto dialect).
    // Without the aliases, the expression "UNNEST(a)" outputs row type
    // same with element type of "a".
    private val itemAliases: List<String>

    //~ Constructors -----------------------------------------------------------
    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        child: RelNode?
    ) : this(cluster, traitSet, child, false, Collections.emptyList()) {
    }

    /** Creates an Uncollect.
     *
     *
     * Use [.create] unless you know what you're doing.  */
    init {
        this.itemAliases = ImmutableList.copyOf(itemAliases)
        assert(deriveRowType() != null) { "invalid child rowtype" }
    }

    /**
     * Creates an Uncollect by parsing serialized output.
     */
    constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getBoolean("withOrdinality", false), Collections.emptyList()
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .itemIf("withOrdinality", withOrdinality, withOrdinality)
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): RelNode {
        return copy(traitSet, sole(inputs))
    }

    fun copy(traitSet: RelTraitSet, input: RelNode?): RelNode {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return Uncollect(getCluster(), traitSet, input, withOrdinality, itemAliases)
    }

    @Override
    protected fun deriveRowType(): RelDataType? {
        return deriveUncollectRowType(input, withOrdinality, itemAliases)
    }

    companion object {
        /**
         * Creates an Uncollect.
         *
         *
         * Each field of the input relational expression must be an array or
         * multiset.
         *
         * @param traitSet       Trait set
         * @param input          Input relational expression
         * @param withOrdinality Whether output should contain an ORDINALITY column
         * @param itemAliases    Aliases for the operand items
         */
        fun create(
            traitSet: RelTraitSet?,
            input: RelNode,
            withOrdinality: Boolean,
            itemAliases: List<String?>?
        ): Uncollect {
            val cluster: RelOptCluster = input.getCluster()
            return Uncollect(cluster, traitSet, input, withOrdinality, itemAliases)
        }

        /**
         * Returns the row type returned by applying the 'UNNEST' operation to a
         * relational expression.
         *
         *
         * Each column in the relational expression must be a multiset of
         * structs or an array. The return type is the combination of expanding
         * element types from each column, plus an ORDINALITY column if `withOrdinality`. If `itemAliases` is not empty, the element types
         * would not expand, each column element outputs as a whole (the return
         * type has same column types as input type).
         */
        fun deriveUncollectRowType(
            rel: RelNode,
            withOrdinality: Boolean, itemAliases: List<String?>
        ): RelDataType {
            val inputType: RelDataType = rel.getRowType()
            assert(inputType.isStruct()) { inputType.toString() + " is not a struct" }
            val requireAlias = !itemAliases.isEmpty()
            assert(!requireAlias || itemAliases.size() === inputType.getFieldCount())
            val fields: List<RelDataTypeField> = inputType.getFieldList()
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val builder: RelDataTypeFactory.Builder = typeFactory.builder()
            if (fields.size() === 1
                && fields[0].getType().getSqlTypeName() === SqlTypeName.ANY
            ) {
                // Component type is unknown to Uncollect, build a row type with input column name
                // and Any type.
                return builder
                    .add(if (requireAlias) itemAliases[0] else fields[0].getName(), SqlTypeName.ANY)
                    .nullable(true)
                    .build()
            }
            for (i in 0 until fields.size()) {
                val field: RelDataTypeField = fields[i]
                if (field.getType() is MapSqlType) {
                    val mapType: MapSqlType = field.getType() as MapSqlType
                    builder.add(SqlUnnestOperator.MAP_KEY_COLUMN_NAME, mapType.getKeyType())
                    builder.add(SqlUnnestOperator.MAP_VALUE_COLUMN_NAME, mapType.getValueType())
                } else {
                    val ret: RelDataType = field.getType().getComponentType()
                    assert(null != ret)
                    if (requireAlias) {
                        builder.add(itemAliases[i], ret)
                    } else if (ret.isStruct()) {
                        builder.addAll(ret.getFieldList())
                    } else {
                        // Element type is not a record, use the field name of the element directly
                        builder.add(field.getName(), ret)
                    }
                }
            }
            if (withOrdinality) {
                builder.add(
                    SqlUnnestOperator.ORDINALITY_COLUMN_NAME,
                    SqlTypeName.INTEGER
                )
            }
            return builder.build()
        }
    }
}
