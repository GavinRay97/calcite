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
 * A relational expression that collapses multiple rows into one.
 *
 *
 * Rules:
 *
 *
 *  * [org.apache.calcite.rel.rules.SubQueryRemoveRule]
 * creates a Collect from a call to
 * [org.apache.calcite.sql.fun.SqlArrayQueryConstructor],
 * [org.apache.calcite.sql.fun.SqlMapQueryConstructor], or
 * [org.apache.calcite.sql.fun.SqlMultisetQueryConstructor].
 *
 */
class Collect protected constructor(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    input: RelNode?,
    rowType: RelDataType
) : SingleRel(cluster, traitSet, input) {
    //~ Instance fields --------------------------------------------------------
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Collect.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster   Cluster
     * @param traitSet  Trait set
     * @param input     Input relational expression
     * @param rowType   Row type
     */
    init {
        rowType = requireNonNull(rowType, "rowType")
        val collectionType: SqlTypeName = getCollectionType(rowType)
        when (collectionType) {
            ARRAY, MAP, MULTISET -> {}
            else -> throw IllegalArgumentException(
                "not a collection type "
                        + collectionType
            )
        }
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster,
        traitSet: RelTraitSet?,
        input: RelNode,
        fieldName: String?
    ) : this(
        cluster, traitSet, input,
        deriveRowType(
            cluster.getTypeFactory(), SqlTypeName.MULTISET, fieldName,
            input.getRowType()
        )
    ) {
    }

    /**
     * Creates a Collect by parsing serialized output.
     */
    constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), input.getInput(),
        deriveRowType(
            input.getCluster().getTypeFactory(), SqlTypeName.MULTISET,
            requireNonNull(input.getString("field"), "field"),
            input.getInput().getRowType()
        )
    ) {
    }

    /** Returns the row type, guaranteed not null.
     * (The row type is never null after initialization, but
     * CheckerFramework can't deduce that references are safe.)  */
    protected fun rowType(): RelDataType {
        return requireNonNull(rowType, "rowType")
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
        return Collect(getCluster(), traitSet, input, rowType())
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("field", fieldName)
    }

    /**
     * Returns the name of the sole output field.
     *
     * @return name of the sole output field
     */
    val fieldName: String
        get() = Iterables.getOnlyElement(rowType().getFieldList()).getName()

    /** Returns the collection type (ARRAY, MAP, or MULTISET).  */
    val collectionType: SqlTypeName
        get() = getCollectionType(rowType())

    @Override
    protected fun deriveRowType(): RelDataType {
        // this method should never be called; rowType is always set
        throw UnsupportedOperationException()
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        /**
         * Creates a Collect.
         *
         * @param input          Input relational expression
         * @param rowType        Row type
         */
        fun create(input: RelNode, rowType: RelDataType?): Collect {
            val cluster: RelOptCluster = input.getCluster()
            val traitSet: RelTraitSet = cluster.traitSet().replace(Convention.NONE)
            return Collect(cluster, traitSet, input, rowType)
        }

        /**
         * Creates a Collect.
         *
         * @param input          Input relational expression
         * @param collectionType ARRAY, MAP or MULTISET
         * @param fieldName      Name of the sole output field
         */
        fun create(
            input: RelNode,
            collectionType: SqlTypeName?,
            fieldName: String?
        ): Collect {
            return create(
                input,
                deriveRowType(
                    input.getCluster().getTypeFactory(), collectionType,
                    fieldName, input.getRowType()
                )
            )
        }

        private fun getCollectionType(rowType: RelDataType): SqlTypeName {
            return Iterables.getOnlyElement(rowType.getFieldList())
                .getType().getSqlTypeName()
        }

        /**
         * Derives the output row type of a Collect relational expression.
         *
         * @param rel       relational expression
         * @param fieldName name of sole output field
         * @return output row type of a Collect relational expression
         */
        @Deprecated // to be removed before 2.0
        fun deriveCollectRowType(
            rel: SingleRel,
            fieldName: String?
        ): RelDataType {
            val inputType: RelDataType = rel.getInput().getRowType()
            assert(inputType.isStruct())
            return deriveRowType(
                rel.getCluster().getTypeFactory(),
                SqlTypeName.MULTISET, fieldName, inputType
            )
        }

        /**
         * Derives the output row type of a Collect relational expression.
         *
         * @param typeFactory    Type factory
         * @param collectionType MULTISET, ARRAY or MAP
         * @param fieldName      Name of sole output field
         * @param elementType    Element type
         * @return output row type of a Collect relational expression
         */
        fun deriveRowType(
            typeFactory: RelDataTypeFactory,
            collectionType: SqlTypeName?, fieldName: String?, elementType: RelDataType?
        ): RelDataType {
            val type1: RelDataType
            type1 = when (collectionType) {
                ARRAY -> SqlTypeUtil.createArrayType(typeFactory, elementType, false)
                MULTISET -> SqlTypeUtil.createMultisetType(typeFactory, elementType, false)
                MAP -> SqlTypeUtil.createMapTypeFromRecord(typeFactory, elementType)
                else -> throw AssertionError(collectionType)
            }
            return typeFactory.createTypeWithNullability(
                typeFactory.builder().add(fieldName, type1).build(), false
            )
        }
    }
}
