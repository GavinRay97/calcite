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

import org.apache.calcite.plan.RelOptCluster

/**
 * Relational operator that returns the contents of a table.
 */
abstract class TableScan protected constructor(
    cluster: RelOptCluster, traitSet: RelTraitSet?,
    hints: List<RelHint?>?, table: RelOptTable
) : AbstractRelNode(cluster, traitSet), Hintable {
    //~ Instance fields --------------------------------------------------------
    /**
     * The table definition.
     */
    protected val table: RelOptTable

    /**
     * The table hints.
     */
    protected val hints: ImmutableList<RelHint>

    //~ Constructors -----------------------------------------------------------
    init {
        this.table = Objects.requireNonNull(table, "table")
        val relOptSchema: RelOptSchema = table.getRelOptSchema()
        if (relOptSchema != null) {
            cluster.getPlanner().registerSchema(relOptSchema)
        }
        this.hints = ImmutableList.copyOf(hints)
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster, traitSet: RelTraitSet?,
        table: RelOptTable
    ) : this(cluster, traitSet, ImmutableList.of(), table) {
    }

    /**
     * Creates a TableScan by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(),
        input.getTraitSet(),
        ImmutableList.of(),
        input.getTable("table")
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        return table.getRowCount()
    }

    @Override
    fun getTable(): RelOptTable {
        return table
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery?
    ): RelOptCost {
        val dRows: Double = table.getRowCount()
        val dCpu = dRows + 1 // ensure non-zero cost
        val dIo = 0.0
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo)
    }

    @Override
    fun deriveRowType(): RelDataType {
        return table.getRowType()
    }

    /** Returns an identity projection.  */
    fun identity(): ImmutableIntList {
        return identity(table)
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("table", table.getQualifiedName())
    }

    /**
     * Projects a subset of the fields of the table, and also asks for "extra"
     * fields that were not included in the table's official type.
     *
     *
     * The default implementation assumes that tables cannot do either of
     * these operations, therefore it adds a [Project] that projects
     * `NULL` values for the extra fields, using the
     * [RelBuilder.project] method.
     *
     *
     * Sub-classes, representing table types that have these capabilities,
     * should override.
     *
     * @param fieldsUsed  Bitmap of the fields desired by the consumer
     * @param extraFields Extra fields, not advertised in the table's row-type,
     * wanted by the consumer
     * @param relBuilder Builder used to create a Project
     * @return Relational expression that projects the desired fields
     */
    fun project(
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>,
        relBuilder: RelBuilder
    ): RelNode {
        val fieldCount: Int = getRowType().getFieldCount()
        if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))
            && extraFields.isEmpty()
        ) {
            return this
        }
        val fieldSize: Int = fieldsUsed.size() + extraFields.size()
        val exprList: List<RexNode> = ArrayList(fieldSize)
        val nameList: List<String> = ArrayList(fieldSize)
        val rexBuilder: RexBuilder = getCluster().getRexBuilder()
        val fields: List<RelDataTypeField> = getRowType().getFieldList()

        // Project the subset of fields.
        for (i in fieldsUsed) {
            val field: RelDataTypeField = fields[i]
            exprList.add(rexBuilder.makeInputRef(this, i))
            nameList.add(field.getName())
        }

        // Project nulls for the extra fields. (Maybe a sub-class table has
        // extra fields, but we don't.)
        for (extraField in extraFields) {
            exprList.add(rexBuilder.makeNullLiteral(extraField.getType()))
            nameList.add(extraField.getName())
        }
        return relBuilder.push(this).project(exprList, nameList).build()
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    @Override
    fun getHints(): ImmutableList<RelHint> {
        return hints
    }

    companion object {
        /** Returns an identity projection for the given table.  */
        fun identity(table: RelOptTable): ImmutableIntList {
            return ImmutableIntList.identity(table.getRowType().getFieldCount())
        }
    }
}
