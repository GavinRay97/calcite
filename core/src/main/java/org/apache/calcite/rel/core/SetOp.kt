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

import org.apache.calcite.linq4j.Ord

/**
 * `SetOp` is an abstract base for relational set operators such
 * as UNION, MINUS (aka EXCEPT), and INTERSECT.
 */
abstract class SetOp protected constructor(
    cluster: RelOptCluster?, traits: RelTraitSet?,
    inputs: List<RelNode?>?, kind: SqlKind, all: Boolean
) : AbstractRelNode(cluster, traits) {
    //~ Instance fields --------------------------------------------------------
    protected var inputs: ImmutableList<RelNode>
    val kind: SqlKind
    val all: Boolean
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SetOp.
     */
    init {
        Preconditions.checkArgument(kind === SqlKind.UNION || kind === SqlKind.INTERSECT || kind === SqlKind.EXCEPT)
        this.kind = kind
        this.inputs = ImmutableList.copyOf(inputs)
        this.all = all
    }

    /**
     * Creates a SetOp by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), input.getInputs(),
        SqlKind.UNION, input.getBoolean("all", false)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    abstract fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?,
        all: Boolean
    ): SetOp

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): SetOp {
        return copy(traitSet, inputs, all)
    }

    @Override
    fun replaceInput(ordinalInParent: Int, p: RelNode?) {
        val newInputs: List<RelNode> = ArrayList(inputs)
        newInputs.set(ordinalInParent, p)
        inputs = ImmutableList.copyOf(newInputs)
        recomputeDigest()
    }

    @Override
    fun getInputs(): List<RelNode> {
        return inputs
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        for (ord in Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e)
        }
        return pw.item("all", all)
    }

    @Override
    protected fun deriveRowType(): RelDataType {
        val inputRowTypes: List<RelDataType> = Util.transform(inputs, RelNode::getRowType)
        return getCluster().getTypeFactory().leastRestrictive(inputRowTypes)
            ?: throw IllegalArgumentException(
                "Cannot compute compatible row type "
                        + "for arguments to set op: "
                        + Util.sepList(inputRowTypes, ", ")
            )
    }

    /**
     * Returns whether all the inputs of this set operator have the same row
     * type as its output row.
     *
     * @param compareNames Whether column names are important in the
     * homogeneity comparison
     * @return Whether all the inputs of this set operator have the same row
     * type as its output row
     */
    fun isHomogeneous(compareNames: Boolean): Boolean {
        val unionType: RelDataType = getRowType()
        for (input in getInputs()) {
            if (!RelOptUtil.areRowTypesEqual(
                    input.getRowType(), unionType, compareNames
                )
            ) {
                return false
            }
        }
        return true
    }
}
