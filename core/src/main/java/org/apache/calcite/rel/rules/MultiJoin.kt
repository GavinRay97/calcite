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
package org.apache.calcite.rel.rules

import org.apache.calcite.linq4j.Ord

/**
 * A MultiJoin represents a join of N inputs, whereas regular Joins
 * represent strictly binary joins.
 */
class MultiJoin(
    cluster: RelOptCluster,
    inputs: List<RelNode?>?,
    joinFilter: RexNode,
    rowType: RelDataType,
    isFullOuterJoin: Boolean,
    outerJoinConditions: List<RexNode?>,
    joinTypes: List<JoinRelType?>?,
    projFields: List<ImmutableBitSet?>?,
    joinFieldRefCountsMap: ImmutableMap<Integer?, ImmutableIntList?>,
    @Nullable postJoinFilter: RexNode?
) : AbstractRelNode(cluster, cluster.traitSetOf(Convention.NONE)) {
    //~ Instance fields --------------------------------------------------------
    private val inputs: List<RelNode>
    private val joinFilter: RexNode

    @SuppressWarnings("HidingField")
    private val rowType: RelDataType

    /**
     * Returns true if the MultiJoin corresponds to a full outer join.
     */
    val isFullOuterJoin: Boolean
    private val outerJoinConditions: List<RexNode>
    private val joinTypes: ImmutableList<JoinRelType?>
    private val projFields: List<ImmutableBitSet>
    val joinFieldRefCountsMap: ImmutableMap<Integer?, ImmutableIntList?>

    @Nullable
    private val postJoinFilter: RexNode?
    //~ Constructors -----------------------------------------------------------
    /**
     * Constructs a MultiJoin.
     *
     * @param cluster               cluster that join belongs to
     * @param inputs                inputs into this multi-join
     * @param joinFilter            join filter applicable to this join node
     * @param rowType               row type of the join result of this node
     * @param isFullOuterJoin       true if the join is a full outer join
     * @param outerJoinConditions   outer join condition associated with each join
     * input, if the input is null-generating in a
     * left or right outer join; null otherwise
     * @param joinTypes             the join type corresponding to each input; if
     * an input is null-generating in a left or right
     * outer join, the entry indicates the type of
     * outer join; otherwise, the entry is set to
     * INNER
     * @param projFields            fields that will be projected from each input;
     * if null, projection information is not
     * available yet so it's assumed that all fields
     * from the input are projected
     * @param joinFieldRefCountsMap counters of the number of times each field
     * is referenced in join conditions, indexed by
     * the input #
     * @param postJoinFilter        filter to be applied after the joins are
     */
    init {
        this.inputs = Lists.newArrayList(inputs)
        this.joinFilter = joinFilter
        this.rowType = rowType
        this.isFullOuterJoin = isFullOuterJoin
        this.outerJoinConditions = ImmutableNullableList.copyOf(outerJoinConditions)
        assert(outerJoinConditions.size() === inputs.size())
        this.joinTypes = ImmutableList.copyOf(joinTypes)
        this.projFields = ImmutableNullableList.copyOf(projFields)
        this.joinFieldRefCountsMap = joinFieldRefCountsMap
        this.postJoinFilter = postJoinFilter
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun replaceInput(ordinalInParent: Int, p: RelNode?) {
        inputs.set(ordinalInParent, p)
        recomputeDigest()
    }

    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>?): RelNode {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return MultiJoin(
            getCluster(),
            inputs,
            joinFilter,
            rowType,
            isFullOuterJoin,
            outerJoinConditions,
            joinTypes,
            projFields,
            joinFieldRefCountsMap,
            postJoinFilter
        )
    }

    /**
     * Returns a deep copy of [.joinFieldRefCountsMap].
     */
    private fun cloneJoinFieldRefCountsMap(): Map<Integer, IntArray> {
        val clonedMap: Map<Integer, IntArray> = HashMap()
        for (i in 0 until inputs.size()) {
            clonedMap.put(i, joinFieldRefCountsMap.get(i).toIntArray())
        }
        return clonedMap
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        val joinTypeNames: List<String> = ArrayList()
        val outerJoinConds: List<String> = ArrayList()
        val projFieldObjects: List<String> = ArrayList()
        for (i in 0 until inputs.size()) {
            joinTypeNames.add(joinTypes.get(i).name())
            val outerJoinCondition: RexNode = outerJoinConditions[i]
            if (outerJoinCondition == null) {
                outerJoinConds.add("NULL")
            } else {
                outerJoinConds.add(outerJoinCondition.toString())
            }
            val projField: ImmutableBitSet = projFields[i]
            if (projField == null) {
                projFieldObjects.add("ALL")
            } else {
                projFieldObjects.add(projField.toString())
            }
        }
        super.explainTerms(pw)
        for (ord in Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e)
        }
        return pw.item("joinFilter", joinFilter)
            .item("isFullOuterJoin", isFullOuterJoin)
            .item("joinTypes", joinTypeNames)
            .item("outerJoinConditions", outerJoinConds)
            .item("projFields", projFieldObjects)
            .itemIf("postJoinFilter", postJoinFilter, postJoinFilter != null)
    }

    @Override
    fun deriveRowType(): RelDataType {
        return rowType
    }

    @Override
    fun getInputs(): List<RelNode> {
        return inputs
    }

    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val joinFilter: RexNode = shuttle.apply(joinFilter)
        val outerJoinConditions: List<RexNode> = shuttle.apply(outerJoinConditions)
        val postJoinFilter: RexNode = shuttle.apply(postJoinFilter)
        return if (joinFilter === this.joinFilter && outerJoinConditions === this.outerJoinConditions && postJoinFilter === this.postJoinFilter) {
            this
        } else MultiJoin(
            getCluster(),
            inputs,
            joinFilter,
            rowType,
            isFullOuterJoin,
            outerJoinConditions,
            joinTypes,
            projFields,
            joinFieldRefCountsMap,
            postJoinFilter
        )
    }

    /**
     * Returns join filters associated with this MultiJoin.
     */
    fun getJoinFilter(): RexNode {
        return joinFilter
    }

    /**
     * Returns outer join conditions for null-generating inputs.
     */
    fun getOuterJoinConditions(): List<RexNode> {
        return outerJoinConditions
    }

    /**
     * Returns join types of each input.
     */
    fun getJoinTypes(): List<JoinRelType?> {
        return joinTypes
    }

    /**
     * Returns bitmaps representing the fields projected from each input; if an
     * entry is null, all fields are projected.
     */
    fun getProjFields(): List<ImmutableBitSet> {
        return projFields
    }

    /**
     * Returns the map of reference counts for each input, representing the fields
     * accessed in join conditions.
     */
    fun getJoinFieldRefCountsMap(): ImmutableMap<Integer?, ImmutableIntList?> {
        return joinFieldRefCountsMap
    }

    /**
     * Returns a copy of the map of reference counts for each input, representing
     * the fields accessed in join conditions.
     */
    val copyJoinFieldRefCountsMap: Map<Any, IntArray>
        get() = cloneJoinFieldRefCountsMap()

    /**
     * Returns post-join filter associated with this MultiJoin.
     */
    @Nullable
    fun getPostJoinFilter(): RexNode? {
        return postJoinFilter
    }

    fun containsOuter(): Boolean {
        for (joinType in joinTypes) {
            if (joinType.isOuterJoin()) {
                return true
            }
        }
        return false
    }
}
