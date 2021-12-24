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

import org.apache.calcite.linq4j.Ord

/**
 * Utilities for traits propagation.
 */
@API(since = "1.24", status = API.Status.INTERNAL)
internal object EnumerableTraitsUtils {
    /**
     * Determine whether there is mapping between project input and output fields.
     * Bail out if sort relies on non-trivial expressions.
     */
    private fun isCollationOnTrivialExpr(
        projects: List<RexNode>, typeFactory: RelDataTypeFactory,
        map: Mappings.TargetMapping, fc: RelFieldCollation, passDown: Boolean
    ): Boolean {
        val index: Int = fc.getFieldIndex()
        val target: Int = map.getTargetOpt(index)
        if (target < 0) {
            return false
        }
        val node: RexNode = if (passDown) projects[index] else projects[target]
        if (node.isA(SqlKind.CAST)) {
            // Check whether it is a monotonic preserving cast
            val cast: RexCall = node as RexCall
            val newFieldCollation: RelFieldCollation = Objects.requireNonNull(RexUtil.apply(map, fc))
            val binding: RexCallBinding = RexCallBinding.create(
                typeFactory, cast,
                ImmutableList.of(RelCollations.of(newFieldCollation))
            )
            if (cast.getOperator().getMonotonicity(binding)
                === SqlMonotonicity.NOT_MONOTONIC
            ) {
                return false
            }
        }
        return true
    }

    @Nullable
    fun passThroughTraitsForProject(
        required: RelTraitSet,
        exps: List<RexNode>,
        inputRowType: RelDataType?,
        typeFactory: RelDataTypeFactory?,
        currentTraits: RelTraitSet
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        val collation: RelCollation = required.getCollation()
        if (collation == null || collation === RelCollations.EMPTY) {
            return null
        }
        val map: Mappings.TargetMapping = RelOptUtil.permutationIgnoreCast(
            exps, inputRowType
        )
        if (collation.getFieldCollations().stream().anyMatch { rc ->
                !isCollationOnTrivialExpr(
                    exps, typeFactory,
                    map, rc, true
                )
            }) {
            return null
        }
        val newCollation: RelCollation = collation.apply(map)
        return Pair.of(
            currentTraits.replace(collation),
            ImmutableList.of(currentTraits.replace(newCollation))
        )
    }

    @Nullable
    fun deriveTraitsForProject(
        childTraits: RelTraitSet, childId: Int, exps: List<RexNode>,
        inputRowType: RelDataType, typeFactory: RelDataTypeFactory?, currentTraits: RelTraitSet
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        val collation: RelCollation = childTraits.getCollation()
        if (collation == null || collation === RelCollations.EMPTY) {
            return null
        }
        val maxField: Int = Math.max(
            exps.size(),
            inputRowType.getFieldCount()
        )
        val mapping: Mappings.TargetMapping = Mappings
            .create(MappingType.FUNCTION, maxField, maxField)
        for (node in Ord.zip(exps)) {
            if (node.e is RexInputRef) {
                mapping.set((node.e as RexInputRef).getIndex(), node.i)
            } else if (node.e.isA(SqlKind.CAST)) {
                val operand: RexNode = (node.e as RexCall).getOperands().get(0)
                if (operand is RexInputRef) {
                    mapping.set((operand as RexInputRef).getIndex(), node.i)
                }
            }
        }
        val collationFieldsToDerive: List<RelFieldCollation> = ArrayList()
        for (rc in collation.getFieldCollations()) {
            if (isCollationOnTrivialExpr(exps, typeFactory, mapping, rc, false)) {
                collationFieldsToDerive.add(rc)
            } else {
                break
            }
        }
        return if (collationFieldsToDerive.size() > 0) {
            val newCollation: RelCollation = RelCollations
                .of(collationFieldsToDerive).apply(mapping)
            Pair.of(
                currentTraits.replace(newCollation),
                ImmutableList.of(currentTraits.replace(collation))
            )
        } else {
            null
        }
    }

    /**
     * This function can be reused when a Join's traits pass-down shall only
     * pass through collation to left input.
     *
     * @param required required trait set for the join
     * @param joinType the join type
     * @param leftInputFieldCount number of field count of left join input
     * @param joinTraitSet trait set of the join
     */
    @Nullable
    fun passThroughTraitsForJoin(
        required: RelTraitSet, joinType: JoinRelType,
        leftInputFieldCount: Int, joinTraitSet: RelTraitSet
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        val collation: RelCollation = required.getCollation()
        if (collation == null || collation === RelCollations.EMPTY || joinType === JoinRelType.FULL || joinType === JoinRelType.RIGHT) {
            return null
        }
        for (fc in collation.getFieldCollations()) {
            // If field collation belongs to right input: cannot push down collation.
            if (fc.getFieldIndex() >= leftInputFieldCount) {
                return null
            }
        }
        val passthroughTraitSet: RelTraitSet = joinTraitSet.replace(collation)
        return Pair.of(
            passthroughTraitSet,
            ImmutableList.of(
                passthroughTraitSet,
                passthroughTraitSet.replace(RelCollations.EMPTY)
            )
        )
    }

    /**
     * This function can be reused when a Join's traits derivation shall only
     * derive collation from left input.
     *
     * @param childTraits trait set of the child
     * @param childId id of the child (0 is left join input)
     * @param joinType the join type
     * @param joinTraitSet trait set of the join
     * @param rightTraitSet trait set of the right join input
     */
    @Nullable
    fun deriveTraitsForJoin(
        childTraits: RelTraitSet, childId: Int, joinType: JoinRelType,
        joinTraitSet: RelTraitSet, rightTraitSet: RelTraitSet?
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        // should only derive traits (limited to collation for now) from left join input.
        assert(childId == 0)
        val collation: RelCollation = childTraits.getCollation()
        if (collation == null || collation === RelCollations.EMPTY || joinType === JoinRelType.FULL || joinType === JoinRelType.RIGHT) {
            return null
        }
        val derivedTraits: RelTraitSet = joinTraitSet.replace(collation)
        return Pair.of(
            derivedTraits,
            ImmutableList.of(derivedTraits, rightTraitSet)
        )
    }
}
