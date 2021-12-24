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
package org.apache.calcite.rex

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import java.util.List

/**
 * Shuttle which applies a permutation to its input fields.
 *
 * @see RexPermutationShuttle
 *
 * @see RexUtil.apply
 */
class RexPermuteInputsShuttle private constructor(
    mapping: Mappings.TargetMapping,
    fields: ImmutableList<RelDataTypeField>
) : RexShuttle() {
    //~ Instance fields --------------------------------------------------------
    private val mapping: Mappings.TargetMapping
    private val fields: ImmutableList<RelDataTypeField>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RexPermuteInputsShuttle.
     *
     *
     * The mapping provides at most one target for every source. If a source
     * has no targets and is referenced in the expression,
     * [org.apache.calcite.util.mapping.Mappings.TargetMapping.getTarget]
     * will give an error. Otherwise the mapping gives a unique target.
     *
     * @param mapping Mapping
     * @param inputs  Input relational expressions
     */
    constructor(
        mapping: Mappings.TargetMapping,
        vararg inputs: RelNode
    ) : this(mapping, fields(inputs)) {
    }

    init {
        this.mapping = mapping
        this.fields = fields
    }

    @Override
    override fun visitInputRef(local: RexInputRef): RexNode {
        val index: Int = local.getIndex()
        val target: Int = mapping.getTarget(index)
        return RexInputRef(
            target,
            local.getType()
        )
    }

    @Override
    override fun visitCall(call: RexCall): RexNode {
        if (call.getOperator() === RexBuilder.GET_OPERATOR) {
            val name = (call.getOperands().get(1) as RexLiteral).getValue2() as String
            val i = lookup(fields, name)
            if (i >= 0) {
                return RexInputRef.of(i, fields)
            }
        }
        return super.visitCall(call)
    }

    companion object {
        /** Creates a shuttle with an empty field list. It cannot handle GET calls but
         * otherwise works OK.  */
        fun of(mapping: Mappings.TargetMapping): RexPermuteInputsShuttle {
            return RexPermuteInputsShuttle(
                mapping,
                ImmutableList.of()
            )
        }

        //~ Methods ----------------------------------------------------------------
        private fun fields(inputs: Array<RelNode>): ImmutableList<RelDataTypeField> {
            val fields: ImmutableList.Builder<RelDataTypeField> = ImmutableList.builder()
            for (input in inputs) {
                fields.addAll(input.getRowType().getFieldList())
            }
            return fields.build()
        }

        private fun lookup(fields: List<RelDataTypeField>, @Nullable name: String?): Int {
            for (i in 0 until fields.size()) {
                val field: RelDataTypeField = fields[i]
                if (field.getName().equals(name)) {
                    return i
                }
            }
            return -1
        }
    }
}
