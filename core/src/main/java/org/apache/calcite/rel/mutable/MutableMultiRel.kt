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
package org.apache.calcite.rel.mutable

import org.apache.calcite.linq4j.Ord

/** Base Class for relations with three or more inputs.  */
abstract class MutableMultiRel @SuppressWarnings("initialization.invalid.field.write.initialized") protected constructor(
    cluster: RelOptCluster?,
    rowType: RelDataType?,
    type: MutableRelType?,
    inputs: List<MutableRel?>?
) : MutableRel(cluster, rowType, type) {
    override val inputs: List<MutableRel>

    init {
        this.inputs = ArrayList(inputs)
        for (input in Ord.zip(inputs)) {
            input.e.parent = this
            input.e.ordinalInParent = input.i
        }
    }

    @Override
    override fun setInput(ordinalInParent: Int, input: MutableRel?) {
        inputs.set(ordinalInParent, input)
        if (input != null) {
            input.parent = this
            input.ordinalInParent = ordinalInParent
        }
    }

    @Override
    fun getInputs(): List<MutableRel> {
        return inputs
    }

    @Override
    fun childrenAccept(visitor: MutableRelVisitor) {
        for (input in inputs) {
            visitor.visit(input)
        }
    }

    protected fun cloneChildren(): List<MutableRel> {
        return Util.transform(inputs, MutableRel::clone)
    }
}
