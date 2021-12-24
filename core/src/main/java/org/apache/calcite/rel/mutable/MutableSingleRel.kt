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

import org.apache.calcite.rel.type.RelDataType

/** Mutable equivalent of [org.apache.calcite.rel.SingleRel].  */
abstract class MutableSingleRel @SuppressWarnings("initialization.invalid.field.write.initialized") protected constructor(
    type: MutableRelType?,
    rowType: RelDataType?,
    input: MutableRel
) : MutableRel(input.cluster, rowType, type) {
    var input: MutableRel?

    init {
        this.input = input
        input.parent = this
        input.ordinalInParent = 0
    }

    @Override
    override fun setInput(ordinalInParent: Int, input: MutableRel?) {
        if (ordinalInParent > 0) {
            throw IllegalArgumentException()
        }
        this.input = input
        if (input != null) {
            input.parent = this
            input.ordinalInParent = 0
        }
    }

    @get:Override
    override val inputs: List<org.apache.calcite.rel.mutable.MutableRel>
        get() = ImmutableList.of(input)

    @Override
    fun childrenAccept(visitor: MutableRelVisitor) {
        visitor.visit(input)
    }

    fun getInput(): MutableRel? {
        return input
    }
}
